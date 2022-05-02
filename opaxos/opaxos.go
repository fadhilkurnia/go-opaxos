package opaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"runtime"
	"strings"
	"time"
)

// entry in the log
type entry struct {
	ballot    paxi.Ballot              // the leadership ballot number
	command   *paxi.ClientBytesCommand // clear or secret shared command in []bytes, with reply writer
	commit    bool                     // commit indicates whether this entry is already committed or not
	quorum    *paxi.Quorum             // phase-2 quorum
	timestamp time.Time                // timestamp when the command in this entry is proposed
	ssTime    time.Duration            // time needed for secret-sharing process

	commandShares []*CommandShare // collection of command from multiple acceptors, with the same slot number
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     *Config      // OPaxos configuration
	algorithm  string       // secret-sharing algorithm
	K          int          // the minimum number of secret shares to make it reconstructive
	N          int          // the number of nodes
	IsProposer bool         // IsProposer indicates whether this replica has proposer role or not
	IsAcceptor bool         // IsAcceptor indicates whether this replica has acceptor role or not
	IsLearner  bool         // IsLearner indicates whether this replica has learner role or not
	IsLeader   bool         // IsLeader indicates whether this instance perceives itself as a leader or not
	quorum     *paxi.Quorum // quorum store all ack'd responses for phase-1 / leader election

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	numSSWorkers         int                           // number of workers to secret-share client's raw command
	defaultSSWorker      secretSharingWorker           // secret-sharing worker for reconstruction only, used without channel
	protocolMessages     chan interface{}              // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientBytesCommand // raw commands from clients
	ssJobs               chan *paxi.ClientBytesCommand // raw commands ready to be secret shared
	pendingCommands      chan *SecretSharedCommand     // pending commands that will be proposed
	onOffPendingCommands chan *SecretSharedCommand     // non nil pointer to pendingCommands after get response for phase 1

	//rawRequests chan *paxi.BytesRequest // raw requests, ready to be secret-shared
	//ssRequests  chan *SSBytesRequest    // phase 1 pending requests
	//storage      paxi.PersistentStorage

	Q1 func(*paxi.Quorum) bool
	Q2 func(*paxi.Quorum) bool

	// TODO: parse config so the OPaxos instance know the list of each roles.
	// A proposer can send a clear proposed value to other proposer, but need to
	// send a secret shared value to the acceptor.
	// proposerIDs []ID
	// acceptorIDs []ID
	// learnerIDs  []ID

	// TODO: a learner which also a proposer can execute command in the DB, but learner that not a proposer
	// just need to change the execute slot number, since it can not execute a secret shared command.

	// TODO: learner only node does not need http server
	// TODO: the OPaxos implementation should be oblivious with the command sent ([]byte), it is the replica's job
	// to parse and execute the command.
}

// NewOPaxos creates new OPaxos instance (constructor)
func NewOPaxos(n paxi.Node, options ...func(*OPaxos)) *OPaxos {
	cfg := InitConfig(n.GetConfig())
	log.Debugf("config: %v", cfg)

	op := &OPaxos{
		Node:                 n,
		config:               &cfg,
		algorithm:            cfg.Protocol.SecretSharing,
		log:                  make(map[int]*entry, cfg.BufferSize),
		slot:                 -1,
		execute:              0,
		quorum:               paxi.NewQuorum(),
		protocolMessages:     make(chan interface{}, cfg.ChanBufferSize),
		rawCommands:          make(chan *paxi.ClientBytesCommand, cfg.ChanBufferSize),
		ssJobs:               make(chan *paxi.ClientBytesCommand, cfg.ChanBufferSize),
		pendingCommands:      make(chan *SecretSharedCommand, cfg.ChanBufferSize),
		onOffPendingCommands: nil,
		numSSWorkers:         runtime.NumCPU(),
		K:                    cfg.Protocol.Threshold,
		N:                    n.GetConfig().N(),
		Q1:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
		//storage:         paxi.NewPersistentStorage(n.ID()),
	}
	roles := strings.Split(cfg.Roles[n.ID()], ",")

	for _, opt := range options {
		opt(op)
	}

	// parse roles
	for _, r := range roles {
		if r == "proposer" {
			op.IsProposer = true
			op.initDefaultSecretSharingWorker()
		}
		if r == "acceptor" {
			op.IsAcceptor = true
		}
		if r == "learner" {
			op.IsLearner = true
		}
	}

	return op
}

func (op *OPaxos) initAndRunSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.K

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	worker := newWorker(op.algorithm, numShares, numThreshold)
	worker.startProcessingInput(op.ssJobs, op.pendingCommands)
}

func (op *OPaxos) initDefaultSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.K

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	op.defaultSSWorker = newWorker(op.algorithm, numShares, numThreshold)
}

// run is the main event processing loop
// all commands from client and protocol messages processed here
func (op *OPaxos) run() {
	var err error
	for err == nil {
		select {
		case rCmd := <-op.rawCommands:
			op.ssJobs <- rCmd

			// start phase 1 if this proposer has not started it previously
			if !op.IsLeader && op.ballot.ID() != op.ID() {
				op.Prepare()
			}
			break

		// onOffPendingCommands is nil before this replica successfully running phase-1
		// see OPaxos.HandlePrepareResponse for more detail
		case pCmd := <-op.onOffPendingCommands:
			op.Propose(pCmd)
			break

		//	protocol messages have higher priority compared to
		//	raw and pending commands
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			break
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			break
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			break
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			break
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			break
		}
	}

	panic(fmt.Sprintf("opaxos exited its main loop: %v", err))
}

func (op *OPaxos) handleProtocolMessages(pmsg interface{}) error {
	switch pmsg.(type) {
	case P1a:
		op.HandlePrepareRequest(pmsg.(P1a))
		break
	case P1b:
		op.HandlePrepareResponse(pmsg.(P1b))
		break
	case P2a:
		op.HandleProposeRequest(pmsg.(P2a))
		break
	case P2b:
		op.HandleProposeResponse(pmsg.(P2b))
		break
	case P3:
		op.HandleCommitRequest(pmsg.(P3))
		break
	default:
		log.Errorf("unknown protocol messages")
	}
	return nil
}
