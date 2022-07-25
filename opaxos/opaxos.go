package opaxos

import (
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"runtime"
	"strings"
	"time"
)

// entry in the log
type entry struct {
	ballot          paxi.Ballot         // the accepted ballot number
	oriBallot       paxi.Ballot         // oriBallot is the original ballot of the secret value
	commands        []paxi.BytesCommand // a batch of clear commands
	commandsHandler []*paxi.RPCMessage  // corresponding handler for each command in commands
	sharesBatch     []SecretShare       // a batch secret-shares of each command in commands, one share per command
	commit          bool                // commit indicates whether this entry is already committed or not
	quorum          *paxi.Quorum        // phase-2 quorum
	ssTime          []time.Duration     // time needed for secret-sharing process for each command in commands

	// used for recovery
	commandShares []*CommandShare // collection of command from multiple acceptors, with the same slot number
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     *Config      // OPaxos configuration
	algorithm  string       // secret-sharing algorithm: shamir or ssms
	T          int          // the minimum number of secret shares to make it reconstructive
	N          int          // the number of nodes
	isProposer bool         // isProposer indicates whether this replica has proposer role or not
	isAcceptor bool         // isAcceptor indicates whether this replica has acceptor role or not
	isLearner  bool         // IsLearner indicates whether this replica has learner role or not
	isLeader   bool         // IsLeader indicates whether this instance perceives itself as a leader or not
	quorum     *paxi.Quorum // quorum store all ack'd responses for phase-1 / leader election

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	numSSWorkers         int                           // number of workers to secret-share client's raw command
	defaultSSWorker      SecretSharingWorker           // secret-sharing worker for reconstruction only, used without channel
	protocolMessages     chan interface{}              // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientBytesCommand // raw commands from clients
	ssJobs               chan *paxi.ClientBytesCommand // raw commands ready to be secret-shared
	pendingCommands      chan *SecretSharedCommand     // pending commands that will be proposed
	onOffPendingCommands chan *SecretSharedCommand     // non nil pointer to pendingCommands after get response for phase 1

	buffer []byte // buffer used to persist ballot and accepted ballot

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
		T:                    cfg.Protocol.Threshold,
		N:                    n.GetConfig().N(),
		Q1:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
		buffer:               make([]byte, 32),
	}
	roles := strings.Split(cfg.Roles[n.ID()], ",")

	for _, opt := range options {
		opt(op)
	}

	// parse roles
	for _, r := range roles {
		if r == "proposer" {
			op.isProposer = true
			op.initDefaultSecretSharingWorker()
		}
		if r == "acceptor" {
			op.isAcceptor = true
		}
		if r == "learner" {
			op.isLearner = true
		}
	}

	return op
}

func (op *OPaxos) initAndRunSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.T

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	worker := NewWorker(op.algorithm, numShares, numThreshold)
	worker.StartProcessingInput(op.ssJobs, op.pendingCommands)
}

func (op *OPaxos) initDefaultSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.T

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	op.defaultSSWorker = NewWorker(op.algorithm, numShares, numThreshold)
}

// run is the main event processing loop
// all commands from client and protocol messages processed here
func (op *OPaxos) run() {
	var err error
	for err == nil {
		select {
		case rCmd := <-op.rawCommands:
			// start phase-1 if this proposer has not started it previously
			if !op.isLeader && op.ballot.ID() != op.ID() {
				op.Prepare()
			}

			// secret-shares the first and the remaining raw commands
			op.nonBlockingEnqueueSSJobs(rCmd)
			numRawCmd := len(op.rawCommands)
			for numRawCmd > 0 {
				rCmd = <-op.rawCommands
				op.nonBlockingEnqueueSSJobs(rCmd)
				numRawCmd--
			}
			break

		// onOffPendingCommands is nil before this replica successfully running phase-1
		// see OPaxos.HandlePrepareResponse for more detail
		case pCmd := <-op.onOffPendingCommands:
			op.Propose(pCmd)
			break

		// protocolMessages has higher priority.
		// We try to empty the protocolMessages in each loop since for every
		// client command potentially it will create O(N) protocol messages (propose & commit),
		// where N is the number of nodes in the consensus cluster
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			numPMsg := len(op.protocolMessages)
			for numPMsg > 0 && err == nil {
				err = op.handleProtocolMessages(<-op.protocolMessages)
				numPMsg--
			}
			break
		}
	}

	panic(fmt.Sprintf("opaxos exited its main loop: %v", err))
}

func (op *OPaxos) nonBlockingEnqueueSSJobs(rawCmd *paxi.ClientBytesCommand) {
	isChannelFull := false
	if len(op.ssJobs) == cap(op.ssJobs) {
		log.Warningf("Channel for ss jobs is full (len=%d)", len(op.ssJobs))
		isChannelFull = true
	}

	if !isChannelFull {
		op.ssJobs <- rawCmd
	} else {
		go func() {
			op.ssJobs <- rawCmd
		}()
	}
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

func (op *OPaxos) persistHighestBallot(b paxi.Ballot) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(op.buffer[:8], uint64(b))
	if _, err := storage.Write(op.buffer[:8]); err != nil {
		log.Errorf("failed to store max ballot %v", err)
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer", err)
	}
}

func (op *OPaxos) persistAcceptedValues(slot int, b paxi.Ballot, bori paxi.Ballot, values []paxi.BytesCommand) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(op.buffer[:8], uint64(slot))
	binary.BigEndian.PutUint64(op.buffer[8:16], uint64(b))
	binary.BigEndian.PutUint64(op.buffer[16:24], uint64(bori))
	for i, val := range values {
		binary.BigEndian.PutUint16(op.buffer[24:26], uint16(i))
		if _, err := storage.Write(op.buffer[:26]); err != nil {
			log.Errorf("failed to store accepted ballot (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
		if _, err := storage.Write(val); err != nil {
			log.Errorf("failed to store accepted values (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer", err)
	}
}

func (op *OPaxos) persistAcceptedShares(slot int, b paxi.Ballot, bori paxi.Ballot, shares []SecretShare) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(op.buffer[:8], uint64(slot))
	binary.BigEndian.PutUint64(op.buffer[8:16], uint64(b))
	binary.BigEndian.PutUint64(op.buffer[16:24], uint64(bori))
	for i, val := range shares {
		binary.BigEndian.PutUint16(op.buffer[24:26], uint16(i))
		if _, err := storage.Write(op.buffer[:26]); err != nil {
			log.Errorf("failed to store accepted ballot (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
		if _, err := storage.Write(val); err != nil {
			log.Errorf("failed to store accepted shares (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer", err)
	}
}
