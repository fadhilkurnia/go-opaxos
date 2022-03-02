package opaxos

import (
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

	numSSWorkers    int                           // number of workers to secret-share client's raw command
	defaultSSWorker secretSharingWorker           // secret-sharing worker for reconstruction only, used without channel
	rawCommands     chan *paxi.ClientBytesCommand // raw commands, ready to be secret-shared
	pendingCommands chan *SecretSharedCommand     // pending commands that will be proposed

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
		Node:            n,
		config:          &cfg,
		algorithm:       cfg.Protocol.SecretSharing,
		log:             make(map[int]*entry, cfg.BufferSize),
		slot:            -1,
		execute:         0,
		quorum:          paxi.NewQuorum(),
		rawCommands:     make(chan *paxi.ClientBytesCommand, cfg.ChanBufferSize),
		pendingCommands: make(chan *SecretSharedCommand, cfg.ChanBufferSize),
		numSSWorkers:    maxInt(10, runtime.GOMAXPROCS(-1)),
		K:               cfg.Protocol.Threshold,
		N:               n.GetConfig().N(),
		Q1:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
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
	worker.startProcessingInput(op.rawCommands, op.pendingCommands)
}

func (op *OPaxos) initDefaultSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.K

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	op.defaultSSWorker = newWorker(op.algorithm, numShares, numThreshold)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// HandleCommandRequest handles request and start phase 1 or phase 2
func (op *OPaxos) HandleCommandRequest(r *paxi.ClientBytesCommand) {
	if !op.IsProposer {
		log.Warningf("non-proposer node %v receiving user request, ignoring it", op.ID())
		return
	}
	if !op.IsLeader {
		op.rawCommands <- r

		// start phase 1 if this replica has not started it previously
		if op.ballot.ID() != op.ID() {
			op.Prepare()
		}
	} else {
		//op.rawCommands <- r
		//op.Propose(<-op.pendingCommands)
		ss, ssTime, err := op.defaultSSWorker.secretShareCommand(r.Data)
		if err != nil {
			log.Errorf("failed to do secret sharing: %v", err)
		}
		op.Propose(&SecretSharedCommand{r, ssTime, ss})
	}
}
