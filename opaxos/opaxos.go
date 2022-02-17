package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"runtime"
	"strings"
	"time"
)

// entry in log
type entry struct {
	ballot       paxi.Ballot
	command      paxi.BytesCommand // clear or secret shared command in []bytes
	clearCommand *paxi.Command     // clear command
	commit       bool              // commit indicates whether this entry is already committed or not
	request      *SSBytesRequest   // each request has reply channel, so we need to store it
	quorum       *paxi.Quorum
	timestamp    time.Time

	commandShares []*CommandShare // collection of client-command from acceptors, with the same slot number
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     *Config
	algorithm  string
	K          int // the minimum number of secret shares to make it reconstructive
	N          int // the number of nodes
	IsProposer bool
	IsAcceptor bool
	IsLearner  bool
	IsLeader   bool // IsLeader indicates whether this instance perceives itself as a leader or not

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	quorum       *paxi.Quorum            // quorum store all ack'd responses
	numSSWorkers int                     // number of worker for secret-sharing
	rawRequests  chan *paxi.BytesRequest // raw requests, ready to be secret-shared
	ssRequests   chan *SSBytesRequest    // phase 1 pending requests
	//storage      paxi.PersistentStorage

	defaultSSWorker secretSharingWorker // secret-sharing worker for reconstruction only, used without channel

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool

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
func NewOPaxos(n paxi.Node, cfg *Config, options ...func(*OPaxos)) *OPaxos {
	op := &OPaxos{
		Node:         n,
		config:       cfg,
		algorithm:    cfg.Protocol.SecretSharing,
		log:          make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:         -1,
		execute:      0,
		quorum:       paxi.NewQuorum(),
		rawRequests:  make(chan *paxi.BytesRequest, 1000),
		ssRequests:   make(chan *SSBytesRequest, 1000),
		numSSWorkers: maxInt(10, runtime.GOMAXPROCS(-1)),
		//storage:         paxi.NewPersistentStorage(n.ID()),
		K:               cfg.Protocol.Threshold,
		N:               n.GetConfig().N(),
		Q1:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
		ReplyWhenCommit: false,
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

// HandleRequest handles request and start phase 1 or phase 2
func (op *OPaxos) HandleRequest(r paxi.BytesRequest) {
	if !op.IsProposer {
		log.Warningf("non-proposer node %v receiving user request, ignoring it", op.ID())
		return
	}
	if !op.IsLeader {
		op.rawRequests <- &r

		// start phase 1 if this replica has not started it previously
		if op.ballot.ID() != op.ID() {
			op.Prepare()
		}
	} else {
		op.rawRequests <- &r
		op.Propose(<-op.ssRequests)
	}
}

func (op *OPaxos) initAndRunSecretSharingWorker() {
	numShares := op.N - 1
	numThreshold := op.K

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2 - 1
	}

	worker := newWorker(op.algorithm, numShares, numThreshold)
	worker.startProcessingInput(op.rawRequests, op.ssRequests)
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
