package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strings"
)

const (
	HTTPHeaderSlot         = "Slot"
	HTTPHeaderBallot       = "Ballot"
	HTTPHeaderExecute      = "Execute"
	HTTPHeaderInProgress   = "Inprogress"
	HTTPHeaderEncodingTime = "Encoding"
)

type Replica struct {
	paxi.Node
	*OPaxos
}

func NewReplica(id paxi.ID) *Replica {
	// parse paxi config to opaxos config
	globalCfg := paxi.GetConfig()
	cfg := InitConfig(&globalCfg)
	roles := strings.Split(globalCfg.Roles[id], ",")

	log.Debugf("instantiating a replica with role=%v", roles)
	log.Debugf("config: %v", cfg)

	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.OPaxos = NewOPaxos(r, &cfg)

	r.Register(paxi.BytesRequest{}, r.handleByteRequest)
	r.Register(paxi.Request{}, r.handleRequest)

	if r.OPaxos.IsProposer {
		r.Register(P1b{}, r.HandlePrepareResponse)
		r.Register(P2b{}, r.HandleProposeResponse)
	}
	if r.OPaxos.IsAcceptor {
		r.Register(P2a{}, r.HandleProposeRequest)
		r.Register(P1a{}, r.HandlePrepareRequest)
	}
	if r.OPaxos.IsLearner {
		r.Register(P3{}, r.HandleCommitRequest)
	}

	return r
}

func (r *Replica) RunWithWorker() {
	if r.OPaxos.IsProposer {
		for i := 0; i < r.OPaxos.numSSWorkers; i++ {
			go r.OPaxos.runSecretSharingWorkers()
		}
	}
	r.Run()
}

func (r *Replica) handleRequest(m paxi.Request) {
	// log.Debugf("Replica %s received %v\n", r.ID(), m)
	r.OPaxos.HandleRequest(m.ToBytesRequest())
}

func (r *Replica) handleByteRequest(m paxi.BytesRequest) {
	// log.Debugf("Replica %s received %v\n", r.ID(), m.Command.ToCommand())
	r.OPaxos.HandleRequest(m)
}
