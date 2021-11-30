package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)

type Replica struct {
	paxi.Node
	*OPaxos
}

func NewReplica(id paxi.ID, threshold int) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.OPaxos = NewOPaxos(r, threshold)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(PrepareRequest{}, r.HandlePrepareRequest)
	r.Register(PrepareResponse{}, r.HandlePrepareResponse)
	r.Register(ProposeRequest{}, r.HandleProposeRequest)
	r.Register(ProposeResponse{}, r.HandleProposeResponse)
	r.Register(CommitRequest{}, r.HandleCommitRequest)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	// TODO: for now, we only handle write request
	r.OPaxos.HandleRequest(m)
}