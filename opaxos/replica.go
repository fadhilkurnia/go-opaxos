package opaxos

import (
	"flag"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strings"
)

var opaxosThreshold = flag.Int("opaxos_k", 2, "minimum number of shares to reconstruct a secret")
var opaxosRoles = flag.String("opaxos_roles", "proposer,acceptor,learner", "the roles of this server, separated by comma.")

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
	ssThreshold := *opaxosThreshold
	roles := strings.Split(*opaxosRoles, ",")

	log.Debugf("instantiating a replica with role=%v", roles)

	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.OPaxos = NewOPaxos(r, ssThreshold, roles)

	if r.OPaxos.IsProposer {
		r.Register(paxi.Request{}, r.handleRequest)
		r.Register(PrepareResponse{}, r.HandlePrepareResponse)
		r.Register(ProposeResponse{}, r.HandleProposeResponse)
	}
	if r.OPaxos.IsAcceptor {
		r.Register(ProposeRequest{}, r.HandleProposeRequest)
		r.Register(PrepareRequest{}, r.HandlePrepareRequest)
	}
	if r.OPaxos.IsLearner {
		r.Register(CommitRequest{}, r.HandleCommitRequest)
	}

	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	r.OPaxos.HandleRequest(m)
}
