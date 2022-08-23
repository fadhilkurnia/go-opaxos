package paxos

import (
	"flag"
	"github.com/ailidani/paxi"
)

var ephemeralLeader = flag.Bool("ephemeral_leader", false, "unstable leader, if true paxos replica try to become leader instead of forward requests to current leader")
var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
	HTTPHeaderEncodingTime = "Encoding"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)

	r.Register(P1a{}, r.EnqueueProtocolMessage)
	r.Register(P1b{}, r.EnqueueProtocolMessage)
	r.Register(P2a{}, r.EnqueueProtocolMessage)
	r.Register(P2b{}, r.EnqueueProtocolMessage)
	r.Register(P3{}, r.EnqueueProtocolMessage)
	r.Register(&paxi.ClientCommand{}, r.EnqueueClientCommand)

	return r
}

func (r *Replica) EnqueueProtocolMessage(pmsg interface{}) {
	r.Paxos.protocolMessages <- pmsg
}

func (r *Replica) EnqueueClientCommand(ccmd *paxi.ClientCommand) {
	r.Paxos.rawCommands <- ccmd
}

func (r *Replica) RunWithChannel() {
	go r.Paxos.run()
	r.Run()
}
