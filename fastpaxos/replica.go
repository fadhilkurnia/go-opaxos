package fastpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Replica for a single FastPaxos node instance
type Replica struct {
	paxi.Node
	*FastPaxos
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.FastPaxos = NewFastPaxos(r.Node)
	r.Register(P2a{}, r.EnqueueProtocolMessages)
	r.Register(P2b{}, r.EnqueueProtocolMessages)
	r.Register(P3{}, r.EnqueueProtocolMessages)
	r.Register(&paxi.ClientBytesCommand{}, r.EnqueueClientRequests)

	return r
}

func (r *Replica) EnqueueProtocolMessages(pmsg interface{}) {
	log.Debugf("enqueuing protocol message: %v", pmsg)
	r.FastPaxos.protocolMessages <- pmsg
}

func (r *Replica) EnqueueClientRequests(ccmd *paxi.ClientBytesCommand) {
	log.Debugf("enqueuing client request: %v", ccmd)
	r.FastPaxos.rawCommands <- ccmd
}

func (r *Replica) RunWithChannel() {
	go r.FastPaxos.run()
	r.Run()
}