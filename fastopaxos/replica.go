package fastopaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Replica for a single FastOPaxos untrusted node instance
type Replica struct {
	paxi.Node
	*FastOPaxos
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.FastOPaxos = NewFastOPaxos(r.Node)
	r.Register(P2a{}, r.EnqueueProtocolMessages)
	r.Register(P2b{}, r.EnqueueProtocolMessages)
	r.Register(P3{}, r.EnqueueProtocolMessages)
	r.Register(&paxi.ClientCommand{}, r.EnqueueClientRequests)

	return r
}

func (r *Replica) EnqueueProtocolMessages(pmsg interface{}) {
	log.Debugf("enqueuing protocol message: %v", pmsg)
	r.FastOPaxos.protocolMessages <- pmsg
}

func (r *Replica) EnqueueClientRequests(ccmd *paxi.ClientCommand) {
	log.Debugf("enqueuing client request: %v", ccmd)
	r.FastOPaxos.rawCommands <- ccmd
}

func (r *Replica) RunWithWorker() {
	for i := 0; i < r.FastOPaxos.numSSWorkers; i++ {
		go r.FastOPaxos.initRunSecretSharingWorker()
	}
	go r.FastOPaxos.run()
	r.Run()
}