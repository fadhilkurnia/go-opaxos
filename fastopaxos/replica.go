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
	r.Register(P3c{}, r.EnqueueProtocolMessages)
	r.Register(&paxi.ClientCommand{}, r.HandleClientCommand)

	return r
}

func (r *Replica) EnqueueProtocolMessages(pmsg interface{}) {
	log.Debugf("enqueuing protocol message: %v", pmsg)
	r.FastOPaxos.protocolMessages <- pmsg
}

func (r *Replica) HandleClientCommand(ccmd *paxi.ClientCommand) {
	log.Debugf("enqueuing client commands: %v", ccmd)
	if ccmd.CommandType == paxi.TypeOtherCommand {
		r.FastOPaxos.rawCommands <- ccmd
	} else if ccmd.CommandType == paxi.TypeGetMetadataCommand {
		r.FastOPaxos.protocolMessages <- ccmd
	} else {
		log.Errorf("unknown client's command")
	}
}

func (r *Replica) RunWithWorker() {
	go r.FastOPaxos.run()
	r.Run()
}
