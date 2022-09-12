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
	r.nonBlockingEnqueueProtocolMessage(pmsg)
}

func (r *Replica) HandleClientCommand(ccmd *paxi.ClientCommand) {
	log.Debugf("enqueuing client commands: %v", ccmd)
	if ccmd.CommandType == paxi.TypeOtherCommand {
		r.nonBlockingEnqueueRawCommand(ccmd)
	} else if ccmd.CommandType == paxi.TypeGetMetadataCommand {
		r.nonBlockingEnqueueProtocolMessage(ccmd)
	} else {
		log.Errorf("unknown client's command")
	}
}

func (r *Replica) RunWithWorker() {
	go r.FastOPaxos.run()
	r.Run()
}

func (r *Replica) nonBlockingEnqueueRawCommand(ccmd *paxi.ClientCommand)  {
	if len(r.FastOPaxos.rawCommands) == cap(r.FastOPaxos.rawCommands) {
		log.Warningf("buffered channel for client's raw commands is full, cap=%d", len(r.FastOPaxos.rawCommands))
		go func() {
			r.FastOPaxos.rawCommands <- ccmd
		}()
		return
	}

	r.FastOPaxos.rawCommands <- ccmd
}

func (r *Replica) nonBlockingEnqueueProtocolMessage(pmsg interface{})  {
	if len(r.FastOPaxos.protocolMessages) == cap(r.FastOPaxos.protocolMessages) {
		log.Warningf("buffered channel for protocol messages is full, cap=%d", len(r.FastOPaxos.protocolMessages))
		go func() {
			r.FastOPaxos.protocolMessages <- pmsg
		}()
		return
	}

	r.FastOPaxos.protocolMessages <- pmsg
}