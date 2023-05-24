package untrustedopaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strings"
)

type Replica struct {
	paxi.Node
	*OPaxos
}

func NewUntrustedServer(id paxi.ID) *Replica {
	// parse paxi config to opaxos config
	globalCfg := paxi.GetConfig()
	roles := strings.Split(globalCfg.Roles[id], ",")
	log.Debugf("instantiating a replica with role=%s", roles)

	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.OPaxos = NewOPaxos(r)

	r.Register(P1b{}, r.EnqueueProtocolMessages)
	r.Register(P2b{}, r.EnqueueProtocolMessages)
	r.Register(P2a{}, r.EnqueueProtocolMessages)
	r.Register(P1a{}, r.EnqueueProtocolMessages)
	r.Register(P3{}, r.EnqueueProtocolMessages)
	r.Register(paxi.BeLeaderRequest{}, r.EnqueueProtocolMessages)
	r.Register(&paxi.ClientCommand{}, r.EnqueueClientCommand)

	return r
}

func (r *Replica) Run() {
	go r.OPaxos.run()
	r.Node.Run()
}

func (r *Replica) EnqueueProtocolMessages(pmsg interface{}) {
	log.Debugf("enqueue protocol message: %v", pmsg)
	r.OPaxos.protocolMessages <- pmsg
}

func (r *Replica) EnqueueClientCommand(ccmd *paxi.ClientCommand) {
	log.Debugf("enqueue client command: %v", ccmd)
	r.OPaxos.rawCommands <- ccmd
}
