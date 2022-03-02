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
	roles := strings.Split(globalCfg.Roles[id], ",")

	log.Debugf("instantiating a replica with role=%v", roles)

	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.OPaxos = NewOPaxos(r)

	r.Register(&paxi.ClientBytesCommand{}, r.EnqueueClientRequests)
	r.Register(P1b{}, r.EnqueueProtocolMessages)
	r.Register(P2b{}, r.EnqueueProtocolMessages)
	r.Register(P2a{}, r.EnqueueProtocolMessages)
	r.Register(P1a{}, r.EnqueueProtocolMessages)
	r.Register(P3{}, r.EnqueueProtocolMessages)

	return r
}

func (r *Replica) RunWithWorker() {
	if r.OPaxos.IsProposer {
		for i := 0; i < r.OPaxos.numSSWorkers; i++ {
			go r.OPaxos.initAndRunSecretSharingWorker()
		}
	}
	go r.OPaxos.run()
	r.Run()
}

func (r *Replica) EnqueueProtocolMessages(pmsg interface{}) {
	r.OPaxos.protocolMessages <- pmsg
}

func (r *Replica) EnqueueClientRequests(ccmd *paxi.ClientBytesCommand) {
	r.OPaxos.rawCommands <- ccmd
}
