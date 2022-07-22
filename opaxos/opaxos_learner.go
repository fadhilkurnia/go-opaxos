package opaxos

import (
	"encoding/binary"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
)

func (op *OPaxos) HandleCommitRequest(m P3) {
	op.slot = paxi.Max(op.slot, m.Slot)

	e, exist := op.log[m.Slot]
	if exist {
		e.sharesBatch = m.SharesBatch
		e.commit = true
	} else {
		op.log[m.Slot] = &entry{
			ballot:      m.Ballot,
			oriBallot:   m.OriBallot,
			sharesBatch: m.SharesBatch,
			commit:      true,
		}
	}

	if len(m.CommandBatch) > 0 {
		op.log[m.Slot].commands = m.CommandBatch
	}

	op.exec()
}

func (op *OPaxos) exec() {
	for {
		e, ok := op.log[op.execute]
		if !ok || !e.commit {
			break
		}

		var cmdReply paxi.CommandReply

		if len(e.commands) > 0 {
			for i, cmd := range e.commands {
				cmdReply = op.execCommands(&cmd, op.execute, e, i)
				if e.commandsHandler != nil && len(e.commandsHandler) > i && e.commandsHandler[i] != nil {
					err := e.commandsHandler[i].SendBytesReply(cmdReply.Marshal())
					if err != nil {
						log.Errorf("failed to send CommandReply: %v", err)
					}
					e.commandsHandler[i] = nil
				}
			}
		}

		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}

// execCommands parse cmd since it can be any type of command
// depends on the client type
func (op *OPaxos) execCommands(byteCmd *paxi.BytesCommand, slot int, e *entry, cid int) paxi.CommandReply {
	var cmd paxi.Command

	// by default we do not send all the data, to make the response compact
	reply := paxi.CommandReply{
		OK:         true,
		Ballot:     "", // unused for now (always empty)
		Slot:       0,  // unused for now (always empty)
		EncodeTime: 0,
		SentAt:     0,
		Data:       nil,
	}

	if *paxi.ClientIsStateful {
		cmd = byteCmd.ToCommand()

	} else if *paxi.ClientIsStateful == false {
		gcmd := &paxi.GenericCommand{}
		err := msgpack.Unmarshal(*byteCmd, &gcmd)
		if err != nil {
			log.Fatalf("failed to unmarshal client's generic command %s", err.Error())
		}
		cmd.Key = paxi.Key(binary.BigEndian.Uint32(gcmd.Key))
		cmd.Value = gcmd.Value
		reply.SentAt = gcmd.SentAt // forward sentAt from client back to client

	} else {
		log.Errorf("unknown client stateful property, does not know how to handle the command")
		reply.OK = false
	}

	if *paxi.GatherSecretShareTime {
		reply.EncodeTime = e.ssTime[cid]
	}

	value := op.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	//log.Debugf("cmd=%v, value=%x", cmd, value)
	return reply
}
