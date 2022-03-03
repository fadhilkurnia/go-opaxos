package opaxos

import (
	"encoding/binary"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
)

func (op *OPaxos) HandleCommitRequest(m P3) {
	op.slot = paxi.Max(op.slot, m.Slot)

	// TODO: tell the leader if the slot is empty
	e, exist := op.log[m.Slot]
	if !exist {
		op.log[m.Slot] = &entry{}
		e = op.log[m.Slot]
	}
	e.commit = true

	op.exec()
}

func (op *OPaxos) exec() {
	for {
		e, ok := op.log[op.execute]
		if !ok || !e.commit {
			break
		}

		// only learner that also a proposer that can execute the command
		// since it has clear command, not the secret-shared command
		var cmdReply paxi.CommandReply

		if op.IsLearner && op.IsProposer {
			cmdReply = op.execCommands(e.command.BytesCommand, op.execute, e)

			if e.command.RPCMessage != nil && e.command.RPCMessage.Reply != nil {
				err := e.command.RPCMessage.SendBytesReply(cmdReply.Marshal())
				if err != nil {
					log.Errorf("failed to send CommandReply %s", err)
				}
				e.command.RPCMessage = nil
				//log.Infof("slot=%d time from proposed until executed %v", op.execute, time.Since(e.timestamp))
			} else {
				log.Errorf("missing RPCMessage! $v", e.command.Data)
			}
		}

		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}

// execCommands parse cmd since it can be any type of command
// depends on the client type
func (op *OPaxos) execCommands(byteCmd *paxi.BytesCommand, slot int, e *entry) paxi.CommandReply {
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

	if *paxi.ClientType == "default" || *paxi.ClientType == "" || *paxi.ClientType == "callback" {
		cmd = byteCmd.ToCommand()

	} else if *paxi.ClientType == "pipeline" || *paxi.ClientType == "unix"{
		gcmd := &paxi.GenericCommand{}
		err := msgpack.Unmarshal(*byteCmd, &gcmd)
		if err != nil {
			log.Fatalf("failed to unmarshal client's generic command %s", err.Error())
		}
		cmd.Key = paxi.Key(binary.BigEndian.Uint32(gcmd.Key))
		cmd.Value = gcmd.Value
		reply.SentAt = gcmd.SentAt // forward sentAt from client back to client

	} else {
		log.Errorf("unknown client type, does not know how to handle the command")
		reply.OK = false
	}

	if *paxi.GatherSecretShareTime {
		reply.EncodeTime = e.ssTime
	}

	value := op.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	//log.Debugf("cmd=%v, value=%x", cmd, value)
	return reply
}
