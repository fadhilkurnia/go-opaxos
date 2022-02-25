package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"time"
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
		value := paxi.Value{}
		var cmd paxi.Command
		if op.IsLearner && op.IsProposer {
			cmd = e.command.ToCommand()

			//if err := op.storage.ClearValue(op.execute); err != nil {
			//	log.Errorf("failed to clear executed message %v", err)
			//}

			value = op.Execute(cmd)
			log.Debugf("cmd=%v, value=%x", cmd, value)
		}

		if e.command.RPCMessage != nil && e.command.RPCMessage.Reply != nil {
			reply := paxi.CommandReply{
				OK:           true,
				EncodingTime: e.ssTime,
				Slot:         op.execute,
				Ballot:       e.ballot.String(),
				Value:        value,
			}
			err := e.command.RPCMessage.SendReply(reply.Marshal())
			if err != nil {
				log.Errorf("failed to send CommandReply %s", err)
			}
			e.command.RPCMessage = nil
			log.Infof("slot=%d time from proposed until executed %v", op.execute, time.Since(e.timestamp))
		}

		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}
