package opaxos

import (
	"bytes"
	"encoding/gob"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strconv"
)

func (op *OPaxos) HandleCommitRequest(m CommitRequest) {
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
		value := paxi.Value{}
		var cmd *paxi.Command
		if op.IsLearner && op.IsProposer {
			if e.clearCommand == nil {
				decoder := gob.NewDecoder(bytes.NewReader(e.command))
				decodedCmd := paxi.Command{}
				if err := decoder.Decode(&decodedCmd); err != nil {
					log.Errorf("failed to decode user's command: %v", err)
					continue
				}
				value = op.Execute(decodedCmd)
			} else {
				cmd = e.clearCommand
				value = op.Execute(*e.clearCommand)
			}
		}

		if e.request != nil {
			reply := paxi.Reply{
				Command:    *cmd,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderEncodingTime] = strconv.FormatInt(e.encodingTime, 10)
			e.request.Reply(reply)
			e.request = nil
			//log.Infof("slot=%d time from received until executed %v", op.execute, time.Since(e.timestamp))
		}
		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}
