package opaxos

import (
	"github.com/ailidani/paxi"
)

func (op *OPaxos) HandlePrepareRequest(m P1a) {
	// handle if there is a new leader with higher ballot number
	// promise not to accept value with lower ballot
	if m.Ballot > op.ballot {
		op.persistHighestBallot(m.Ballot)
		op.ballot = m.Ballot
		op.IsLeader = false
		op.onOffPendingCommands = nil
	}

	// send command-shares to proposer, if previously this acceptor
	// already accept some command-shares
	l := make(map[int]CommandShare)
	for s := op.execute; s <= op.slot; s++ {
		if op.log[s] == nil || op.log[s].commit {
			continue
		}
		l[s] = CommandShare{
			Ballot:    op.log[s].ballot,
			OriBallot: op.log[s].oriBallot,
			Command:   op.log[s].command.Data,
		}
	}

	// Send P1b back to proposer / leader
	op.Send(m.Ballot.ID(), P1b{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
}

func (op *OPaxos) HandleProposeRequest(m P2a) {

	// TODO: handle if this is acceptor that also a proposer (clear command, instead of secret-shared command)

	if m.Ballot >= op.ballot {
		if m.Ballot != op.ballot {
			op.persistHighestBallot(m.Ballot)
		}

		op.ballot = m.Ballot
		op.IsLeader = false

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// update entry
		op.persistAcceptedValue(m.Slot, m.Ballot, m.Command)
		bc := paxi.BytesCommand(m.Command) // secret-shared command
		if e, exists := op.log[m.Slot]; exists {
			// TODO: forward client request to the leader, now we just discard it
			if !e.commit && m.Ballot > e.ballot && e.command != nil {
				e.command = &paxi.ClientBytesCommand{
					BytesCommand: &bc,
					RPCMessage:   nil,
				}
			}
			e.ballot = m.Ballot
		} else {
			op.log[m.Slot] = &entry{
				ballot: m.Ballot,
				command: &paxi.ClientBytesCommand{
					BytesCommand: &bc,
					RPCMessage:   nil,
				},
				commit: false,
			}
		}
	}

	// reply to proposer
	op.Send(m.Ballot.ID(), P2b{
		Ballot: op.ballot,
		Slot:   m.Slot,
		ID:     op.ID(),
	})
}
