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
		op.isLeader = false
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
			Ballot:      op.log[s].ballot,
			OriBallot:   op.log[s].oriBallot,
			SharesBatch: op.log[s].sharesBatch,
			ID:          op.ID(),
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
	if m.Ballot >= op.ballot {
		if m.Ballot != op.ballot {
			op.persistHighestBallot(m.Ballot)
		}

		op.ballot = m.Ballot
		op.isLeader = false

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// update entry
		op.persistAcceptedShares(m.Slot, m.Ballot, m.OriBallot, m.SharesBatch)
		if e, exists := op.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				e.sharesBatch = m.SharesBatch
				e.ballot = m.Ballot
				e.oriBallot = m.OriBallot
				e.commands = nil
				e.commandsHandler = nil
			}
		} else {
			op.log[m.Slot] = &entry{
				ballot:          m.Ballot,
				oriBallot:       m.OriBallot,
				commands:        nil,
				commandsHandler: nil,
				sharesBatch:     m.SharesBatch,
				commit:          false,
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
