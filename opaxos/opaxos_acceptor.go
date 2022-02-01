package opaxos

import "github.com/ailidani/paxi"

func (op *OPaxos) HandlePrepareRequest(m PrepareRequest) {
	// handle if there is a new leader with higher ballot number
	// promise not to accept value with lower ballot
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
	}

	// send command-shares to proposer, if previously this acceptor
	// already accept some command-shares
	l := make(map[int]CommandShare)
	for s := op.execute; s <= op.slot; s++ {
		if op.log[s] == nil || op.log[s].commit {
			continue
		}
		l[s] = CommandShare{op.log[s].ballot, op.log[s].command}
	}

	// Send PrepareResponse back to proposer / leader
	op.Send(m.Ballot.ID(), PrepareResponse{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
}

func (op *OPaxos) HandleProposeRequest(m ProposeRequest) {

	// TODO: handle if this is acceptor that also a proposer (clear command, instead of secret-shared command)

	if m.Ballot >= op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false

		// log.Infof("message slot=%v acked after %v\n", m.Slot, time.Since(m.SendTime))

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// update entry
		if e, exists := op.log[m.Slot]; exists {
			// TODO: forward client request to the leader, now we just discard it
			if !e.commit && m.Ballot > e.ballot && e.request != nil {
				e.request = nil
			}
			e.command = m.Command // secret-shared command
			e.ballot = m.Ballot
		} else {
			op.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command, // secret-shared command
				commit:  false,
			}
		}
	}

	// reply to proposer
	op.Send(m.Ballot.ID(), ProposeResponse{
		Ballot: op.ballot,
		Slot:   m.Slot,
		ID:     op.ID(),
	})
}
