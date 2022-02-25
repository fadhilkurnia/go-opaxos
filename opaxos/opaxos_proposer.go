package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/shamir"
	"time"
)

// Prepare initiates phase 1 of opaxos
func (op *OPaxos) Prepare() {
	if op.IsLeader {
		return
	}
	op.ballot.Next(op.ID())
	op.quorum.Reset()

	//if err := op.storage.PersistBallot(op.ballot); err != nil {
	//	log.Errorf("failed to persist max ballot %v", err)
	//}

	op.quorum.ACK(op.ID())

	log.Debugf("broadcasting prepare message %s", op.ballot.String())
	op.Broadcast(P1a{Ballot: op.ballot})
}

// Propose initiates phase 2 of OPaxos
func (op *OPaxos) Propose(r *SecretSharedCommand) {
	op.slot++
	op.log[op.slot] = &entry{
		ballot:    op.ballot,
		command:   r.ClientBytesCommand,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
		ssTime:    r.ssTime,
	}

	//if err := op.storage.PersistValue(op.slot, op.log[op.slot].command); err != nil {
	//	log.Errorf("failed to persist accepted value %v", err)
	//}

	op.log[op.slot].quorum.ACK(op.ID())

	// TODO: broadcast clear message to trusted acceptors, secret-shared message to untrusted acceptors
	// for now we are sending secret-shared only
	proposeRequests := make([]interface{}, len(r.ssCommands))
	for i := 0; i < len(r.ssCommands); i++ {
		proposeRequests[i] = P2a{
			Ballot:  op.ballot,
			Slot:    op.slot,
			Command: r.ssCommands[i],
		}
	}

	// broadcast propose message to the acceptors
	if op.config.Thrifty {
		op.MulticastQuorumUniqueMessage(op.config.Protocol.Quorum2-1, proposeRequests)
	} else {
		op.MulticastUniqueMessage(proposeRequests)
	}

	// TODO: store secret-shared commands for backup
	//commandShares := make([]*CommandShare, len(ssCommand))
	//for i := 0; i < len(ssCommand); i++ {
	//	commandShares[i] = &CommandShare{
	//		Ballot:  op.ballot,
	//		Command: ssCommand[i],
	//	}
	//}
	//op.log[op.slot].commandShares = commandShares
	// TODO: decode []byte command become a struct
}

func (op *OPaxos) HandlePrepareResponse(m P1b) {
	// update log, store the cmdShares for reconstruction, if necessary.
	if len(m.Log) > 0 {
		op.updateLog(m.Log)
	}

	// handle old message from the previous leadership
	if m.Ballot < op.ballot || op.IsLeader {
		return
	}

	// yield to another proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
		// TODO: forward message to the new leader
		return
	}

	// ack message, if the response was sent for this proposer
	if m.Ballot == op.ballot && m.Ballot.ID() == op.ID() {
		op.quorum.ACK(m.ID)

		// phase-1 quorum is fulfilled, this proposer is a leader now
		if op.Q1(op.quorum) {
			op.IsLeader = true

			// propose any uncommitted entries,
			// this happened when other leaders yield down before
			// the entries executed by this node.
			op.proposeUncommittedEntries()

			// propose new commands, until it is empty
			numPendingRequests := len(op.pendingCommands)
			for i := 0; i < numPendingRequests; i++ {
				op.Propose(<-op.pendingCommands)
			}
		}
	}
}

func (op *OPaxos) proposeUncommittedEntries() {
	for i := op.execute; i <= op.slot; i++ {

		// for now, we are ignoring a nil gap
		if op.log[i] == nil || op.log[i].commit {
			continue
		}

		op.log[i].ballot = op.ballot
		op.log[i].quorum = paxi.NewQuorum()

		// check if there are previously accepted command-shares in this slot,
		// proposed by different leader
		if len(op.log[i].commandShares) > 0 {
			// if there are less than K command-shares, we can ignore them, reset the shares.
			// but if there are more than or exactly K command-shares, we try to reconstruct them
			if len(op.log[i].commandShares) >= op.K {
				// get K command-shares with the biggest ballot number
				ballotShares := map[paxi.Ballot][][]byte{}
				for j := 0; j < len(op.log[i].commandShares); j++ {
					ballotShares[op.log[i].commandShares[j].Ballot] = append(
						ballotShares[op.log[i].commandShares[j].Ballot], op.log[i].commandShares[j].Command)
				}
				var acceptedCmdBallot *paxi.Ballot = nil
				for ballot, cmdShares := range ballotShares {
					if len(cmdShares) >= op.K && (acceptedCmdBallot == nil || ballot > *acceptedCmdBallot) {
						acceptedCmdBallot = &ballot
					}
				}

				if acceptedCmdBallot == nil {
					log.Errorf("this should not happen, a proposer get phase-1 quorum without seeing k command-shares")
					continue
				}

				reconstructedCmd, err := shamir.Combine(ballotShares[*acceptedCmdBallot])
				if err != nil {
					log.Errorf("failed to reconstruct command in slot %d: %v", i, err)
					continue
				}
				op.log[i].command.Data = reconstructedCmd
			}
			op.log[i].commandShares = nil
		}

		if len(op.log[i].command.Data) == 0 {
			// TODO: after reconstruction, the command is still empty, we need to propose NULL value (command)
			// TODO: so we can "skip" this slot. For now we are skipping this slot.
			continue
		}

		// regenerate secret-shared command
		newSSCommands, ssTime, err := op.defaultSSWorker.secretShareCommand(op.log[i].command.Data)
		if err != nil {
			log.Errorf("failed to secret share command %v", err)
			continue
		}

		// reset the request for this slot, we can not reply to the client
		// anymore, since this request initially might be sent to other proposer
		// not this one.
		bc := paxi.BytesCommand(op.log[i].command.Data)
		op.log[i].command = &paxi.ClientBytesCommand{
			BytesCommand: &bc,
			RPCMessage:   nil,
		}
		op.log[i].ssTime = ssTime
		op.log[i].timestamp = time.Now()
		op.log[i].quorum.ACK(op.ID())

		// TODO: broadcast clear message to other proposer, secret shared message to learner
		proposeRequests := make([]interface{}, op.N-1)
		for j := 0; j < len(newSSCommands); j++ {
			proposeRequests[i] = P2a{
				Ballot:  op.log[i].ballot,
				Slot:    i,
				Command: newSSCommands[j],
			}
		}
		if op.config.Thrifty {
			op.MulticastQuorumUniqueMessage(op.config.Protocol.Quorum2-1, proposeRequests)
		} else {
			op.MulticastUniqueMessage(proposeRequests)
		}
	}
}

func (op *OPaxos) updateLog(acceptedCmdShares map[int]CommandShare) {
	for slot, cmdShare := range acceptedCmdShares {
		op.slot = paxi.Max(op.slot, slot)
		if e, exist := op.log[slot]; exist {
			if !e.commit && cmdShare.Ballot > e.ballot {
				e.ballot = cmdShare.Ballot
				e.commandShares = append(e.commandShares, &cmdShare)
			}
		} else {
			op.log[slot] = &entry{
				// TODO: only do this if the acceptor is also a proposer
				// command:       cmdShare.Command,
				ballot:        cmdShare.Ballot,
				commandShares: []*CommandShare{&cmdShare},
				commit:        false,
			}
		}
	}
}

func (op *OPaxos) HandleProposeResponse(m P2b) {
	// handle old message and committed command
	e, exist := op.log[m.Slot]
	if !exist || m.Ballot < e.ballot || e.commit {
		return
	}

	// log.Infof("for slot=%d, time until received by leader %v", m.Slot, time.Since(m.SendTime))

	// yield to other proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
	}

	if m.Ballot.ID() == op.ID() && m.Ballot == op.log[m.Slot].ballot {
		op.log[m.Slot].quorum.ACK(m.ID)
		if op.Q2(op.log[m.Slot].quorum) {
			op.log[m.Slot].commit = true
			op.Broadcast(P3{
				Ballot: m.Ballot,
				Slot:   m.Slot,
			})

			// update execute slot idx
			op.exec()
		}
	}
}
