package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/shamir"
	"time"
)

// Prepare initiates phase 1 of opaxos
func (op *OPaxos) Prepare() {
	if op.isLeader {
		return
	}
	op.ballot.Next(op.ID())
	op.quorum.Reset()

	op.persistHighestBallot(op.ballot)
	op.quorum.ACK(op.ID())

	log.Debugf("broadcasting prepare message %s", op.ballot.String())
	op.Broadcast(P1a{Ballot: op.ballot})
}

// Propose initiates phase 2 of OPaxos
func (op *OPaxos) Propose(r *SecretSharedCommand) {
	// prepare places for proposal
	proposalShares := make([][]SecretShare, op.N-1)

	// prepare batch of commands to be proposed
	batchSize := len(op.onOffPendingCommands) + 1
	if batchSize > paxi.MaxBatchSize {
		batchSize = paxi.MaxBatchSize
	}
	commands := make([]paxi.BytesCommand, batchSize)
	commandsHandler := make([]*paxi.RPCMessage, batchSize)
	ssEncTimes := make([]time.Duration, batchSize)
	sharesBatch := make([]SecretShare, batchSize)

	// handle first command r in the batch
	commands[0] = *r.BytesCommand
	commandsHandler[0] = r.RPCMessage
	ssEncTimes[0] = r.SSTime
	for i := 0; i < op.N-1; i++ {
		proposalShares[i] = append(proposalShares[i], r.Shares[i])
	}

	// handle the remaining commands in the batch
	for i := 1; i < batchSize; i++ {
		cmd := <-op.onOffPendingCommands
		commands[i] = *cmd.BytesCommand
		commandsHandler[i] = cmd.RPCMessage
		ssEncTimes[i] = cmd.SSTime
		for j := 0; j < op.N-1; j++ {
			proposalShares[j] = append(proposalShares[j], cmd.Shares[j])
		}
	}
	log.Debugf("batching %d commands", batchSize)

	// prepare the entry that contains a batch of commands
	op.slot++
	op.log[op.slot] = &entry{
		ballot:          op.ballot,
		oriBallot:       op.ballot,
		commands:        commands,
		commandsHandler: commandsHandler,
		sharesBatch:     sharesBatch,
		commit:          false,
		quorum:          paxi.NewQuorum(),
		ssTime:          ssEncTimes,
	}

	log.Debugf("get batch of commands for slot %d", op.slot)

	op.persistAcceptedValues(op.slot, op.ballot, op.ballot, commands)
	op.log[op.slot].quorum.ACK(op.ID())

	// preparing different proposal for each acceptors
	proposeRequests := make([]interface{}, op.N-1)
	for i := 0; i < op.N-1; i++ {
		proposeRequests[i] = P2a{
			Ballot:      op.ballot,
			Slot:        op.slot,
			SharesBatch: proposalShares[i],
			OriBallot:   op.ballot,
		}
	}

	// broadcast propose message to the acceptors
	op.MulticastUniqueMessage(proposeRequests)
}

func (op *OPaxos) HandlePrepareResponse(m P1b) {
	log.Debugf("handling prepare response %s %d", m, op.isLeader)

	// update log, store the cmdShares for reconstruction, if necessary.
	op.updateLog(m.Log)

	// handle old message from the previous leadership
	if m.Ballot < op.ballot || op.isLeader {
		return
	}

	// yield to another proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.persistHighestBallot(m.Ballot)
		op.ballot = m.Ballot
		op.isLeader = false
		op.onOffPendingCommands = nil
		return
	}

	// ack message, if the response was sent for this proposer
	if m.Ballot == op.ballot && m.Ballot.ID() == op.ID() {
		op.quorum.ACK(m.ID)

		// phase-1 quorum is fulfilled, this proposer is a leader now
		if op.Q1(op.quorum) {
			op.isLeader = true

			// propose any uncommitted entries,
			// this happened when other leaders yield down before
			// the entries are committed by this node.
			op.proposeUncommittedEntries()

			// propose pending commands
			op.onOffPendingCommands = op.pendingCommands
			log.Debugf("opening pending commands channel len=%d, %d %d", len(op.pendingCommands), len(op.ssJobs), len(op.rawCommands))
		}
	}
}

func (op *OPaxos) updateLog(acceptedCmdShares map[int]CommandShare) {
	if len(acceptedCmdShares) == 0 {
		return
	}

	for slot, cmdShare := range acceptedCmdShares {
		op.slot = paxi.Max(op.slot, slot)
		if e, exist := op.log[slot]; exist {
			if !e.commit && cmdShare.Ballot > e.ballot {
				e.ballot = cmdShare.Ballot
				// store the secret-share from the acceptor
				e.commandShares = append(e.commandShares, &cmdShare)
			}
		} else {
			op.log[slot] = &entry{
				ballot:          cmdShare.Ballot,
				oriBallot:       cmdShare.OriBallot,
				commands:        nil,
				commandsHandler: nil,
				sharesBatch:     cmdShare.SharesBatch,
				commit:          false,
				commandShares:   []*CommandShare{&cmdShare}, // store the secret-share from the acceptor
			}
		}
	}
}

// proposeUncommittedEntries does recovery process
func (op *OPaxos) proposeUncommittedEntries() {
	for i := op.execute; i <= op.slot; i++ {

		// for now, we are ignoring a nil gap
		if op.log[i] == nil || op.log[i].commit {
			continue
		}

		op.log[i].ballot = op.ballot
		op.log[i].quorum = paxi.NewQuorum()

		// prepare places for new proposal
		proposalShares := make([][]SecretShare, op.N-1)

		// check if there are previously accepted command-shares in this slot,
		// proposed by different leader
		isValueRecovered := false
		if len(op.log[i].commandShares) > 0 {
			// find share that has the highest accepted ballot
			highestBallot := op.log[i].commandShares[0].Ballot
			maxShareOriBallot := op.log[i].commandShares[0].OriBallot
			for _, share := range op.log[i].commandShares {
				if share.Ballot > highestBallot {
					highestBallot = share.Ballot
					maxShareOriBallot = share.OriBallot
				}
			}

			// collect shares which OriBallot == maxShareOriBallot (have same ID)
			recoveryShares := make([]*CommandShare, 0)
			for _, share := range op.log[i].commandShares {
				if share.OriBallot == maxShareOriBallot {
					recoveryShares = append(recoveryShares, share)
				}
			}

			// if there are at least T shares, then the value might be chosen, we need to recover it
			if len(recoveryShares) >= op.T {
				isValueRecovered = true

				// check the batch size, all shares must have the same bath size
				batchSize := len(recoveryShares[0].SharesBatch)
				for _, share := range recoveryShares {
					if len(share.SharesBatch) != batchSize {
						log.Fatalf("found share with different batch size (s=%d, b=%s, ob=%s) from %s",
							i, share.Ballot, share.OriBallot, share.ID,
						)
					}
				}

				// recovery process: regenerate the shares for each reconstructed command in the batch
				allSharesBatch := make([]*[][]byte, batchSize)
				commandBatch := make([]paxi.BytesCommand, batchSize)
				for j := 0; j < batchSize; j++ {
					ss := make([][]byte, 0)
					for k := 0; k < len(recoveryShares); k++ {
						ss = append(ss, recoveryShares[k].SharesBatch[j])
					}
					newShares, err := shamir.Regenerate(ss, op.N)
					if err != nil {
						log.Fatalf("failed to reconstruct command (s=%d, ob=%s)",
							i, maxShareOriBallot,
						)
					}
					plainCmd, err := shamir.Combine(newShares)
					log.Debugf("reconstructed new command: %x", plainCmd)

					genericCmd, err := paxi.UnmarshalGenericCommand(plainCmd)
					if err != nil {
						log.Fatalf("the reconstructed value is not a valid command: %v", err)
					}

					allSharesBatch[j] = &newShares
					commandBatch[j] = genericCmd.ToBytesCommand()
				}

				// put the recovered commands to the consensus instance (entry)
				op.log[i].oriBallot = maxShareOriBallot
				op.log[i].commands = commandBatch
				op.log[i].commandsHandler = nil
				op.log[i].sharesBatch = nil
				op.log[i].commit = false
				op.log[i].ssTime = nil
				op.log[i].commandShares = nil

				// distribute the secret-shares to each acceptors
				for j := 0; j < batchSize; j++ {
					op.log[i].sharesBatch = append(op.log[i].sharesBatch, (*allSharesBatch[j])[0])
					for k := 0; k < op.N-1; k++ {
						proposalShares[k] = append(proposalShares[k], (*allSharesBatch[j])[k+1])
					}
				}
			}
		}

		// if no value recovered, this proposer can propose any value
		if !isValueRecovered {
			// TODO: implement this
			panic("unimplemented")
		}

		// preparing new proposal
		op.log[i].quorum.ACK(op.ID())

		// preparing different proposal for each acceptors
		proposeRequests := make([]interface{}, op.N-1)
		for j := 0; j < op.N-1; j++ {
			proposeRequests[j] = P2a{
				Ballot:      op.log[i].ballot,
				Slot:        i,
				SharesBatch: proposalShares[j],
				OriBallot:   op.log[i].oriBallot,
			}
		}

		op.MulticastUniqueMessage(proposeRequests)
	}
}

func (op *OPaxos) HandleProposeResponse(m P2b) {
	// handle old message and committed command
	e, exist := op.log[m.Slot]
	if !exist || m.Ballot < e.ballot || e.commit {
		return
	}

	// yield to other proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.isLeader = false
		op.onOffPendingCommands = nil
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
