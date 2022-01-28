package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/krawczyk"
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
	op.quorum.ACK(op.ID())

	if paxi.GetConfig().Thrifty {
		op.MulticastQuorum(op.nQuorum1, PrepareRequest{Ballot: op.ballot})
	} else {
		op.Broadcast(PrepareRequest{Ballot: op.ballot})
	}
}

// Propose initiates phase 2 of opaxos
func (op *OPaxos) Propose(r *paxi.GenericRequest) {
	// secret shared the command
	ssCommand, encodingDuration, err := op.secretSharesCommand(r.GenericCommand)
	if err != nil {
		log.Errorf("failed to secret share command %v", err)
		return
	}

	op.slot++

	commandShares := make([]CommandShare, len(ssCommand))
	for i := 0; i < len(ssCommand); i++ {
		commandShares[i] = CommandShare{
			Command: ssCommand[i],
			Ballot:  op.ballot,
		}
	}

	op.log[op.slot] = &entry{
		ballot:        op.ballot,
		command:       r.GenericCommand,
		commandShares: commandShares,
		request:       r,
		quorum:        paxi.NewQuorum(),
		timestamp:     time.Now(),
		encodingTime:  *encodingDuration,
	}
	op.log[op.slot].quorum.ACK(op.ID())

	// TODO: broadcast clear message to other proposer, secret shared message to learner
	// for now we are sending secret shared only
	proposeRequests := make([]interface{}, len(ssCommand))
	for i := 0; i < len(ssCommand); i++ {
		proposeRequests[i] = ProposeRequest{
			Ballot:  op.ballot,
			Slot:    op.slot,
			Command: ssCommand[i],
		}
	}

	if paxi.GetConfig().Thrifty {
		op.MulticastQuorumUniqueMessage(op.nQuorum2, proposeRequests)
	} else {
		op.MulticastUniqueMessage(proposeRequests)
	}
}

func (op *OPaxos) secretSharesCommand(cmdBytes []byte) ([][]byte, *time.Duration, error) {
	var err error
	var secretShares [][]byte

	s := time.Now()

	if op.algorithm == AlgShamir {
		if paxi.GetConfig().Thrifty {
			secretShares, err = shamir.Split(cmdBytes, op.nQuorum2, op.K)
		} else {
			secretShares, err = shamir.Split(cmdBytes, op.N-1, op.K)
		}
	} else if op.algorithm == AlgSSMS {
		if paxi.GetConfig().Thrifty {
			secretShares, err = krawczyk.Split(cmdBytes, op.nQuorum2, op.K)
		} else {
			secretShares, err = krawczyk.Split(cmdBytes, op.N-1, op.K)
		}
	} else {
		nShares := op.nQuorum2
		if !paxi.GetConfig().Thrifty {
			nShares = op.N - 1
		}
		secretShares = make([][]byte, nShares)
		for i := 0; i < nShares; i++ {
			secretShares[i] = cmdBytes
		}
	}

	ssTime := time.Since(s)

	if err != nil {
		log.Errorf("failed to split secret %v\n", err)
		return nil, nil, err
	}

	log.Debugf("cmd length: before=%d, after=%d. processing-time=%v, #N=%d, #k=%d", len(cmdBytes), len(secretShares[0]), ssTime, op.N-1, op.K)

	return secretShares, &ssTime, nil
}

func (op *OPaxos) HandlePrepareResponse(m PrepareResponse) {
	// update log, store the cmdShares for reconstruction, if necessary.
	op.updateLog(m.Log)

	// handle old message from the previous leadership
	if m.Ballot < op.ballot || op.IsLeader {
		return
	}

	// yield to another proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
		// TODO: forward message to the new leader, for now we assume one proposer deployment
	}

	// ack message, if the response was sent for this proposer
	if m.Ballot == op.ballot && m.Ballot.ID() == op.ID() {
		op.quorum.ACK(m.ID)

		if op.Q1(op.quorum) {
			op.IsLeader = true

			// propose any uncommitted entries
			for i := op.execute; i <= op.slot; i++ {
				// ignore nil gap or committed log
				// TODO: need to propose nil value as well
				if op.log[i] == nil || op.log[i].commit {
					continue
				}

				op.log[i].ballot = op.ballot
				op.log[i].quorum = paxi.NewQuorum()

				var ssCommands [][]byte

				// check if there are previously accepted command-shares in this slot, proposed by different leader
				if len(op.log[i].commandShares) > 0 && op.log[i].ballot != op.ballot {
					// if there are less than K command-shares, we can ignore them, reset the shares.
					// if there are more than or exactly K command share, we need to reconstruct them
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
						op.log[i].command = reconstructedCmd
					}
					op.log[i].commandShares = []CommandShare{}
				}

				// regenerate secret-shares, if empty
				if len(op.log[i].commandShares) == 0 {
					newSSCommands, encDuration, err := op.secretSharesCommand(op.log[i].command)
					if err != nil {
						log.Errorf("failed to secret share command %v", err)
						continue
					}
					commandShares := make([]CommandShare, len(newSSCommands))
					for i := 0; i < len(newSSCommands); i++ {
						commandShares[i] = CommandShare{
							Command: newSSCommands[i],
							Ballot:  op.ballot,
						}
					}
					op.log[i].commandShares = commandShares
					op.log[i].encodingTime = *encDuration
					ssCommands = newSSCommands
				}

				// TODO: broadcast clear message to other proposer, secret shared message to learner
				proposeRequests := make([]interface{}, op.N-1)
				for i := 0; i < op.N-1; i++ {
					proposeRequests[i] = ProposeRequest{
						Ballot:  op.ballot,
						Slot:    i,
						Command: ssCommands[i+1],
					}
				}
				op.MulticastUniqueMessage(proposeRequests)
			}

			// propose new commands
			for _, req := range op.requests {
				op.Propose(req)
			}

			// reset new requests
			op.requests = make([]*paxi.GenericRequest, 0)
		}
	}
}

func (op *OPaxos) updateLog(acceptedCmdShares map[int]CommandShare) {
	for slot, cmdShare := range acceptedCmdShares {
		op.slot = paxi.Max(op.slot, slot)
		if e, exist := op.log[slot]; exist {
			if !e.commit && cmdShare.Ballot > e.ballot {
				e.ballot = cmdShare.Ballot
				e.commandShares = append(e.commandShares, cmdShare)
			}
		} else {
			op.log[slot] = &entry{
				ballot:        cmdShare.Ballot,
				command:       cmdShare.Command,
				commandShares: []CommandShare{cmdShare},
				commit:        false,
			}
		}
	}
}

func (op *OPaxos) HandleProposeResponse(m ProposeResponse) {
	// handle old message and committed command
	e, exist := op.log[m.Slot]
	if !exist || m.Ballot < e.ballot || e.commit {
		return
	}

	//log.Infof("for slot=%d, time until received by leader %v", m.Slot, time.Since(m.SendTime))

	// yield to other proposer with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
	}

	if m.Ballot.ID() == op.ID() && m.Ballot == op.log[m.Slot].ballot {
		op.log[m.Slot].quorum.ACK(m.ID)
		if op.Q2(op.log[m.Slot].quorum) {
			op.log[m.Slot].commit = true
			op.Broadcast(CommitRequest{
				Ballot: m.Ballot,
				Slot:   m.Slot,
			})

			// update execute slot idx
			op.exec()
		}
	}
}