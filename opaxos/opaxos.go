package opaxos

import (
	"bytes"
	"encoding/gob"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/shamir"
	"github.com/fadhilkurnia/shamir/krawczyk"
	"strconv"
	"time"
)

const AlgShamir = "shamir"
const AlgSSMS = "ssms"

// entry in log
type entry struct {
	ballot        paxi.Ballot
	command       []byte               // clear or secret shared command
	commit        bool                 // commit indicates whether this entry is already committed or not
	request       *paxi.GenericRequest // each request has reply channel, so we need to store it
	quorum        *paxi.Quorum
	timestamp     time.Time
	encodingTime  time.Duration
	commandShares []CommandShare // collection of client-command from acceptors, with the same slot number
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     []paxi.ID
	algorithm  string
	K          int // the minimum number of secret shares to make it reconstructive
	N          int // the number of nodes
	IsProposer bool
	IsAcceptor bool
	IsLearner  bool
	IsLeader   bool // IsLeader indicates whether this instance perceives itself as a leader or not

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	quorum   *paxi.Quorum           // quorum store all ack'd responses
	requests []*paxi.GenericRequest // phase 1 pending requests

	nQuorum1    int
	nQuorum2    int
	nQuorumFast int

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool

	// TODO: parse config so the OPaxos instance know the list of each roles.
	// A proposer can send a clear proposed value to other proposer, but need to
	// send a secret shared value to the acceptor.
	// proposerIDs []ID
	// acceptorIDs []ID
	// learnerIDs  []ID

	// TODO: a learner which also a proposer can execute command in the DB, but learner that not a proposer
	// just need to change the execute slot number, since it can not execute a secret shared command.

	// TODO: learner only node does not need http server
	// TODO: the OPaxos implementation should be oblivious with the command sent ([]byte), it is the replica's job
	// to parse and execute the command.
}

// NewOPaxos creates new OPaxos instance (constructor)
func NewOPaxos(n paxi.Node, ssThreshold int, algorithm string, roles []string, options ...func(*OPaxos)) *OPaxos {
	var quorum1 = (n.GetConfig().N() / 2) + ssThreshold
	var quorum2 = (n.GetConfig().N() / 2) + 1
	if n.GetConfig().N()%2 == 0 {
		quorum1 -= 1
	}

	op := &OPaxos{
		Node:            n,
		algorithm:       algorithm,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make([]*paxi.GenericRequest, 0),
		K:               ssThreshold,
		N:               n.GetConfig().N(),
		Q1:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(quorum1) },
		Q2:              func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(quorum2) },
		ReplyWhenCommit: false,
		nQuorum1:        quorum1,
		nQuorum2:        quorum2,
	}

	for _, opt := range options {
		opt(op)
	}

	// parse roles
	for _, r := range roles {
		if r == "proposer" {
			op.IsProposer = true
		}
		if r == "acceptor" {
			op.IsAcceptor = true
		}
		if r == "learner" {
			op.IsLearner = true
		}
	}

	return op
}

// HandleRequest handles request and start phase 1 or phase 2
func (op *OPaxos) HandleRequest(r paxi.GenericRequest) {
	if !op.IsLeader {
		op.requests = append(op.requests, &r)

		// start phase 1 if this replica has not started it previously
		if op.ballot.ID() != op.ID() {
			op.Prepare()
		}
	} else {
		op.Propose(&r)
	}
}

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
	s := time.Now()

	var err error
	var secretShares [][]byte

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
			nShares = op.N-1
		}
		secretShares = make([][]byte, nShares)
		for i := 0; i < nShares; i++ {
			secretShares[i] = cmdBytes
		}
	}

	if err != nil {
		log.Errorf("failed to split secret %v\n", err)
		return nil, nil, err
	}

	duration := time.Since(s)
	return secretShares, &duration, nil
}

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
		l[s] = CommandShare{op.log[s].command, op.log[s].ballot}
	}

	// Send PrepareResponse back to proposer / leader
	op.Send(m.Ballot.ID(), PrepareResponse{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
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

func (op *OPaxos) HandleProposeRequest(m ProposeRequest) {

	// TODO: handle if this is acceptor that also a proposer (clear command, instead of secret-shared command)

	if m.Ballot >= op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false

		//log.Infof("message slot=%v acked after %v\n", m.Slot, time.Since(m.SendTime))

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
		var cmd paxi.Command
		if op.IsLearner && op.IsProposer {
			decoder := gob.NewDecoder(bytes.NewReader(e.command))
			if err := decoder.Decode(&cmd); err != nil {
				log.Errorf("failed to decode user's command: %v", err)
				break
			}
			value = op.Execute(cmd)
		}

		if e.request != nil {
			reply := paxi.Reply{
				Command:    cmd,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderEncodingTime] = strconv.FormatInt(e.encodingTime.Nanoseconds(), 10)
			e.request.Reply(reply)
			e.request = nil
			//log.Infof("slot=%d time from received until executed %v", op.execute, time.Since(e.timestamp))
		}
		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}
