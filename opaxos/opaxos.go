package opaxos

import (
	"encoding/json"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/shamir"
	"strconv"
	"time"
)

// entry in log
type entry struct {
	ballot       paxi.Ballot
	command      paxi.Command  // clear command
	ssCommand    ClientCommand // secret shared command
	commit       bool          // commit indicates whether this entry is already committed or not
	request      *paxi.Request // each request has reply channel, so we need to store it
	quorum       *paxi.Quorum
	timestamp    time.Time
	encodingTime time.Duration
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     []paxi.ID
	K          int // the minimum number of secret shares to make it reconstructible
	N          int // the number of nodes
	IsProposer bool
	IsAcceptor bool
	IsLearner  bool
	IsLeader   bool // IsLeader indicates whether this instance perceives itself as a leader or not

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests

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
func NewOPaxos(n paxi.Node, ssThreshold int, roles []string, options ...func(*OPaxos)) *OPaxos {

	var quorum1 = (n.GetConfig().N() / 2) + ssThreshold
	var quorum2 = (n.GetConfig().N() / 2) + 1
	if n.GetConfig().N()%2 == 0 {
		quorum1 -= 1
	}

	op := &OPaxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make([]*paxi.Request, 0),
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
func (op *OPaxos) HandleRequest(r paxi.Request) {
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
func (op *OPaxos) Propose(r *paxi.Request) {
	// secret shared the command
	ssCommand, encodingDuration, err := op.secretSharesCommand(r.Command)
	if err != nil {
		log.Errorf("failed to secret share command %v", err)
		return
	}

	op.slot++
	op.log[op.slot] = &entry{
		ballot:       op.ballot,
		command:      r.Command,
		ssCommand:    ClientCommand{ssCommand[0], r.Command.ClientID},
		request:      r,
		quorum:       paxi.NewQuorum(),
		timestamp:    time.Now(),
		encodingTime: *encodingDuration,
	}
	op.log[op.slot].quorum.ACK(op.ID())

	// TODO: broadcast clear message to other proposer, secret shared message to learner
	proposeRequests := make([]interface{}, len(ssCommand))
	for i := 0; i < len(ssCommand); i++ {
		proposeRequests[i] = ProposeRequest{
			Ballot:   op.ballot,
			Slot:     op.slot,
			Command:  ClientCommand{ssCommand[i], r.Command.ClientID},
			SendTime: time.Now(),
		}
	}

	if paxi.GetConfig().Thrifty {
		op.MulticastQuorumUniqueMessage(op.nQuorum2, proposeRequests)
	} else {
		op.MulticastUniqueMessage(proposeRequests)
	}
}

func (op *OPaxos) secretSharesCommand(c paxi.Command) ([][]byte, *time.Duration, error) {
	log.Debugf("proposer: secret share the proposed value")

	// TODO: do secret sharing here correctly
	//var cmdBytes bytes.Buffer
	//gobEncoder := gob.NewEncoder(&cmdBytes)
	//if err := gobEncoder.Encode(r.Command); err != nil {
	//	log.Errorf("failed to secret shares client's value, %v\n", err)
	//	return
	//}

	s := time.Now()
	cmdBytes, err := json.Marshal(c)
	if err != nil {
		log.Errorf("failed to encode command %v\n", err)
		return nil, nil, err
	}
	//log.Debugf("time to encode command %v\n", time.Since(s))

	//s = time.Now()
	secretShares := [][]byte{}
	if paxi.GetConfig().Thrifty {
		secretShares, err = shamir.Split(cmdBytes, op.nQuorum2, op.K)
	} else {
		secretShares, err = shamir.Split(cmdBytes, op.N-1, op.K)
	}
	if err != nil {
		log.Errorf("failed to split secret %v\n", err)
		return nil, nil, err
	}
	//log.Infof("time to secret share command %v\n", time.Since(s))
	duration := time.Since(s)

	return secretShares, &duration, nil
}

func (op *OPaxos) HandlePrepareRequest(m PrepareRequest) {
	// handle if there is a new leader with higher ballot number
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false
	}

	l := make(map[int]CommandBallot)
	for s := op.execute; s <= op.slot; s++ {
		if op.log[s] == nil || op.log[s].commit {
			continue
		}
		l[s] = CommandBallot{op.log[s].ssCommand, op.log[s].ballot}

		// TODO: if this is a proposer, send clear command to the new leader, instead of the secret shared one.
	}

	// Send PrepareResponse back to proposer / leader
	op.Send(m.Ballot.ID(), PrepareResponse{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
}

func (op *OPaxos) HandlePrepareResponse(m PrepareResponse) {
	// handle old message from the previous leadership
	if m.Ballot < op.ballot || op.IsLeader {
		return
	}

	// update current leadership
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
	}

	// ack message
	if m.Ballot == op.ballot && m.Ballot.ID() == op.ID() {
		op.quorum.ACK(m.ID)

		// TODO: store the PrepareResponse for reconstruction, if necessary.

		if op.Q1(op.quorum) {
			op.IsLeader = true

			// propose any uncommitted entries
			for i := op.execute; i <= op.slot; i++ {
				// ignore nil gap or committed log
				if op.log[i] == nil || op.log[i].commit {
					continue
				}
				op.log[i].ballot = op.ballot
				op.log[i].quorum = paxi.NewQuorum()

				ssCommand, encDuration, err := op.secretSharesCommand(op.log[i].command)
				if err != nil {
					log.Errorf("failed to secret share command %v", err)
					return
				}
				op.log[i].encodingTime = *encDuration

				// TODO: broadcast clear message to other proposer, secret shared message to learner
				proposeRequests := make([]interface{}, op.N-1)
				for i := 0; i < op.N-1; i++ {
					proposeRequests[i] = ProposeRequest{
						Ballot:   op.ballot,
						Slot:     i,
						Command:  ClientCommand{ssCommand[i+1], op.log[i].ssCommand.ClientID},
						SendTime: time.Now(),
					}
				}
				op.MulticastUniqueMessage(proposeRequests)
			}

			// propose new commands
			for _, req := range op.requests {
				op.Propose(req)
			}

			// reset new requests
			op.requests = make([]*paxi.Request, 0)
		}
	}
}

func (op *OPaxos) HandleProposeRequest(m ProposeRequest) {

	// TODO: handle if this is another proposer

	if m.Ballot >= op.ballot {
		op.ballot = m.Ballot
		op.IsLeader = false

		//log.Infof("message slot=%v acked after %v\n", m.Slot, time.Since(m.SendTime))

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// update entry
		if e, exists := op.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot && e.request != nil {
				e.request = nil
			}
			e.ssCommand = m.Command
			e.ballot = m.Ballot
		} else {
			op.log[m.Slot] = &entry{
				ballot:    m.Ballot,
				ssCommand: m.Command,
				commit:    false,
			}
		}
	}

	// reply to proposer
	op.Send(m.Ballot.ID(), ProposeResponse{
		Ballot:   op.ballot,
		Slot:     m.Slot,
		ID:       op.ID(),
		SendTime: m.SendTime,
	})
}

func (op *OPaxos) HandleProposeResponse(m ProposeResponse) {
	// handle old message and committed command
	entry, exist := op.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	//log.Infof("for slot=%d, time until received by leader %v", m.Slot, time.Since(m.SendTime))

	// update the highest ballot
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
	}

	if m.Ballot.ID() == op.ID() && m.Ballot == op.log[m.Slot].ballot {
		op.log[m.Slot].quorum.ACK(m.ID)
		if op.Q2(op.log[m.Slot].quorum) {
			op.log[m.Slot].commit = true
			op.Broadcast(CommitRequest{
				Ballot:   m.Ballot,
				Slot:     m.Slot,
				ClientID: op.log[m.Slot].ssCommand.ClientID,
			})

			// update execute slot idx
			op.exec()
		}
	}
}

func (op *OPaxos) HandleCommitRequest(m CommitRequest) {
	op.slot = paxi.Max(op.slot, m.Slot)

	e, exist := op.log[m.Slot]
	if exist {
		if !e.ssCommand.ClientID.Equal(m.ClientID) && e.request != nil {
			e.request = nil
		}
	} else {
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

		// only learner what also a proposer that can execute the command
		value := paxi.Value{}
		if op.IsLearner && op.IsProposer {
			value = op.Execute(e.command)
		}

		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderEncodingTime] = strconv.Itoa(int(e.encodingTime.Milliseconds()))
			e.request.Reply(reply)
			e.request = nil
			//log.Infof("slot=%d time from received until executed %v", op.execute, time.Since(e.timestamp))
		}
		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}
