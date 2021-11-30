package opaxos

import (
	"encoding/json"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/hashicorp/vault/shamir"
	"strconv"
	"time"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config []paxi.ID
	K      int // minimum number of secret shares to make it reconstructible

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	active  bool           // active leader
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewOPaxos creates new OPaxos instance
func NewOPaxos(n paxi.Node, ssThreshold int, options ...func(*OPaxos)) *OPaxos {
	op := &OPaxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make([]*paxi.Request, 0),
		K:               ssThreshold,
		Q1:              func(q *paxi.Quorum) bool { return q.MajorityWithKIntersection(ssThreshold) },
		Q2:              func(q *paxi.Quorum) bool { return q.MajorityWithKIntersection(ssThreshold) },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(op)
	}

	return op
}

// HandleRequest handles request and start phase 1 or phase 2
func (op *OPaxos) HandleRequest(r paxi.Request) {
	if !op.active {
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
	if op.active {
		return
	}
	op.ballot.Next(op.ID())
	op.quorum.Reset()
	// since this replica is a proposer, it does not ack the message
	// op.quorum.ACK(op.ID())
	op.Broadcast(PrepareRequest{Ballot: op.ballot})
}

// Propose initiates phase 2 of opaxos
func (op *OPaxos) Propose(r *paxi.Request) {
	op.slot++
	op.log[op.slot] = &entry{
		ballot:    op.ballot,
		command:   r.Command,
		request:   r,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}
	// since this replica is a proposer, it does not ack the message
	// op.log[op.slot].quorum.ACK(op.ID())

	// TODO: do secret sharing here correctly
	//var cmdBytes bytes.Buffer
	//gobEncoder := gob.NewEncoder(&cmdBytes)
	//if err := gobEncoder.Encode(r.Command); err != nil {
	//	log.Errorf("failed to secret shares client's value, %v\n", err)
	//	return
	//}
	s := time.Now()
	cmdBytes, err := json.Marshal(r.Command)
	if err != nil {
		log.Errorf("failed to encode command %v\n", err)
		return
	}
	log.Infof("time to encode command %v\n", time.Since(s))
	s = time.Now()
	secretShares, err := shamir.Split(cmdBytes, paxi.GetConfig().N()-1, op.K)
	if err != nil {
		log.Errorf("failed to split secret %v\n", err)
	}
	log.Infof("time to secret share command %v\n", time.Since(s))

	s = time.Now()
	for i := 0; i < len(secretShares); i++ {
		share := secretShares[i]
		m := ProposeRequest{
			Ballot:       op.ballot,
			Slot:         op.slot,
			CommandShare: share,
			SendTime:     s,
			//Command:      r.Command,
		}
		op.Send(paxi.NewID(1, 2+i), m)
	}
	log.Infof("time to broadcast propose messages to %d nodes: %v\n", len(secretShares), time.Since(s))
}

func (op *OPaxos) HandlePrepareRequest(m PrepareRequest) {
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
		op.active = false
	}

	l := make(map[int]CommandBallot)
	for s := op.execute; s <= op.slot; s++ {
		if op.log[s] == nil || op.log[s].commit {
			continue
		}
		l[s] = CommandBallot{op.log[s].command, op.log[s].ballot}
	}

	// Send PrepareResponse back to proposer / leader
	op.Send(m.Ballot.ID(), PrepareResponse{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
}

func (op *OPaxos) HandlePrepareResponse(m PrepareResponse) {
	// TODO: update our log

	// handle old message
	if m.Ballot < op.ballot || op.active {
		return
	}

	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
	}

	// ack message
	if m.Ballot.ID() == op.ID() && m.Ballot == op.ballot {
		op.quorum.ACK(m.ID)
		if op.Q1(op.quorum) {
			op.active = true

			// propose any uncommitted entries
			for i := op.execute; i <= op.slot; i++ {
				// ignore nil gap or committed log
				if op.log[i] == nil || op.log[i].commit {
					continue
				}
				op.log[i].ballot = op.ballot
				op.log[i].quorum = paxi.NewQuorum()

				op.Broadcast(ProposeRequest{
					Ballot:  op.ballot,
					Slot:    i,
					Command: op.log[i].command,
				})
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
	if m.Ballot >= op.ballot {
		op.ballot = m.Ballot
		op.active = false

		log.Infof("message %v acked after %v\n", m.Slot, time.Since(m.SendTime))

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// update entry
		if e, exists := op.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				if !e.command.Equal(m.Command) && e.request != nil {
					e.request = nil
				}
			}
			e.command = m.Command
			e.ballot = m.Ballot
		} else {
			op.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false,
			}
		}
	}

	// reply to proposer
	op.Send(m.Ballot.ID(), ProposeResponse{
		Ballot: op.ballot,
		Slot:   m.Slot,
		ID:     op.ID(),
		SendTime: time.Now(),
	})
}

func (op *OPaxos) HandleProposeResponse(m ProposeResponse) {
	// handle old message
	entry, exist := op.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	log.Infof("%d time until received by leader %v", m.Slot, time.Since(m.SendTime))

	// update the highest ballot
	if m.Ballot > op.ballot {
		op.ballot = m.Ballot
	}

	if m.Ballot.ID() == op.ID() && m.Ballot == op.log[m.Slot].ballot {
		op.log[m.Slot].quorum.ACK(m.ID)
		if op.Q2(op.log[m.Slot].quorum) {
			op.log[m.Slot].commit = true
			op.Broadcast(CommitRequest{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: op.log[m.Slot].command,
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
		if !e.command.Equal(m.Command) && e.request != nil {
			e.request = nil
		}
	} else {
		op.log[m.Slot] = &entry{}
		e = op.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true

	op.exec()
}

func (op *OPaxos) exec() {
	for {
		e, ok := op.log[op.execute]
		if !ok || !e.commit {
			break
		}
		// log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		log.Infof("%d time from created until executed %v", op.execute, time.Since(e.timestamp))
		value := op.Execute(e.command)
		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(op.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(op.execute)
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
	}
}
