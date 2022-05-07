package paxos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
)

// entry in log
type entry struct {
	ballot  paxi.Ballot
	command *paxi.ClientBytesCommand
	commit  bool
	quorum  *paxi.Quorum
}

// Paxos instance
type Paxos struct {
	paxi.Node

	config []paxi.ID

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	active  bool           // active leader
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	quorum               *paxi.Quorum                  // phase 1 quorum
	requests             []*paxi.ClientBytesCommand    // phase 1 pending requests
	protocolMessages     chan interface{}              // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientBytesCommand // raw commands from clients
	pendingCommands      chan *paxi.ClientBytesCommand // pending commands ready to be proposed
	onOffPendingCommands chan *paxi.ClientBytesCommand // non nil pointer to pendingCommands after get response for phase 1

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	p := &Paxos{
		Node:                 n,
		log:                  make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:                 -1,
		quorum:               paxi.NewQuorum(),
		protocolMessages:     make(chan interface{}, paxi.GetConfig().ChanBufferSize),
		rawCommands:          make(chan *paxi.ClientBytesCommand, paxi.GetConfig().ChanBufferSize),
		pendingCommands:      make(chan *paxi.ClientBytesCommand, paxi.GetConfig().ChanBufferSize),
		onOffPendingCommands: nil,
		Q1:                   func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:                   func(q *paxi.Quorum) bool { return q.Majority() },
		ReplyWhenCommit:      false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func (p *Paxos) run() {
	var err error
	for err == nil {
		select {
		case cmd := <-p.rawCommands:
			// start phase 1 if this proposer has not started it previously
			if !p.active && p.ballot.ID() != p.ID() {
				p.P1a()
			}

			// put commands in the pendingCommands channel
			// the commands will be proposed after this node
			// successfully run phase-1
			// (onOffPendingCommands will point to pendingCommands)
			p.pendingCommands <- cmd
			numRawCmd := len(p.rawCommands)
			for numRawCmd > 0 {
				cmd = <-p.rawCommands
				p.pendingCommands <- cmd
				numRawCmd--
			}
			break

		// onOffPendingCommands is nil before this replica successfully running phase-1
		// see Paxos.HandleP1b for more detail
		case pCmd := <-p.onOffPendingCommands:
			p.P2a(pCmd)
			break

		// protocolMessages has higher priority.
		// We try to empty the protocolMessages in each loop since for every
		// client command potentially it will create O(N) protocol messages (propose & commit),
		// where N is the number of nodes in the consensus cluster
		case pcmd := <-p.protocolMessages:
			p.handleProtocolMessages(pcmd)
			numPMsg := len(p.protocolMessages)
			for numPMsg > 0 {
				p.handleProtocolMessages(<-p.protocolMessages)
				numPMsg--
			}
			break
		}
	}

	panic(fmt.Sprintf("paxos exited its main loop: %v", err))
}

func (p *Paxos) handleProtocolMessages(pmsg interface{}) {
	log.Debugf("receiving %v", pmsg)
	switch pmsg.(type) {
	case P1a:
		p.HandleP1a(pmsg.(P1a))
		break
	case P1b:
		p.HandleP1b(pmsg.(P1b))
		break
	case P2a:
		p.HandleP2a(pmsg.(P2a))
		break
	case P2b:
		p.HandleP2b(pmsg.(P2b))
		break
	case P3:
		p.HandleP3(pmsg.(P3))
		break
	default:
		log.Errorf("unknown protocol messages")
	}
}

// IsLeader indicates if this node is current leader
func (p *Paxos) IsLeader() bool {
	return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *Paxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
func (p *Paxos) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
func (p *Paxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// HandleRequest handles request and start phase 1 or phase 2
func (p *Paxos) HandleRequest(r *paxi.ClientBytesCommand) {
	if !p.active {
		p.requests = append(p.requests, r)

		// current phase 1 pending
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		p.P2a(r)
	}
}

// P1a starts phase 1 prepare
func (p *Paxos) P1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()

	//if err := p.storage.PersistBallot(p.ballot); err != nil {
	//	log.Errorf("failed to persist max ballot %v", err)
	//}

	p.quorum.ACK(p.ID())

	p.Broadcast(P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *paxi.ClientBytesCommand) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:  p.ballot,
		command: r,
		quorum:  paxi.NewQuorum(),
	}

	//if err := p.storage.PersistValue(p.slot, p.log[p.slot].command.ToBytesCommand()); err != nil {
	//	log.Errorf("failed to persist accepted value %v", err)
	//}

	p.log[p.slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Data,
	}

	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// HandleP1a handles P1a message
func (p *Paxos) HandleP1a(m P1a) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		//p.forward()
		// if len(p.requests) > 0 {
		// 	defer p.P1a()
		// }
	}

	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command.Data, p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command.Data = cb.Command
				bc := paxi.BytesCommand(cb.Command)
				e.command.BytesCommand = &bc
			}
		} else {
			bc := paxi.BytesCommand(cb.Command)
			p.log[s] = &entry{
				ballot: cb.Ballot,
				command: &paxi.ClientBytesCommand{
					BytesCommand: &bc,
				},
				commit: false,
			}
		}
	}
}

// HandleP1b handles P1b message
func (p *Paxos) HandleP1b(m P1b) {
	p.update(m.Log)

	// old message
	if m.Ballot < p.ballot || p.active {
		log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.onOffPendingCommands = nil
		// forward pending requests to new leader
		// p.forward()
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true

			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command.Data,
				})
			}

			// propose pending commands
			numPendingRequests := len(p.pendingCommands)
			for i := 0; i < numPendingRequests; i++ {
				pCmd := <-p.pendingCommands
				p.P2a(pCmd)
			}

			p.onOffPendingCommands = p.pendingCommands
		}
	}
}

// HandleP2a handles P2a message
func (p *Paxos) HandleP2a(m P2a) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	if m.Ballot >= p.ballot {
		//if m.Ballot != p.ballot {
		//	if err := p.storage.PersistBallot(p.ballot); err != nil {
		//		log.Errorf("failed to persist max ballot %v", err)
		//	}
		//}
		//if err := p.storage.PersistValue(p.slot, m.Command.ToBytesCommand()); err != nil {
		//	log.Errorf("failed to persist accepted value %v", err)
		//}
		p.ballot = m.Ballot
		p.active = false
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !bytes.Equal(e.command.Data, m.Command) && e.command.RPCMessage != nil {
					//p.Forward(m.Ballot.ID(), *e.request)
					// p.Retry(*e.request)
					e.command.RPCMessage = nil
				}
				e.command.Data = m.Command
				bc := paxi.BytesCommand(m.Command)
				e.command.BytesCommand = &(bc)
				e.ballot = m.Ballot
			}
		} else {
			bc := paxi.BytesCommand(m.Command)
			p.log[m.Slot] = &entry{
				ballot: m.Ballot,
				command: &paxi.ClientBytesCommand{
					BytesCommand: &bc,
					RPCMessage:   nil,
				},
				commit: false,
			}
		}
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

// HandleP2b handles P2b message
func (p *Paxos) HandleP2b(m P2b) {
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	// log.Infof("%d time until received by leader %v", m.Slot, time.Since(m.SendTime))

	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
		p.log[m.Slot].quorum.ACK(m.ID)
		if p.Q2(p.log[m.Slot].quorum) {
			p.log[m.Slot].commit = true
			p.Broadcast(P3{
				Ballot: m.Ballot,
				Slot:   m.Slot,
				//Command: p.log[m.Slot].command.Data,
			})

			//if p.ReplyWhenCommit {
			//	r := p.log[m.Slot].request
			//	r.Reply(paxi.Reply{
			//		Command:   r.Command,
			//		Timestamp: r.Timestamp,
			//	})
			//} else {
			//	p.exec()
			//}
			p.exec()
		}
	}
}

// HandleP3 handles phase 3 commit message
func (p *Paxos) HandleP3(m P3) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	p.slot = paxi.Max(p.slot, m.Slot)

	e, exist := p.log[m.Slot]
	//if exist {
	//	if !e.command.Equal(m.Command) && e.request != nil {
	//		// p.Retry(*e.request)
	//		p.Forward(m.Ballot.ID(), *e.request)
	//		e.request = nil
	//	}
	//} else {
	//	p.log[m.Slot] = &entry{}
	//	e = p.log[m.Slot]
	//}
	if !exist {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	//e.command = m.Command
	e.commit = true

	//if p.ReplyWhenCommit {
	//	if e.request != nil {
	//		e.request.Reply(paxi.Reply{
	//			Command:   e.request.Command,
	//			Timestamp: e.request.Timestamp,
	//		})
	//	}
	//} else {
	//	p.exec()
	//}
	p.exec()
}

func (p *Paxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		// log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)

		cmdReply := p.execCommands(e.command.BytesCommand, p.execute, e)

		//if err := p.storage.ClearValue(p.execute); err != nil {
		//	log.Errorf("failed to clear executed message %v", err)
		//}
		if e.command.RPCMessage != nil && e.command.RPCMessage.Reply != nil {
			//reply := paxi.Reply{
			//	Command:    e.command,
			//	Value:      value,
			//	Properties: make(map[string]string),
			//}
			//reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
			//reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			//reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
			//reply.Properties[HTTPHeaderEncodingTime] = "0"
			//e.request.Reply(reply)
			//e.request = nil

			err := e.command.RPCMessage.SendBytesReply(cmdReply.Marshal())
			if err != nil {
				log.Errorf("failed to send CommandReply %s", err)
			}
			e.command.RPCMessage = nil

		}

		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}

//func (p *Paxos) forward() {
//	for _, m := range p.requests {
//		p.Forward(p.ballot.ID(), *m)
//	}
//	p.requests = make([]*paxi.Request, 0)
//}

func (p *Paxos) execCommands(byteCmd *paxi.BytesCommand, slot int, e *entry) paxi.CommandReply {
	var cmd paxi.Command

	reply := paxi.CommandReply{
		OK:         true,
		Ballot:     "", // unused for now (always empty)
		Slot:       0,  // unused for now (always empty)
		EncodeTime: 0,
		SentAt:     0,
		Data:       nil,
	}

	if *paxi.ClientIsStateful {
		cmd = byteCmd.ToCommand()

	} else if *paxi.ClientIsStateful == false {
		gcmd := &paxi.GenericCommand{}
		err := msgpack.Unmarshal(*byteCmd, &gcmd)
		if err != nil {
			log.Fatalf("failed to unmarshal client's generic command %s", err.Error())
		}
		cmd.Key = paxi.Key(binary.BigEndian.Uint32(gcmd.Key))
		cmd.Value = gcmd.Value
		log.Debugf("sent time %v", gcmd.SentAt)
		reply.SentAt = gcmd.SentAt // forward sentAt from client back to client

	} else {
		log.Errorf("unknown client stateful property, dooes not know how to handle the command")
		reply.OK = false
	}

	value := p.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	log.Debugf("cmd=%v, value=%x", cmd, value)
	return reply
}
