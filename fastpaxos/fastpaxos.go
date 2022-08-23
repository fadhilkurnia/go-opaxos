package fastpaxos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"math"
)

// entry in the log
type entry struct {
	ballot  paxi.Ballot              // the accepted ballot number
	isFast  bool                     // indicate whether ballot is a fast ballot or an ordinary one
	command *paxi.ClientBytesCommand // accepted command in []bytes, with reply writer
	commit  bool                     // commit indicates whether this entry is already committed or not
	quorum  *paxi.Quorum             // phase-2 quorum
}

// FastPaxos instance in a single Node
type FastPaxos struct {
	paxi.Node // extending generic Paxi Node

	log       map[int]*entry // log ordered by slot number
	execute   int            // next execute slot number
	ballot    paxi.Ballot    // the proposer's current ballot
	maxBallot paxi.Ballot    // the acceptor's highest promised ballot
	slot      int            // highest non-empty slot number

	protocolMessages chan interface{}              // receiver channel for prepare, propose, commit messages
	rawCommands      chan *paxi.ClientBytesCommand // raw commands from clients
	retryCommands    chan *paxi.ClientBytesCommand // retryCommands holds command that need to be reproposed due to conflict

	numQ2 int                            // numQ2 is the size of quorum for phase-2 (classic)
	numQF int                            // numQF is the size of fast quorum
	Q2    func(quorum *paxi.Quorum) bool // Q2 return true if there are ack from numQ2 acceptors
}

// NewFastPaxos is constructor for a new FastPaxos instance
func NewFastPaxos(n paxi.Node, options ...func(fp *FastPaxos)) *FastPaxos {
	numQ2 := int(math.Ceil(float64(n.GetConfig().N()) / 2))
	numQF := int(math.Ceil(float64(n.GetConfig().N()) * 3 / 4))

	fp := &FastPaxos{
		Node:             n,
		ballot:           paxi.NewBallot(0, n.ID()),
		maxBallot:        paxi.NewBallot(0, n.ID()),
		log:              make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:             -1,
		protocolMessages: make(chan interface{}, paxi.GetConfig().ChanBufferSize),
		rawCommands:      make(chan *paxi.ClientBytesCommand, paxi.GetConfig().ChanBufferSize),
		retryCommands:    make(chan *paxi.ClientBytesCommand, paxi.GetConfig().ChanBufferSize),
		Q2:               func(q *paxi.Quorum) bool { return q.Majority() },
		numQ2:            numQ2,
		numQF:            numQF,
	}

	for _, opt := range options {
		opt(fp)
	}

	return fp
}

func (fp *FastPaxos) run() {
	var err error
	for err == nil {
		select {
		// handle incoming commands from clients
		case cmd := <-fp.rawCommands:
			fp.handleCommands(cmd)
			numIncomingCmd := len(fp.rawCommands)
			for numIncomingCmd > 0 {
				fp.handleCommands(<-fp.rawCommands)
				numIncomingCmd--
			}
			break

		// handle incoming protocol messages from other node
		case pmsg := <-fp.protocolMessages:
			fp.handleProtocolMessages(pmsg)
			numProtocolMsg := len(fp.protocolMessages)
			for numProtocolMsg > 0 {
				fp.handleProtocolMessages(<-fp.protocolMessages)
				numProtocolMsg--
			}
			break
		}
	}

	panic(fmt.Sprintf("fastpaxos insatnce exited its main loop: %v", err))
}

// handleCommands opportunistically propose the commands directly to the acceptors
// without doing phase1 first
func (fp *FastPaxos) handleCommands(cmd *paxi.ClientBytesCommand) {
	log.Debugf("handling client's command with fast proposal (slot#=%d, cmd=%v)", fp.slot+1, cmd)

	fp.slot++
	fp.log[fp.slot] = &entry{
		ballot:  fp.ballot,
		isFast:  true,
		command: cmd,
		quorum:  paxi.NewQuorum(),
	}

	// self ack the proposal
	fp.log[fp.slot].quorum.ACK(fp.ID())

	// prepare the fast proposal
	m := P2a{
		Ballot:  fp.ballot,
		Slot:    fp.slot,
		Command: *cmd.BytesCommand,
		Fast:    true,
	}

	fp.Broadcast(m)
}

func (fp *FastPaxos) handleProtocolMessages(pmsg interface{}) {
	switch pmsg.(type) {
	case P2a:
		fp.handleP2a(pmsg.(P2a))
		break
	case P2b:
		fp.handleP2b(pmsg.(P2b))
		break
	case P3:
		fp.handleP3(pmsg.(P3))
	}
}

// handleP2a handle p2 messages (proposal) from proposers
func (fp *FastPaxos) handleP2a(m P2a) {
	// handle fast proposal where the ballot is not considered
	if m.Fast {
		fp.handleFastP2a(m)
		return
	}

	// handle classic proposal, the ballot need to be considered.
	// preparing response data for the proposer
	resp := P2b{
		Ballot: m.Ballot,
		ID:     fp.ID(),
		Slot:   m.Slot,
		Acc:    true,
	}

	e, exists := fp.log[m.Slot]
	// ignore (reject) if the proposal has lower ballot number
	if exists && m.Ballot < e.ballot {
		resp.Ballot = e.ballot
		resp.Acc = false
		fp.Send(m.Ballot.ID(), resp)
		return
	}

	// accept the proposal, update the highest proposal ballot,
	// update the value and accepted ballot in the proposed slot
	fp.maxBallot = m.Ballot
	fp.slot = paxi.Max(fp.slot, m.Slot)
	if exists {
		cmdBuff := []byte(*e.command.BytesCommand)
		if e.commit && !bytes.Equal(cmdBuff, m.Command) {
			log.Errorf("safety violation! committed value is updated with different value. %v -> %v", cmdBuff, m.Command)
		}

		bc := paxi.BytesCommand(m.Command)
		e.command.BytesCommand = &(bc) // update the accepted value
		e.ballot = m.Ballot            // update the accepted ballot number
	} else {
		bc := paxi.BytesCommand(m.Command)
		fp.log[m.Slot] = &entry{
			ballot: m.Ballot,
			isFast: false,
			command: &paxi.ClientBytesCommand{
				BytesCommand: &bc,
				RPCMessage:   nil,
			},
			commit: false,
			quorum: paxi.NewQuorum(),
		}
	}

	// send ack to the proposer
	fp.Send(m.Ballot.ID(), resp)
}

func (fp *FastPaxos) handleFastP2a(m P2a) {
	resp := P2b{
		Ballot: m.Ballot,
		ID:     fp.ID(),
		Slot:   m.Slot,
		Fast:   true,
	}

	fp.slot = paxi.Max(fp.slot, m.Slot)
	// fast-proposal also acts as classic phase-1 message,
	// promise to not accept classic proposal lower than maxBallot
	if m.Ballot > fp.maxBallot {
		fp.maxBallot = m.Ballot
	}

	if e, exists := fp.log[m.Slot]; exists && e.command != nil {
		// this acceptor already accepted value in the slot,
		// so the proposal need to be rejected
		resp.Ballot = e.ballot
		resp.Acc = false
		resp.Command = *e.command.BytesCommand

	} else {
		// no value was accepted for this slot, so the acceptor
		// can accept any proposal, including this one
		bc := paxi.BytesCommand(m.Command)
		fp.log[m.Slot] = &entry{
			ballot: m.Ballot,
			isFast: true,
			command: &paxi.ClientBytesCommand{
				BytesCommand: &bc,
				RPCMessage:   nil,
			},
			commit: false,
			quorum: paxi.NewQuorum(),
		}

		resp.Acc = true
	}

	// send the response to the proposer
	fp.Send(m.Ballot.ID(), resp)
}

func (fp *FastPaxos) handleP2b(m P2b) {
	log.Debugf("s=%d e=%d | handling proposal's response %v", fp.slot, fp.execute, m)

	// ignore old message
	e, exist := fp.log[m.Slot]
	if !exist || e.commit {
		return
	}

	// handle response for fast proposal
	if m.Fast {
		if e.isFast {
			fp.handleFastP2b(m)
		}
		return
	}

	log.Debugf("handle responses of classic proposal slot=%d b=%s | %d vs %d vs %d", m.Slot, e.ballot, e.quorum.Total(), e.quorum.Size(), fp.numQ2)
	if e.quorum.Total() >= fp.numQ2 {
		return
	}

	if m.Ballot == e.ballot {
		e.quorum.ACK(m.ID)
	} else {
		// handle rejection: acceptors that already promised to higher ballot
		e.quorum.NACK(m.ID)
	}

	if e.quorum.Total() == fp.numQ2 {
		if fp.Q2(e.quorum) {
			e.commit = true
			fp.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: *e.command.BytesCommand,
			})
			fp.exec()
			return
		}

		log.Warningf("TODO: Need to retry with higher ballot number in another slot")
		if e.command.RPCMessage != nil && m.Ballot.ID() != fp.ID() {
			failResp := paxi.CommandReply{
				Code:     paxi.CommandReplyErr,
				Data:     []byte("need to retry with higher ballot in another slot"),
				Metadata: map[byte]interface{}{paxi.MetadataCurrentBallot: fp.ballot.String()},
			}
			err := e.command.RPCMessage.SendBytesReply(failResp.Serialize())
			if err != nil {
				log.Errorf("fail to reply to client %v", err)
			}
			e.command.RPCMessage = nil
		}

		// increase the ballot number
		if fp.maxBallot > fp.ballot {
			fp.ballot = fp.maxBallot
		}
		fp.ballot.Next(fp.ID())
	}
}

// handleFastP2b handles response of fast proposal
func (fp *FastPaxos) handleFastP2b(m P2b) {
	e := fp.log[m.Slot]

	// ignore if enough responses already available
	if e.quorum.Total() >= fp.numQF {
		return
	}

	log.Debugf("handling fast-proposal's response %v [%d]", m, e.quorum.Total())

	if m.Ballot == fp.ballot {
		// the fast-proposal is accepted
		e.quorum.ACK(m.ID)

	} else {
		// the fast-proposal is rejected (other value was accepted previously)
		e.quorum.NACK(m.ID)

		// store the value with the highest accepted ballot
		// this will be used for recovery
		if m.Ballot > e.ballot {
			e.ballot = m.Ballot
			bc := paxi.BytesCommand(m.Command)
			e.command.BytesCommand = &(bc)
		}
	}

	// we can act if we already heard from numQF of acceptors
	if e.quorum.Total() == fp.numQF {
		log.Debugf("s=%d act for fast proposal (%d vs %d)", m.Slot, e.quorum.Size(), fp.numQF)

		// when all the numQF acceptors ack the value
		// this proposer can directly commit the value
		if e.quorum.Size() == fp.numQF {
			e.commit = true
			fp.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: *e.command.BytesCommand,
			})
			fp.exec()
			return
		}

		// otherwise, conflict happened, we need to fall back
		// to the classic Paxos
		log.Debugf("conflict happened in slot=%d (b=%s vs %s) need to fallback to classic phase2", m.Slot, fp.ballot, e.ballot)

		// propose the value with the highest ballot
		// in classical phase-2
		if fp.ballot >= e.ballot {
			e.quorum.Reset()
			e.isFast = false
			e.ballot = fp.ballot
			e.quorum.ACK(fp.ID())
			fp.maxBallot = fp.ballot
			fp.Broadcast(P2a{
				Ballot:  fp.ballot,
				Slot:    m.Slot,
				Command: *e.command.BytesCommand,
				Fast:    false,
			})
			return
		}

		// otherwise, this proposer need to retry phase-1 or
		// forward the to-be proposed value to the leader
		log.Debugf("other proposer with ballot %s make the value chosen in slot %d", e.ballot, m.Slot)
		// TODO: retry in other slot?
		if e.command.RPCMessage != nil {
			log.Warningf("Send fail response")
			failResp := paxi.CommandReply{
				Code:     paxi.CommandReplyErr,
				Data:     []byte(fmt.Sprintf("other proposer with ballot %s make the value chosen in slot %d", e.ballot, m.Slot)),
				Metadata: map[byte]interface{}{paxi.MetadataCurrentBallot: fp.ballot.String()},
			}
			err := e.command.RPCMessage.SendBytesReply(failResp.Serialize())
			if err != nil {
				log.Errorf("fail to reply to client %v", err)
			}
			e.command.RPCMessage = nil
		}
	}
}

func (fp *FastPaxos) handleP3(m P3) {
	bc := paxi.BytesCommand(m.Command)
	if _, exists := fp.log[m.Slot]; !exists {
		fp.log[m.Slot] = &entry{
			ballot: m.Ballot,
			command: &paxi.ClientBytesCommand{
				BytesCommand: &bc,
				RPCMessage:   nil,
			},
			commit: true,
			quorum: paxi.NewQuorum(),
		}
	}

	fp.slot = paxi.Max(fp.slot, m.Slot)
	e := fp.log[m.Slot]
	e.commit = true
	if e.quorum != nil {
		e.quorum.Reset()
	}
	if e.command == nil {
		e.command = &paxi.ClientBytesCommand{}
	}

	// if the command is not from this node, send failure response
	if e.command.RPCMessage != nil && m.Ballot.ID() != fp.ID() {
		failResp := paxi.CommandReply{
			Code:     paxi.CommandReplyErr,
			Data:     []byte("command is not from this node"),
			Metadata: map[byte]interface{}{paxi.MetadataCurrentBallot: fp.ballot.String()},
		}
		err := e.command.RPCMessage.SendBytesReply(failResp.Serialize())
		if err != nil {
			log.Errorf("fail to reply to client %v", err)
		}
		e.command.RPCMessage = nil
	}

	fp.exec()
}

func (fp *FastPaxos) exec() {
	for {
		e, ok := fp.log[fp.execute]
		if !ok || !e.commit {
			break
		}

		cmdReply := fp.execCommand(e.command.BytesCommand, e)

		if e.command.RPCMessage != nil && e.command.RPCMessage.Reply != nil {
			err := e.command.RPCMessage.SendBytesReply(cmdReply.Serialize())
			if err != nil {
				log.Errorf("failed to send CommandReply %s", err)
			}
			e.command.RPCMessage = nil
		}

		// clean the slot after the command is executed
		delete(fp.log, fp.execute)
		fp.execute++
	}
}

func (fp *FastPaxos) execCommand(byteCmd *paxi.BytesCommand, e *entry) paxi.CommandReply {
	var cmd paxi.Command
	reply := paxi.CommandReply{
		Code:   paxi.CommandReplyOK,
		SentAt: 0,
		Data:   nil,
	}

	if *paxi.ClientIsStateful {
		cmd = byteCmd.ToCommand()

	} else if *paxi.ClientIsStateful == false {
		gcmd, err := paxi.UnmarshalGenericCommand(*byteCmd)
		if err != nil {
			log.Fatalf("failed to unmarshal client's generic command %s", err.Error())
		}
		log.Debugf("generic command %v", gcmd)
		cmd.Key = paxi.Key(binary.BigEndian.Uint32(gcmd.Key))
		cmd.Value = gcmd.Value
		reply.SentAt = gcmd.SentAt // forward sentAt from client back to client

	} else {
		log.Errorf("unknown client stateful property, does not know how to handle the command")
		reply.Code = paxi.CommandReplyErr
		reply.Data = []byte("unknown client stateful property, does not know how to handle the command")
	}

	log.Debugf("executing command %v", cmd)
	value := fp.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	log.Debugf("cmd=%v, value=%x", cmd, value)
	return reply
}
