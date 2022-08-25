package fastopaxos

import (
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
	"github.com/fadhilkurnia/shamir/shamir"
	"math"
	"runtime"
	"time"
)

type entry struct {
	ballot    paxi.Ballot // the accepted ballot number
	oriBallot paxi.Ballot // the original ballot of the accepted secret-share
	commit    bool        // commit indicates whether this entry is already committed or not

	// field for acceptor
	ssVal        []byte               // TODO: deprecate this, replace with share
	share        []opaxos.SecretShare // the accepted secret-share
	isFast       bool                 // indicate whether any secret-share can be accepted or not // TODO: deprecate this
	canAcceptAny bool                 // indicate whether any secret-share can be accepted or not

	// field for trusted proposer
	quorum         *paxi.Quorum        // phase-2 quorum
	command        paxi.BytesCommand   // proposed command
	commandHandler *paxi.ClientCommand // handler to reply to client for the command
	commandShare   []*CommandShare     // collection of command from multiple acceptors, with the same slot number
	shares         []*CommandShare     // TODO: deprecate this, replace with commandShare
	ssTime         time.Duration

	// for debugging purposes
	History []string
	valID   string
}

// FastOPaxos instance in a single Node
type FastOPaxos struct {
	paxi.Node // extending generic paxi.Node

	log       map[int]*entry // log ordered by slot number
	execute   int            // next execute slot number
	ballot    paxi.Ballot    // the proposer's current ballot
	maxBallot paxi.Ballot    // the acceptor's highest promised ballot
	slot      int            // highest non-empty slot number

	protocolMessages chan interface{}                 // receiver channel for prepare, propose, commit messages
	rawCommands      chan *paxi.ClientCommand         // raw commands from clients
	pendingCommands  chan *opaxos.SecretSharedCommand // pending commands that will be proposed
	retryCommands    chan *paxi.ClientCommand         // retryCommands holds command that need to be reproposed due to conflict

	N               int                            // N is the number of acceptors
	threshold       int                            // threshold is the shares required to regenerate the secret value
	numSSWorkers    int                            // number of workers to secret-share client's raw command
	numQ2           int                            // numQ2 is the size of quorum for phase-2 (classic)
	numQF           int                            // numQF is the size of fast quorum
	Q2              func(quorum *paxi.Quorum) bool // Q2 return true if there are ack from numQ2 acceptors
	defaultSSWorker opaxos.SecretSharingWorker     // secret-sharing worker for reconstruction only, used without channel
}

func NewFastOPaxos(n paxi.Node, options ...func(fop *FastOPaxos)) *FastOPaxos {
	cfg := opaxos.InitConfig(n.GetConfig())
	numQ2 := int(math.Ceil(float64(n.GetConfig().N()) / 2))
	numQF := int(math.Ceil(float64(n.GetConfig().N()) * 3 / 4))

	fop := &FastOPaxos{
		Node:             n,
		ballot:           paxi.NewBallot(0, n.ID()),
		maxBallot:        paxi.NewBallot(0, n.ID()),
		log:              make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:             -1,
		protocolMessages: make(chan interface{}, paxi.GetConfig().ChanBufferSize),
		rawCommands:      make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize),
		pendingCommands:  make(chan *opaxos.SecretSharedCommand, paxi.GetConfig().ChanBufferSize),
		retryCommands:    make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize),
		Q2:               func(q *paxi.Quorum) bool { return q.Majority() },
		N:                n.GetConfig().N(),
		threshold:        cfg.Protocol.Threshold,
		numSSWorkers:     runtime.NumCPU(),
		numQ2:            numQ2,
		numQF:            numQF,
	}

	for _, opt := range options {
		opt(fop)
	}

	fop.initDefaultSecretSharingWorker()

	return fop
}

func (fop *FastOPaxos) initRunSecretSharingWorker() {
	worker := opaxos.NewWorker("shamir", fop.N, fop.threshold)
	worker.StartProcessingInput(fop.rawCommands, fop.pendingCommands)
}

func (fop *FastOPaxos) initDefaultSecretSharingWorker() {
	fop.defaultSSWorker = opaxos.NewWorker("shamir", fop.N, fop.threshold)
}

func (fop *FastOPaxos) run() {
	var err error
	for err == nil {
		select {
		// handle incoming commands from clients
		case cmd := <-fop.pendingCommands:
			fop.handleCommands(cmd)
			numPendingCmd := len(fop.pendingCommands)
			for numPendingCmd > 0 {
				fop.handleCommands(<-fop.pendingCommands)
				numPendingCmd--
			}
			break

		// handle incoming protocol messages from other node
		case pmsg := <-fop.protocolMessages:
			fop.handleProtocolMessages(pmsg)
			numProtocolMsg := len(fop.protocolMessages)
			for numProtocolMsg > 0 {
				fop.handleProtocolMessages(<-fop.protocolMessages)
				numProtocolMsg--
			}
			break
		}
	}

	panic(fmt.Sprintf("fastpaxos instance exited its main loop: %v", err))
}

func (fop *FastOPaxos) handleCommands(cmd *opaxos.SecretSharedCommand) {
	log.Debugf("handling client's command with fast proposal (slot#=%d, cmd=%v)", fop.slot+1, cmd)

	fop.slot++
	fop.log[fop.slot] = &entry{
		ballot:    fop.ballot,
		oriBallot: fop.ballot,
		isFast:    true,
		//command:   cmd,
		//ssVal:     cmd.SharesBatch[len(cmd.SharesBatch)-1],
		quorum: paxi.NewQuorum(),
		//shares:    nil,
		valID: fmt.Sprintf("%x", cmd.RawCommand),
	}
	fop.log[fop.slot].History = append(
		fop.log[fop.slot].History,
		fmt.Sprintf("%s %d %d: created with b=%s ob=%s vid=%s ssval=%x",
			fop.ID(),
			time.Now().Unix(),
			fop.slot,
			fop.ballot,
			fop.ballot,
			fop.log[fop.slot].valID,
			"asd"))
	//fop.log[fop.slot].ssVal))

	// self ack the proposal
	fop.log[fop.slot].quorum.ACK(fop.ID())

	// prepare the fast-proposal
	proposals := make([]interface{}, len(cmd.Shares)-1)
	for i := 0; i < len(cmd.Shares)-1; i++ {
		proposals[i] = P2a{
			Fast:      true,
			Ballot:    fop.ballot,
			Slot:      fop.slot,
			Command:   cmd.Shares[i],
			OriBallot: fop.ballot,
			ValID:     fop.log[fop.slot].valID,
		}
	}

	log.Warningf("fast-proposal slot=%s props=%v", fop.slot, proposals)

	// broadcast the proposals
	fop.MulticastUniqueMessage(proposals)
}

func (fop *FastOPaxos) handleProtocolMessages(pmsg interface{}) {
	switch pmsg.(type) {
	case P2a:
		fop.handleP2a(pmsg.(P2a))
		break
	case P2b:
		fop.handleP2b(pmsg.(P2b))
		break
	case P3:
		fop.handleP3(pmsg.(P3))
	}
}

func (fop *FastOPaxos) handleP2a(m P2a) {
	// handle fast-proposal
	if m.Fast {
		fop.handleFastP2a(m)
		return
	}

	// handle classic proposal, the ballot need to be considered.
	// preparing response data for the proposer
	resp := P2b{
		Fast:   false,
		Ballot: m.Ballot,
		ID:     fop.ID(),
		Slot:   m.Slot,
	}

	e, exists := fop.log[m.Slot]
	// ignore (reject) if the proposal has lower ballot number
	if exists && m.Ballot < e.ballot {
		resp.Ballot = e.ballot

		e.History = append(e.History, fmt.Sprintf("%s %d %d: reject classic proposal w lower b=%s", fop.ID(), time.Now().Unix(), m.Slot, m.Ballot))
		resp.History = e.History

		fop.Send(m.Ballot.ID(), resp)
		return
	}

	// accept the proposal, update the highest proposal ballot,
	// update the value and accepted ballot in the proposed slot
	fop.maxBallot = m.Ballot
	fop.slot = paxi.Max(fop.slot, m.Slot)
	if exists {
		// TODO: nullify or update the command
		//if e.oriBallot != m.OriBallot && e.command != nil {
		//	cmdBuff := []byte(*e.command.BytesCommand)
		//	if e.commit && !bytes.Equal(cmdBuff, m.Command) {
		//		log.Errorf("safety violation! committed value is updated with different value. %v -> %v", cmdBuff, m.Command)
		//	}
		//	// bc := paxi.BytesCommand(m.Command)
		//	// e.command.BytesCommand = &(bc) // TODO: nullify the command
		//}

		e.ballot = m.Ballot // update the accepted ballot number
		e.History = append(e.History, fmt.Sprintf("%s %d %d: accept classic proposal w with b=%s ob=%s vid=%s ssval=%x",
			fop.ID(), time.Now().Unix(), m.Slot, m.Ballot, m.OriBallot, m.ValID, m.Command))
		if e.oriBallot != m.OriBallot {
			//e.ssVal = m.Command       // update the accepted secret-share
			e.oriBallot = m.OriBallot // update the accepted original-ballot
		}
		e.History = append(e.History, fmt.Sprintf("%s %d %d: accept classic proposal without update b=%s ob=%s vid=%s ssval=%x",
			fop.ID(), time.Now().Unix(), m.Slot, m.Ballot, m.OriBallot, m.ValID, e.ssVal))
		resp.History = e.History
		e.valID = m.ValID
		resp.ValID = m.ValID

	} else {
		fop.log[m.Slot] = &entry{
			ballot:    m.Ballot,
			isFast:    false,
			command:   nil,
			ssVal:     m.Command,
			oriBallot: m.OriBallot,
			commit:    false,
			quorum:    paxi.NewQuorum(),
			valID:     m.ValID,
		}

		fop.log[m.Slot].History = append(fop.log[m.Slot].History, fmt.Sprintf("%s %d %d: (classic) accepted & created proposal w with b=%s ob=%s ssval=%x",
			fop.ID(), time.Now().Unix(), m.Slot, m.Ballot, m.OriBallot, m.Command))
		resp.History = fop.log[m.Slot].History
		resp.ValID = m.ValID
	}

	fop.Send(m.Ballot.ID(), resp)
}

func (fop *FastOPaxos) handleFastP2a(m P2a) {
	resp := P2b{
		Fast:      true,
		Ballot:    m.Ballot, // resp.Ballot == e.ballot means acceptance
		ID:        fop.ID(),
		Slot:      m.Slot,
		Command:   nil,
		OriBallot: m.OriBallot,
		ValID:     m.ValID,
	}

	fop.slot = paxi.Max(fop.slot, m.Slot)

	if e, exists := fop.log[m.Slot]; exists && e.ssVal != nil {
		// this acceptor already accepted value for this slot
		// rejecting the fast-proposal
		// resp.Ballot != e.ballot means rejection
		resp.Ballot = e.ballot
		resp.Command = e.ssVal
		resp.OriBallot = e.oriBallot
		e.History = append(e.History, fmt.Sprintf("%s %d %d: reject other fast proposal %s", fop.ID(), time.Now().Unix(), m.Slot, m))
		resp.History = e.History
		resp.ValID = e.valID
	} else {
		// no secret-share was accepted before, so this acceptor
		// is accepting the share, regardless the ballot number
		fop.log[m.Slot] = &entry{
			ballot:    m.Ballot,
			isFast:    true,
			ssVal:     m.Command,
			oriBallot: m.OriBallot,
			commit:    false,
			quorum:    paxi.NewQuorum(),
			shares:    nil,
			valID:     m.ValID,
		}
		fop.log[m.Slot].History = append(
			fop.log[m.Slot].History,
			fmt.Sprintf("%s %d %d: (fast) accepted & created with b=%s ob=%s vid=%s ssval=%x",
				fop.ID(),
				time.Now().Unix(),
				m.Slot,
				m.Ballot,
				m.OriBallot,
				m.ValID,
				fop.log[m.Slot].ssVal))

		resp.History = fop.log[m.Slot].History
	}

	// send proposal's response back to proposer
	fop.Send(m.Ballot.ID(), resp)
}

func (fop *FastOPaxos) handleP2b(m P2b) {
	log.Debugf("s=%d e=%d | handling proposal's response %v", fop.slot, fop.execute, m)

	// ignore old proposal's response
	e, exist := fop.log[m.Slot]
	if !exist || e.commit {
		return
	}

	// handle fast proposal
	if m.Fast {
		if e.isFast {
			fop.handleFastP2b(m)
		}
		return
	}

	log.Debugf("handle responses of classic proposal slot=%d b=%s | %d vs %d vs %d", m.Slot, e.ballot, e.quorum.Total(), e.quorum.Size(), fop.numQ2)
	if e.quorum.Total() >= fop.numQ2 {
		return
	}

	if m.Ballot == e.ballot {
		log.Debugf("ack classic proposal's response slot=%d b=%s", m.Slot, e.ballot)
		e.quorum.ACK(m.ID)
	} else {
		log.Debugf("nack classic proposal's response slot=%d b=%s", m.Slot, e.ballot)
		e.quorum.NACK(m.ID)
	}

	if e.quorum.Total() == fop.numQ2 {
		log.Debugf("act for classic proposal slot=%d b=%s", m.Slot, e.ballot)
		if fop.Q2(e.quorum) {
			log.Debugf("committing ...")
			e.commit = true
			fop.Broadcast(P3{
				Ballot:    m.Ballot,
				Slot:      m.Slot,
				Command:   e.command,
				OriBallot: e.oriBallot,
			})
			fop.exec()
			return
		}

		log.Warningf("TODO: Need to retry with higher ballot number in another slot")
		if e.command != nil && e.commandHandler != nil && m.Ballot.ID() != fop.ID() {
			fop.sendFailureResponse(e)
		}

		// increase the ballot number
		if fop.maxBallot > fop.ballot {
			fop.ballot = fop.maxBallot
		}
		fop.ballot.Next(fop.ID())
	}
}

func (fop *FastOPaxos) handleFastP2b(m P2b) {
	e := fop.log[m.Slot]

	// ignore if enough response already received
	if e.quorum.Total() >= fop.numQF {
		return
	}

	log.Debugf("handling fast-proposal's response %v [%d]", m, e.quorum.Total())

	if m.Command == nil {
		// the fast-proposal is accepted
		e.quorum.ACK(m.ID)

	} else {
		// the fast-proposal is rejected (other value was accepted previously)
		e.quorum.NACK(m.ID)

		// store the value with the highest accepted ballot
		// this will be used for recovery
		otherShare := &CommandShare{
			Ballot:    m.Ballot,
			OriBallot: m.OriBallot,
			Command:   m.Command,
			ID:        m.ID,
			History:   m.History,
			ValID:     m.ValID,
		}
		e.shares = append(e.shares, otherShare)
	}

	// the proposer can act when enough responses are received
	if e.quorum.Total() == fop.numQF {
		log.Debugf("s=%d act for fast proposal (%d vs %d)", m.Slot, e.quorum.Size(), fop.numQF)

		// when all the numQF accepted the fast-proposal
		// this proposer can directly commit the value
		if e.quorum.Size() == fop.numQF {
			if e.command == nil {
				log.Errorf("want to commit but command is empty slot=%d b=%s", m.Slot, m.Ballot)
				log.Fatalf("empty cmd %v", e)
			}
			e.commit = true
			fop.Broadcast(P3{
				Ballot:    m.Ballot,
				Slot:      m.Slot,
				Command:   e.command, // TODO: hide this from untrusted node
				OriBallot: e.oriBallot,
			})
			fop.exec()
			return
		}

		// conflict happened: less than numQF acceptors accepted the fast-proposal
		// fallback to classical proposal
		log.Warningf("conflict happened in slot=%d (b=%s vs %s) need to fallback to classic phase2", m.Slot, fop.ballot, e.ballot)

		// find the highest ballot from the response
		highestBallot := e.shares[0].Ballot
		highestShareIdx := 0
		for i, s := range e.shares {
			if s.Ballot > highestBallot {
				highestBallot = s.Ballot
				highestShareIdx = i
			}
		}
		log.Warningf("highestBallot=%s oriBallot=%s", highestBallot, e.shares[highestShareIdx].OriBallot)

		// find T shares with the same original-ballot as the share
		// with the highest ballot
		recoveryShares := make([][]byte, 0)
		i := 0
		for _, s := range e.shares {
			log.Debugf("++++++++ %s: %s %s", s.ID, s.Ballot, s.OriBallot)
			for _, h := range s.History {
				log.Debugf("+++++++++ h |-> %s", h)
			}
			if s.OriBallot == e.shares[highestShareIdx].OriBallot {
				recoveryShares = append(recoveryShares, make([]byte, len(s.Command)))
				recoveryShares[i] = make([]byte, len(s.Command))
				copy(recoveryShares[i], s.Command)
				i++
			}
		}

		// let the "winner" proposer repropose the value
		// for the slot, if this is not the winner proposer
		// then send failure response to client
		if highestBallot > fop.ballot {
			log.Warningf("other proposer with ballot %s make the value chosen in slot %d | hb=%s id=%s", e.ballot, m.Slot, highestBallot, fop.ID())
			if e.command != nil && e.commandHandler != nil {
				fop.sendFailureResponse(e)
			}
			return
		}

		// if enough shares collected, we need to re-propose the secret value
		// otherwise we can re-propose the currently held value
		if len(recoveryShares) == fop.threshold {
			log.Warningf("reconstructing previous value slot=%d hb=%s ob=%s", m.Slot, highestBallot, e.shares[highestShareIdx].OriBallot)
			for _, sh := range recoveryShares {
				log.Warningf("=== %x", sh)
			}

			startTime := time.Now()
			newShares, err := shamir.Regenerate(recoveryShares, fop.N)
			if err != nil {
				log.Fatalf("failed to regenerate secret-shares: %v", err)
			}
			dur := time.Since(startTime)
			val, err := shamir.Combine(recoveryShares)
			if err != nil {
				for _, x := range recoveryShares {
					log.Errorf("---> %d", len(x))
				}
				log.Fatalf("failed to reconstruct a might be chosen value %v", err)
			}
			log.Debugf("reconstructed new secret value %x", val)
			if _, err = paxi.UnmarshalGenericCommand(val); err != nil {
				log.Fatalf("the reconstructed value is not a valid command: %v", err)
			}

			// TODO: reuse some of the shares to prevent duplicate

			if e.command == nil {
				e.command = nil
			}

			newSharesStruct := make([]opaxos.SecretShare, len(newShares))
			for j := 0; j < len(newShares); j++ {
				newSharesStruct[j] = newShares[j]
			}

			e.ssTime = dur
			//e. command.SharesBatch = newSharesStruct
			//bc := paxi.BytesCommand(val)
			//e.command.BytesCommand = &bc // replace the held command (secret value)
			e.ssVal = newShares[len(newShares)-1]
			e.oriBallot = e.shares[highestShareIdx].OriBallot
			e.valID = e.shares[highestShareIdx].ValID

			e.History = append(
				e.History,
				fmt.Sprintf("%s %d %d: update the reconstructed val b=%s ob=%s vid=%s vid(r)=%x ssval=%x",
					fop.ID(),
					time.Now().Unix(),
					m.Slot,
					e.ballot,
					e.oriBallot,
					e.valID,
					val,
					e.ssVal))

			fop.sendFailureResponse(e)
		}

		// re-propose the value with classic proposal
		//fop.ballot.Next(fop.ID())

		e.quorum.Reset()
		e.shares = nil
		e.isFast = false
		e.ballot = fop.ballot
		e.History = append(
			e.History,
			fmt.Sprintf("%s %d %d: update ballot in fallback b=%s ob=%s ssval=%x",
				fop.ID(),
				time.Now().Unix(),
				m.Slot,
				e.ballot,
				e.oriBallot,
				e.ssVal))

		e.quorum.ACK(fop.ID())
		fop.maxBallot = fop.ballot

		// prepare the classic proposal
		log.Debugf("preparing classic proposal slot=%d b=%s ob=%s", m.Slot, fop.ballot, e.oriBallot)
		proposals := make([]interface{}, len(e.command)-1)
		for j := 0; j < len(e.command)-1; j++ {
			proposals[j] = P2a{
				Fast:      false,
				Ballot:    fop.ballot,
				Slot:      m.Slot,
				Command:   e.command,
				OriBallot: e.oriBallot,
				ValID:     e.valID,
			}
		}

		// send the classic proposal
		fop.MulticastUniqueMessage(proposals)
	}
}

func (fop *FastOPaxos) handleP3(m P3) {
	//bc := paxi.BytesCommand(m.Command)
	if _, exists := fop.log[m.Slot]; !exists {
		fop.log[m.Slot] = &entry{
			ballot:    m.Ballot,
			isFast:    false,
			oriBallot: m.OriBallot,
			commit:    true,
			quorum:    paxi.NewQuorum(),
			shares:    nil,
		}
		fop.log[m.Slot].History = append(
			fop.log[m.Slot].History,
			fmt.Sprintf("%s %d %d: accepted & created via commit with b=%s ob=%s ssval=%x",
				fop.ID(),
				time.Now().Unix(),
				m.Slot,
				m.Ballot,
				m.OriBallot,
				fop.log[m.Slot].ssVal))
	}

	fop.slot = paxi.Max(fop.slot, m.Slot)
	e := fop.log[m.Slot]
	e.commit = true
	e.ballot = m.Ballot
	e.oriBallot = m.OriBallot
	if e.quorum != nil {
		e.quorum.Reset()
	}
	//if e.command == nil {
	//	e.command = &opaxos.SecretSharedCommand{}
	//}
	//if e.command.ClientBytesCommand == nil {
	//	e.command.ClientBytesCommand = &paxi.ClientBytesCommand{}
	//}
	//e.command.ClientBytesCommand.BytesCommand = &bc

	e.History = append(
		e.History,
		fmt.Sprintf("%s %d %d: ballot is updated via commit b=%s ob=%s ssval=%x",
			fop.ID(),
			time.Now().Unix(),
			m.Slot,
			m.Ballot,
			m.OriBallot,
			fop.log[m.Slot].ssVal))

	// if the command is not from this node, send failure response
	//if e.command.RPCMessage != nil && m.Ballot.ID() != fop.ID() {
	//	failResp := paxi.CommandReply{
	//		OK:     false,
	//		Ballot: fop.ballot.String(),
	//	}
	//	err := e.command.RPCMessage.SendBytesReply(failResp.Serialize())
	//	if err != nil {
	//		log.Errorf("fail to reply to client %v", err)
	//	}
	//	e.command.RPCMessage = nil
	//}

	fop.exec()
}

func (fop *FastOPaxos) exec() {
	for {
		e, ok := fop.log[fop.execute]
		if !ok || !e.commit {
			break
		}

		//cmdReply := fop.execCommand(e.command.BytesCommand, e)
		//
		//if e.command.RPCMessage != nil && e.command.RPCMessage.Reply != nil {
		//	err := e.command.RPCMessage.SendBytesReply(cmdReply.Serialize())
		//	if err != nil {
		//		log.Errorf("failed to send CommandReply %s", err)
		//	}
		//	e.command.RPCMessage = nil
		//}

		// clean the slot after the command is executed
		//delete(fop.log, fop.execute)
		fop.execute++
	}
}

func (fop *FastOPaxos) execCommand(byteCmd *paxi.BytesCommand, e *entry) paxi.CommandReply {
	var cmd paxi.Command
	reply := paxi.CommandReply{
		Code:   paxi.CommandReplyErr,
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
	}

	log.Debugf("executing command %v", cmd)
	value := fop.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	log.Debugf("cmd=%v, value=%x", cmd, value)
	return reply
}

func (fop *FastOPaxos) sendFailureResponse(e *entry) {
	//failResp := &paxi.CommandReply{
	//	OK:     false,
	//	Ballot: fop.ballot.String(),
	//}
	//err := e.command.Reply(failResp)
	//if err != nil {
	//	log.Errorf("fail to reply to client %v", err)
	//}
	//e.command.ClientCommand = nil
}
