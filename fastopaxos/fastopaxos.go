package fastopaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
	"github.com/vmihailenco/msgpack/v5"
	"math"
	"strings"
)

// firstBallot is a fast round ballot
// with node 1.1 as the coordinator
var firstBallot = NewBallot(0, false,
	paxi.NewID(1, 1))

type entry struct {
	ballot    Ballot      // the accepted ballot number
	oriBallot Ballot      // the original ballot of the accepted secret-share
	commit    bool        // commit indicates whether this entry is already committed or not
	exec      bool        // exec indicates whether this entry is already executed, if true, commit must true
	share     SecretShare // the accepted secret-share of value (single secret-share of command)

	// field for the trusted node (and coordinator)
	quorum         *paxi.Quorum        // phase-2 quorum
	command        paxi.BytesCommand   // proposed command in a clear form
	commandHandler *paxi.ClientCommand // handler to reply to client for the command
	propResponses  []*P2b              // collection of P2b from other nodes
	resendClearCmd bool
}

func (e entry) String() string {
	qt := 0
	qz := 0
	if e.quorum != nil {
		qt = e.quorum.Total()
		qz = e.quorum.Size()
	}
	return fmt.Sprintf("entry{bal: %s, bori: %s, commit: %t, cmd: %x, q_total: %d, q_size: %d, p2b: %v}",
		e.ballot, e.oriBallot, e.commit, e.command, qt, qz, e.propResponses)
}

// FastOPaxos instance in a single Node
type FastOPaxos struct {
	paxi.Node // extending generic paxi.Node

	// fields for trusted proposer, and the coordinator
	ballot           Ballot           // the proposer's current ballot
	isTrusted        bool             // isTrusted is true if this node has proposer role
	isCoordinator    bool             // isCoordinator is true if this node is a coordinator (the leader)
	algorithm        string           // secret-sharing algorithm: shamir or ssms
	N                int              // N is the number of acceptors
	threshold        int              // threshold is the shares required to regenerate the secret value
	trustedNodeIDs   map[paxi.ID]bool // list of trusted node IDs, used to send clear command during commit phase
	untrustedNodeIDs map[paxi.ID]bool // list of untrusted node IDs, used for commit
	ssWorker         opaxos.SecretSharingWorker

	// fields for acceptors
	maxBallot Ballot         // the acceptor's highest promised ballot
	acceptAny bool           // indicate whether any secret-share can be accepted or not
	log       map[int]*entry // log ordered by slot number
	execute   int            // next execute slot number
	slot      int            // highest non-empty slot number

	protocolMessages chan interface{}         // receiver channel for prepare, propose, commit messages
	rawCommands      chan *paxi.ClientCommand // raw commands from clients

	numSSWorkers int                            // number of workers to secret-share client's raw command
	numQ2        int                            // numQ2 is the size of quorum for phase-2 (classic)
	numQF        int                            // numQF is the size of fast quorum
	Q2           func(quorum *paxi.Quorum) bool // Q2 return true if there are ack from numQ2 acceptors
}

func NewFastOPaxos(n paxi.Node, options ...func(fop *FastOPaxos)) *FastOPaxos {
	cfg := opaxos.InitConfig(n.GetConfig())
	numQ2 := int(math.Ceil(float64(n.GetConfig().N()) / 2))
	numQF := int(math.Ceil(float64(n.GetConfig().N()) * 3 / 4))

	fop := &FastOPaxos{
		Node:             n,
		ballot:           firstBallot,
		isTrusted:        false,
		isCoordinator:    false,
		algorithm:        cfg.Protocol.SecretSharing,
		maxBallot:        firstBallot,
		acceptAny:        true,
		log:              make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:             -1,
		protocolMessages: make(chan interface{}, paxi.GetConfig().ChanBufferSize),
		rawCommands:      make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize),
		Q2:               func(q *paxi.Quorum) bool { return q.Majority() },
		N:                n.GetConfig().N(),
		threshold:        cfg.Protocol.Threshold,
		numQ2:            numQ2,
		numQF:            numQF,
		trustedNodeIDs:   make(map[paxi.ID]bool),
		untrustedNodeIDs: make(map[paxi.ID]bool),
	}

	for _, opt := range options {
		opt(fop)
	}

	// parse roles for this node
	roles := strings.Split(cfg.Roles[n.ID()], ",")
	for _, r := range roles {
		if r == "proposer" {
			fop.isTrusted = true
			fop.initDefaultSecretSharingWorker()

			// by default node 1.1 is the trusted node and coordinator
			if n.ID().Node() == 1 && n.ID().Zone() == 1 {
				fop.isCoordinator = true
			}
		}
	}

	// parse other trusted and untrusted node
	for nodeID, nodeRolesStr := range cfg.Roles {
		nodeRoles := strings.Split(nodeRolesStr, ",")
		isNodeTrusted := false
		for _, r := range nodeRoles {
			if r == "proposer" {
				fop.trustedNodeIDs[nodeID] = true
				isNodeTrusted = true
			}
		}
		if !isNodeTrusted {
			fop.untrustedNodeIDs[nodeID] = true
		}
	}

	return fop
}

func (fop *FastOPaxos) initDefaultSecretSharingWorker() {
	numShares := fop.N
	numThreshold := fop.threshold
	fop.ssWorker = opaxos.NewWorker(fop.algorithm, numShares, numThreshold)
}

func (fop *FastOPaxos) run() {
	var err error
	for err == nil {
		select {
		case dcmd := <-fop.rawCommands:
			fop.handleClientDirectCommand(dcmd)

			if fop.isCoordinator {
				numDCmd := len(fop.rawCommands)
				for numDCmd > 0 {
					fop.handleClientDirectCommand(<-fop.rawCommands)
					numDCmd--
				}
			}

			break

		case pMsg := <-fop.protocolMessages:
			fop.handleProtocolMessage(pMsg)

			numPMsg := len(fop.protocolMessages)
			for numPMsg > 0 {
				fop.handleProtocolMessage(<-fop.protocolMessages)
				numPMsg--
			}
			break

		}
	}

	panic(fmt.Sprintf("fastopaxos instance exited its main loop: %v", err))
}

// handleClientDirectCommand need to handle several cases:
// For the coordinator:
// - unassigned: this is the common case, the coordinator receives direct command before P2b from other nodes
// - assigned, not committed: the coordinator already received P2b from other nodes, but not enough to be committed
// - assigned, committed: the coordinator already received |Qf| P2b messages, before receiving client's direct command
// For the acceptor
// - unassigned: this is the common case
// - assigned: this can only happen if P3 comes first from the coordinator before client's DirectCommand
func (fop *FastOPaxos) handleClientDirectCommand(cmd *paxi.ClientCommand) {
	if len(cmd.RawCommand) == 0 {
		log.Errorf("got empty direct command from client: %v", cmd)
		return
	}

	if !fop.acceptAny {
		log.Warning("can not accept any secret-shared value, need to be prepared first")
		if err := cmd.Reply(&paxi.CommandReply{
			Code: paxi.CommandReplyErr,
			Data: []byte("can not accept any command, need to be prepared first"),
		}); err != nil {
			log.Errorf("failed to send err response to client: %s", err)
		}
		return
	}

	directCmd, err := DeserializeDirectCommand(cmd.RawCommand)
	if err != nil {
		log.Errorf("failed to deserialize DirectCommand: %s", err)
		return
	}

	newEntryRequired := true
	slot := directCmd.Slot

	log.Debugf("handling DirectCommand from client: b=%s s=%d bo=%s lencmd=%d",
		fop.ballot, slot, directCmd.OriBallot, len(directCmd.Command))
	if _, exist := fop.log[slot]; exist {
		newEntryRequired = false
		if fop.log[slot].oriBallot == directCmd.OriBallot {
			// the slot is already exist, this is possible in two cases:ß
			// 1. this node is a coordinator and got P2b messages first from other nodes before
			//    getting DirectCommand from the client.
			// 2. this node is a non-coordinator and got P3 (commit) message from the coordinator
			//    before getting DirectCommand from the client.
			log.Debug("received DirectCommand for an already allocated entry")
			fop.log[slot].share = directCmd.Share
			fop.log[slot].commandHandler = cmd
			fop.log[slot].command = directCmd.Command

		} else {
			// the slot is already used by another command
			err = cmd.Reply(&paxi.CommandReply{
				Code: paxi.CommandReplyErr,
				Data: []byte("slot is already used by another command, try to use another slot"),
			})
			if err != nil {
				log.Errorf("failed to send err response to client: %s", err)
			}
			return
		}
	}

	// allocate a new entry with the given slot
	if newEntryRequired {
		fop.log[slot] = &entry{
			ballot:         fop.ballot,
			oriBallot:      directCmd.OriBallot,
			commit:         false,
			share:          directCmd.Share,
			command:        directCmd.Command,
			commandHandler: cmd,
		}
		fop.slot = paxi.Max(fop.slot, slot)

		if fop.isCoordinator {
			fop.log[slot].quorum = paxi.NewQuorum()
		}
	}

	// non-coordinator node needs to send P2b to the coordinator
	if !fop.isCoordinator {
		if !fop.log[slot].commit {
			fop.Send(fop.ballot.ID(), P2b{
				Ballot:    fop.ballot,
				ID:        fop.ID(),
				Slot:      slot,
				Share:     fop.log[slot].share,
				OriBallot: directCmd.OriBallot,
			})
		}
		return
	}
	// --- the action for a non-coordinator stops here, the following code
	// --- is only for the coordinator.

	if len(directCmd.Command) == 0 {
		log.Errorf("the coordinator get empty DirectCommand from client: %s", directCmd)
	}

	// the coordinator handling P2b from itself
	fop.log[slot].quorum.ACK(fop.ID())

	if fop.log[slot].commit {

		if fop.log[slot].command == nil {
			log.Fatalf("command is still empty: %v", fop.log[slot])
		}

		// If the entry is already committed then the coordinator just need to execute it without
		// broadcasting commit. This is possible if previously the coordinator already
		// received |Qf| P2b messages before receiving DirectCommand from the client.
		if fop.log[slot].resendClearCmd {
			fop.broadcastClearCommand(directCmd.Slot, fop.log[slot])
		}
		fop.exec()
		return
	}

	// when there are already numQf P2b messages received, including P2b from itself,
	// then the coordinator can decide whether to commit or recover
	if fop.log[slot].quorum.Total() >= fop.numQF {
		if fop.log[slot].quorum.Size() >= fop.numQF {
			if !fop.log[slot].commit {
				fop.log[slot].commit = true
				fop.log[slot].exec = true
				fop.broadcastCommit(slot, fop.log[slot])
				fop.exec()
			}
		} else {
			log.Errorf("s=%d | quorum total: %d, quorum size: %d",
				directCmd.Slot, fop.log[slot].quorum.Total(), fop.log[slot].quorum.Size())
			fop.recoveryProcess(slot)
		}
	}

	// just in case
	fop.exec()
}

func (fop *FastOPaxos) handleProtocolMessage(pmsg interface{}) {
	switch pmsg.(type) {
	case P2a:
		panic("unimplemented")

	case P2b:
		fop.handleP2b(pmsg.(P2b))
		break

	case P3:
		fop.handleCommitMessage(pmsg.(P3))

	case P3c:
		fop.handleCommittedClearCommand(pmsg.(P3c))

	case *paxi.ClientCommand:
		req := pmsg.(*paxi.ClientCommand)
		if req.CommandType == paxi.TypeGetMetadataCommand {
			fop.handleGetMetadataRequest(req)
		}

	}
}

func (fop *FastOPaxos) handleP2b(m P2b) {
	// ignore outdated or future leadership term
	if m.Ballot != fop.ballot || m.Slot < fop.execute || !fop.isCoordinator {
		log.Debugf("ignoring outdated or future proposal's response: %s", m)
		return
	}

	slot := m.Slot
	_, exist := fop.log[slot]
	if !exist {
		log.Debugf("receives P2b from other nodes before DirectCommand from client: s=%d b=%s bo=%s",
			m.Slot, m.Ballot, m.OriBallot)
		fop.log[slot] = &entry{
			ballot:         m.Ballot,
			oriBallot:      m.OriBallot,
			commit:         false,
			share:          nil,
			quorum:         paxi.NewQuorum(),
			command:        nil,
			commandHandler: nil,
		}
		fop.slot = paxi.Max(fop.slot, m.Slot)

	}

	log.Debugf("s=%d e=%d | handling proposal's response: %s", fop.slot, fop.execute, m)

	fop.log[slot].propResponses = append(fop.log[slot].propResponses, &m)
	if m.OriBallot == fop.log[slot].oriBallot {
		fop.log[slot].quorum.ACK(m.ID)
	} else {
		log.Errorf("non equal original-ballot number | s=%d bori=%s bori'=%s",
			m.Slot, fop.log[slot].oriBallot, m.OriBallot)
		fop.log[slot].quorum.NACK(m.ID)
	}

	if fop.log[slot].quorum.Total() >= fop.numQF {
		if fop.log[slot].quorum.Size() >= fop.numQF {
			if !fop.log[slot].commit {
				fop.log[slot].commit = true
				fop.broadcastCommit(slot, fop.log[slot])

				// Ideally, before the coordinator receives |Qf| acks, the coordinator already
				// received DirectCommand from client which contain clear command. Here, we handle
				// if the coordinator receives |Qf| acks before the clear command.
				if fop.log[slot].command == nil {
					log.Debugf("want to execute but command is empty | s=%d bo=%s",
						m.Slot, fop.log[slot].oriBallot)
					fop.log[slot].resendClearCmd = true
					return
				}

				fop.exec()
			}
		} else {
			log.Errorf("s=%d | quorum total: %d, quorum size: %d",
				m.Slot, fop.log[slot].quorum.Total(), fop.log[slot].quorum.Size())
			fop.recoveryProcess(m.Slot)
		}
	}

	fop.exec()
}

// TODO: conflict happened, need to do recovery
func (fop *FastOPaxos) recoveryProcess(slot int) {
	log.Errorf("conflicted commands: %v", fop.log[slot].propResponses)
	panic("unimplemented")
}

func (fop *FastOPaxos) broadcastCommit(slot int, e *entry) {
	log.Debugf("broadcasting commit, slot=%d b=%s bo=%s", slot, fop.ballot, e.oriBallot)

	// Clear command is sent to fellow trusted nodes, so they
	// can also execute the command.
	for trustedNodeID := range fop.trustedNodeIDs {
		if trustedNodeID == fop.ID() {
			continue
		}
		fop.Send(trustedNodeID, P3{
			Ballot:    fop.ballot,
			Slot:      slot,
			Command:   e.command,
			OriBallot: e.oriBallot,
		})
	}
	for untrustedNodeID := range fop.untrustedNodeIDs {
		fop.Send(untrustedNodeID, P3{
			Ballot:    fop.ballot,
			Slot:      slot,
			Command:   nil,
			OriBallot: e.oriBallot,
		})
		// For optimization, we don't resend the secret-share in the commit message in this prototype.
		// For production usage, the proposer might need to send commit message with secret-share to
		// acceptors whose P2b messages is not in the phase-2 quorum processed; or the acceptors
		// can contact back the proposer asking the committed secret-share.
	}
}

func (fop *FastOPaxos) broadcastClearCommand(slot int, e *entry) {
	log.Debugf("broadcasting commit, slot=%d b=%s bo=%s", slot, fop.ballot, e.oriBallot)

	// Clear command is sent to fellow trusted nodes, so they
	// can also execute the command.
	for trustedNodeID := range fop.trustedNodeIDs {
		if trustedNodeID == fop.ID() {
			continue
		}
		fop.Send(trustedNodeID, P3c{
			Slot:      slot,
			Command:   e.command,
			OriBallot: e.oriBallot,
		})
	}
}

func (fop *FastOPaxos) handleCommitMessage(m P3) {
	fop.slot = paxi.Max(fop.slot, m.Slot)

	e, exist := fop.log[m.Slot]
	if exist {
		if len(m.Command) > 0 {
			e.command = m.Command
		}
		if len(m.Share) > 0 {
			e.share = m.Share
		}
		e.commit = true
	} else {
		fop.log[m.Slot] = &entry{
			ballot:    m.Ballot,
			oriBallot: m.OriBallot,
			commit:    true,
			share:     m.Share,
			command:   m.Command,
		}

		log.Debugf("commit comes before client's direct command| s=%d bo=%s", m.Slot, m.OriBallot)
	}

	fop.exec()
}

func (fop *FastOPaxos) handleCommittedClearCommand(m P3c) {
	if len(m.Command) > 0 {
		log.Errorf("receives empty P3c messages")
		return
	}
	if fop.isTrusted {
		log.Warningf("non trusted node receiving clear commands")
		return
	}
	e, exist := fop.log[m.Slot]
	if exist {
		e.oriBallot = m.OriBallot
		e.command = m.Command
		e.commit = true
	} else {
		fop.log[m.Slot] = &entry{
			oriBallot: m.OriBallot,
			commit:    true,
			command:   m.Command,
		}

		log.Debugf("P3c comes before client's direct command or commit | s=%d bo=%s", m.Slot, m.OriBallot)
	}

	fop.exec()
}

func (fop *FastOPaxos) exec() {
	for {
		e, ok := fop.log[fop.execute]
		if !ok || !e.commit {
			break
		}

		// a non-trusted node does not execute the command
		if !fop.isTrusted {
			log.Infof("id: %s", fop.ID())
			// has not received the DirectCommand from client
			if e.share == nil {
				return
			}

			// already received the DirectCommand before commit
			delete(fop.log, fop.execute)
			fop.execute++
			continue
		}

		// has not received direct command from client
		if e.command == nil {
			log.Infof("cmd not ready s=%d q=%d", fop.execute, e.quorum.Total())
			break
		}

		cmdReply := fop.execCommand(&e.command, fop.execute, e)

		//if e.commandHandler != nil && e.commandHandler.ReplyStream != nil {
		log.Debugf("send reply to client: %v", cmdReply)
		err := e.commandHandler.Reply(cmdReply)
		if err != nil {
			log.Errorf("failed to send CommandReply: %v", err)
		}
		//} else {
		//	log.Errorf("not sending result to client since the cmd handler is empty: %v", e)
		//}

		// clean the slot after the command is executed
		delete(fop.log, fop.execute)
		fop.execute++
	}
}

func (fop FastOPaxos) execCommand(byteCmd *paxi.BytesCommand, slot int, e *entry) *paxi.CommandReply {
	// prepare response for client
	reply := &paxi.CommandReply{
		Code:   paxi.CommandReplyOK,
		SentAt: 0,
		Data:   nil,
	}

	// parse command
	var cmd paxi.Command
	cmdType := paxi.GetDBCommandTypeFromBuffer(*byteCmd)
	switch cmdType {
	case paxi.TypeDBGetCommand:
		dbCmd := paxi.DeserializeDBCommandGet(*byteCmd)
		cmd.Key = dbCmd.Key
		reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
	case paxi.TypeDBPutCommand:
		dbCmd := paxi.DeserializeDBCommandPut(*byteCmd)
		cmd.Key = dbCmd.Key
		cmd.Value = dbCmd.Value
		reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
	default:
		log.Errorf("unknown client db command")
		reply.Code = paxi.CommandReplyErr
		reply.Data = []byte("unknown client db command")
		return reply
	}

	value := fop.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	log.Infof("cmd executed: s=%d op=%d key=%v, value=%x", fop.execute, cmdType, cmd.Key, value)
	return reply
}

func (fop *FastOPaxos) handleGetMetadataRequest(req *paxi.ClientCommand) {
	log.Debugf("handle get metadata request from client")
	reply := &paxi.CommandReply{
		Code:   paxi.CommandReplyOK,
		Data:   nil,
		SentAt: 123,
	}
	lep := fop.log[fop.execute]
	le := entry{}
	if lep != nil {
		le = *lep
	}
	getMetadataResp := GetMetadataResponse{
		NextSlot:  fop.slot,
		Execute:   fop.execute,
		LastEntry: le,
	}
	buff, _ := msgpack.Marshal(getMetadataResp)
	reply.Data = buff
	if err := req.Reply(reply); err != nil {
		log.Errorf("failed to send metadata to client: %s", err)
	}
}
