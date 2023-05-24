package untrustedopaxos

import (
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"strings"
)

// untrustedopaxos is an opaxos implementation when the consensus leader resides in the untrusted
// node. Thus, the leader can not see the user's private data.
// common execution path:
//    1. client =====broadcast to all===> the proposed value in a secret-shared form,
//       this includes sending a share to the leader (assumed to be node 1.1)
//    2. upon receiving a share of a secret value, the leader assign a slot number then
//       send accept messages (or proposal) to all the acceptors.
//    3. the acceptors only respond to the leader if they already received the value previously
//       from the client. Moreover, ballot number constraints also still apply as in ordinary Paxos.
//    4. acceptors send accept-ack (or ProposalResponse) back to the leader, the acceptors also
//       speculatively execute the secret-shared value (command) that they have. After doing
//       speculative execution, the acceptor send a response back to the client.
//    5. leader waits for a quorum of accept-ack messages from the acceptors before the leader
//       can safely broadcast the commit instruction (as in traditional Paxos). When enough accept-ack
//       messages are received, the leader send success response to the client.
//    6. the client wait until it get both t responses AND success response from the leader, after that
//       the client can notify the user that the command is successfully executed.
//       For some command that does not need response (e.g write) the client can directly notify
//       the user without having to wait for t secret-shares of the responses.

// entry in the log containing a batch of client's commands
type entry struct {
	ballot          paxi.Ballot            // the accepted ballot number
	commit          bool                   // commit indicates whether this entry is already committed or not
	quorum          *paxi.Quorum           // phase-2 quorum
	oriBallots      []ClientOriginalBallot // oriBallots is the original ballot of each command in the batch
	commands        []paxi.BytesCommand    // a batch of clear commands
	commandsHandler []*paxi.ClientCommand  // corresponding handler for each command in commands

	isBatchComplete bool // isBatchComplete indicate whether all the commands already received from the client
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config     *Config      // OPaxos configuration
	algorithm  string       // secret-sharing algorithm: shamir or ssms
	T          int          // the minimum number of secret shares to make it reconstructive
	N          int          // the number of nodes
	isProposer bool         // isProposer indicates whether this replica has proposer role or not
	isAcceptor bool         // isAcceptor indicates whether this replica has acceptor role or not
	isLeader   bool         // IsLeader indicates whether this instance perceives itself as a leader or not
	quorum     *paxi.Quorum // quorum store all ack'd responses for phase-1 / leader election

	log         map[int]*entry // log ordered by slot
	execute     int            // next execute slot number
	specExecute int            // next slot number to be speculatively executed
	ballot      paxi.Ballot    // highest ballot number
	slot        int            // highest slot number

	protocolMessages     chan interface{}         // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientCommand // raw commands from clients
	pendingCommands      chan *paxi.ClientCommand // pending commands that will be proposed
	onOffPendingCommands chan *paxi.ClientCommand // non nil pointer to pendingCommands after get response for phase 1

	buffer []byte // buffer used to persist ballot and accepted ballot

	Q1 func(*paxi.Quorum) bool
	Q2 func(*paxi.Quorum) bool

	outstandingCommands map[ClientOriginalBallot]*paxi.ClientCommand
	outstandingProposal map[ClientOriginalBallot]*P2a
}

// NewOPaxos creates new OPaxos instance (constructor)
func NewOPaxos(n paxi.Node, options ...func(*OPaxos)) *OPaxos {
	cfg := InitConfig(n.GetConfig())
	log.Debugf("config: %v", cfg)

	op := &OPaxos{
		Node:                 n,
		config:               &cfg,
		algorithm:            cfg.Protocol.SecretSharing,
		log:                  make(map[int]*entry, cfg.BufferSize),
		slot:                 -1,
		execute:              0,
		specExecute:          0,
		quorum:               paxi.NewQuorum(),
		protocolMessages:     make(chan interface{}, cfg.ChanBufferSize),
		rawCommands:          make(chan *paxi.ClientCommand, cfg.ChanBufferSize),
		pendingCommands:      make(chan *paxi.ClientCommand, cfg.ChanBufferSize),
		onOffPendingCommands: nil,
		T:                    cfg.Protocol.Threshold,
		N:                    n.GetConfig().N(),
		Q1:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
		buffer:               make([]byte, 32),
		outstandingCommands:  map[ClientOriginalBallot]*paxi.ClientCommand{},
		outstandingProposal:  map[ClientOriginalBallot]*P2a{},
	}

	for _, opt := range options {
		opt(op)
	}

	// parse roles for this node
	roles := strings.Split(cfg.Roles[n.ID()], ",")
	for _, r := range roles {
		if r == "proposer" {
			op.isProposer = true
		}
		if r == "acceptor" {
			op.isAcceptor = true
		}
	}

	return op
}

// run is the main event processing loop
// all commands from client and protocol messages processed here
func (op *OPaxos) run() {
	var err error
	for err == nil {
		select {
		case rCmd := <-op.rawCommands:
			op.nonBlockingEnqueuePendingCommands(rCmd)
			numRawCmd := len(op.rawCommands)
			for numRawCmd > 0 {
				rCmd = <-op.rawCommands
				op.nonBlockingEnqueuePendingCommands(rCmd)
				numRawCmd--
			}
			break

		// onOffPendingCommands is nil before this replica successfully running phase-1
		// see OPaxos.HandlePrepareResponse for more detail
		case pCmd := <-op.onOffPendingCommands:
			op.Propose(pCmd)
			break

		// protocolMessages has higher priority.
		// We try to empty the protocolMessages in each loop since for every
		// client command potentially it will create O(N) protocol messages (propose & commit),
		// where N is the number of nodes in the consensus cluster
		case pMsg := <-op.protocolMessages:
			err = op.handleProtocolMessages(pMsg)
			numPMsg := len(op.protocolMessages)
			for numPMsg > 0 && err == nil {
				err = op.handleProtocolMessages(<-op.protocolMessages)
				numPMsg--
			}
			break
		}
	}

	panic(fmt.Sprintf("untrusted-opaxos exited its main loop: %v", err))
}

func (op *OPaxos) nonBlockingEnqueuePendingCommands(rawCmd *paxi.ClientCommand) {

	cmdID := op.getCommandIDFromRawCmd(rawCmd.RawCommand, rawCmd.CommandType)
	op.outstandingCommands[cmdID] = rawCmd
	log.Debugf("received an outstanding request %d", cmdID)

	// handle if this node previously already receives a proposal for this value (command)
	// this node can accept the previously proposed value
	if !op.isLeader {
		log.Debugf("non-leader receiving client's command %s", cmdID)
		if op.outstandingProposal[cmdID] != nil {
			log.Debugf("receiving client's command after proposal")
			p2a := op.outstandingProposal[cmdID]
			e := op.log[p2a.Slot]
			proposalCmdBatch := p2a.CommandBatch

			if e == nil {
				log.Errorf("already receiving proposal, but entry is still nil (cob=%s)", cmdID)
			}

			isBatchComplete := true
			for _, cid := range proposalCmdBatch {
				if op.outstandingCommands[cid] == nil {
					isBatchComplete = false
					break
				}
			}
			if isBatchComplete && op.ballot >= p2a.Ballot {
				// gather the command, put into the log entry
				e.commands = make([]paxi.BytesCommand, len(proposalCmdBatch))
				e.commandsHandler = make([]*paxi.ClientCommand, len(proposalCmdBatch))
				for i, cid := range proposalCmdBatch {
					e.commands[i] = make([]byte, len(op.outstandingCommands[cid].RawCommand))
					copy(e.commands[i], op.outstandingCommands[cid].RawCommand)
					e.commandsHandler[i] = op.outstandingCommands[cid]
				}
				e.isBatchComplete = true
				op.speculativeExecution(e)

				log.Debugf("respond to the earlier proposal slot=%d cid=%d", p2a.Slot, cmdID)
				// reply to the proposer
				op.Send(p2a.Ballot.ID(), P2b{
					Ballot: op.ballot,
					Slot:   p2a.Slot,
					ID:     op.ID(),
				})
			}
		}
		return
	}

	isChannelFull := false
	if len(op.pendingCommands) == cap(op.pendingCommands) {
		log.Warningf("Channel for pending command is full (len=%d)", len(op.pendingCommands))
		isChannelFull = true
	}

	if !isChannelFull {
		op.pendingCommands <- rawCmd
	} else {
		go func() {
			op.pendingCommands <- rawCmd
		}()
	}
}

func (op *OPaxos) handleProtocolMessages(pmsg interface{}) error {
	switch pmsg.(type) {
	case paxi.BeLeaderRequest:
		op.Prepare()
		break
	case P1a:
		op.HandlePrepareRequest(pmsg.(P1a))
		break
	case P1b:
		op.HandlePrepareResponse(pmsg.(P1b))
		break
	case P2a:
		op.HandleProposeRequest(pmsg.(P2a))
		break
	case P2b:
		op.HandleProposeResponse(pmsg.(P2b))
		break
	case P3:
		op.HandleCommitRequest(pmsg.(P3))
		break
	default:
		log.Errorf("unknown protocol messages")
	}
	return nil
}

func (op *OPaxos) persistHighestBallot(b paxi.Ballot) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(op.buffer[:8], uint64(b))
	if _, err := storage.Write(op.buffer[:8]); err != nil {
		log.Errorf("failed to store max ballot %v", err)
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer: %s", err)
	}
}

func (op *OPaxos) persistAcceptedCommands(slot int, b paxi.Ballot, cmds []*paxi.ClientCommand) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	panic("unimplemented")
}

// Prepare initiates phase 1 of opaxos
func (op *OPaxos) Prepare() {
	if op.isLeader {
		return
	}
	op.ballot.Next(op.ID())
	op.quorum.Reset()

	log.Debugf("broadcasting prepare message %s", op.ballot.String())
	op.Broadcast(P1a{Ballot: op.ballot})
}

func (op *OPaxos) HandlePrepareRequest(m P1a) {
	// handle if there is a new leader with higher ballot number
	// promise not to accept value with lower ballot
	if m.Ballot > op.ballot {
		op.persistHighestBallot(m.Ballot)
		op.ballot = m.Ballot
		op.isLeader = false
		op.onOffPendingCommands = nil
	}

	// send command-shares to proposer, if previously this acceptor
	// already accept some command-shares
	l := make(map[int]CommandShare)
	for s := op.execute; s <= op.slot; s++ {
		if op.log[s] == nil || op.log[s].commit {
			continue
		}

		sharedCommand := make([][]byte, len(op.log[s].commands))
		// filter out the value in put request, for privacy reason
		for i := 0; i < len(op.log[s].commands); i++ {
			cmd := op.log[s].commandsHandler[i]
			if cmd.CommandType == paxi.TypeDBGetCommand {
				// get request can be shared with other untrusted node
				sharedCommand[i] = make([]byte, len(cmd.RawCommand))
				copy(sharedCommand[i], cmd.RawCommand)
			} else if cmd.CommandType == paxi.TypeDBPutCommand {
				// put request need to be filtered by omitting the value
				// before sharing it to other untrusted node
				pcmd := paxi.DeserializeDBCommandPut(cmd.RawCommand)
				pcmd.Value = nil
				filteredCmd := pcmd.Serialize()
				sharedCommand[i] = make([]byte, len(filteredCmd))
				copy(sharedCommand[i], filteredCmd)
			} else {
				log.Fatalf("unrecognized command type %d", cmd.CommandType)
			}
		}

		l[s] = CommandShare{
			Ballot:      op.log[s].ballot,
			OriBallots:  op.log[s].oriBallots,
			RawCommands: sharedCommand,
			ID:          op.ID(),
		}
	}

	// Send P1b back to proposer.
	// Note that, for optimization, P1b is also sent
	// even if m.Ballot <= op.ballot. Technically this node
	// can ignore prepare message in that case.
	op.Send(m.Ballot.ID(), P1b{
		Ballot: op.ballot,
		ID:     op.ID(),
		Log:    l,
	})
}

func (op *OPaxos) HandlePrepareResponse(m P1b) {
	log.Debugf("handling prepare response msg=%s is_leader=%t", m, op.isLeader)

	// handle old message from the previous leadership
	if m.Ballot < op.ballot || op.isLeader {
		return
	}

	// rejection message, yield to another proposer
	// with higher ballot number
	if m.Ballot > op.ballot {
		op.persistHighestBallot(m.Ballot)
		op.ballot = m.Ballot
		op.isLeader = false
		op.onOffPendingCommands = nil

		// for real system, here we need to forward the un-proposed commands (values)
		// to the new leader (or candidate) OR try to execute
		// new leader-election with higher ballot number
		return
	}

	// ack message, if the response was sent for this proposer,
	// in the proposer's current round (the same ballot)
	if m.Ballot == op.ballot && m.Ballot.ID() == op.ID() {
		op.updateLog(m.Log)
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
			log.Debugf("opening pending commands channel len=%d, %d",
				len(op.pendingCommands), len(op.rawCommands))
		}
	}
}

func (op *OPaxos) updateLog(acceptedCmds map[int]CommandShare) {
	if len(acceptedCmds) == 0 {
		return
	}
	panic("unimplemented")
}

// proposeUncommittedEntries does the recovery process
func (op *OPaxos) proposeUncommittedEntries() {
	if op.execute > op.slot {
		return
	}
	panic("unimplemented")
}

// Propose initiates phase 2 of OPaxos
func (op *OPaxos) Propose(r *paxi.ClientCommand) {
	// prepare batch of commands to be proposed
	batchSize := len(op.onOffPendingCommands) + 1
	if batchSize > paxi.MaxBatchSize {
		batchSize = paxi.MaxBatchSize
	}
	oriBallots := make([]ClientOriginalBallot, batchSize)
	commands := make([]paxi.BytesCommand, batchSize)
	commandsHandler := make([]*paxi.ClientCommand, batchSize)

	// handle the first command r in the batch
	oriBallots[0] = op.getCommandIDFromRawCmd(r.RawCommand, r.CommandType)
	commands[0] = r.RawCommand
	commandsHandler[0] = r

	// handle the remaining commands in the batch
	for i := 1; i < batchSize; i++ {
		cmd := <-op.onOffPendingCommands
		oriBallots[i] = op.getCommandIDFromRawCmd(cmd.RawCommand, cmd.CommandType)
		commands[i] = cmd.RawCommand
		commandsHandler[i] = cmd
	}
	log.Debugf("batching %d commands", batchSize)

	// prepare the entry that contains a batch of commands
	op.slot++
	op.log[op.slot] = &entry{
		ballot:          op.ballot,
		oriBallots:      oriBallots,
		commit:          false,
		commands:        commands,
		commandsHandler: commandsHandler,
		quorum:          paxi.NewQuorum(),
		isBatchComplete: true,
	}

	log.Debugf("get batch of commands for slot %d", op.slot)

	op.persistAcceptedCommands(op.slot, op.ballot, commandsHandler)
	op.log[op.slot].quorum.ACK(op.ID())

	// broadcast propose message to the acceptors
	op.Broadcast(P2a{
		Ballot:       op.ballot,
		Slot:         op.slot,
		CommandBatch: oriBallots,
	})
}

func (op *OPaxos) getCommandIDFromRawCmd(rawCmd []byte, cmdType byte) ClientOriginalBallot {
	if cmdType != paxi.TypeDBGetCommand && cmdType != paxi.TypeDBPutCommand {
		log.Fatalf("unrecognized command type %d", cmdType)
	}

	if cmdType == paxi.TypeDBGetCommand {
		cmd := paxi.DeserializeDBCommandGet(rawCmd)
		return ClientOriginalBallot(cmd.CommandID)
	}
	if cmdType == paxi.TypeDBPutCommand {
		cmd := paxi.DeserializeDBCommandPut(rawCmd)
		return ClientOriginalBallot(cmd.CommandID)
	}

	return 0
}

func (op *OPaxos) HandleProposeRequest(m P2a) {
	if m.Ballot >= op.ballot {
		if m.Ballot != op.ballot {
			op.persistHighestBallot(m.Ballot)
		}

		op.ballot = m.Ballot
		op.isLeader = false

		// update slot number
		op.slot = paxi.Max(op.slot, m.Slot)

		// due to asynchrony, there are two possible cases:
		// 1. the leader propose a command that this node has already received
		// 2. the leader propose a command that this node has not received yet
		log.Debugf("handle proposal, first-cid:%s", m.CommandBatch[0])
		batchSize := len(m.CommandBatch)
		isBatchReceived := true
		oriBallots := make([]ClientOriginalBallot, batchSize)
		commands := make([]paxi.BytesCommand, batchSize)
		clientCommands := make([]*paxi.ClientCommand, batchSize)
		for i := 0; i < batchSize; i++ {
			// TODO: complete this, match with outstanding request
			cmdBallot := m.CommandBatch[i]
			op.outstandingProposal[cmdBallot] = &m

			oriBallots[i] = cmdBallot
			if op.outstandingCommands[cmdBallot] != nil {
				// case-1 : this node already receive the proposed value.
				// Thus, this node can directly accept the leader's proposal.
				// Also, this node can speculatively execute the proposed value (command)
				// and return the result to the client.
				log.Debugf("receiving proposal after the client's command")
				clientCommands[i] = op.outstandingCommands[cmdBallot]
				commands[i] = op.outstandingCommands[cmdBallot].RawCommand
			} else {
				// case-2 : this node has not received the proposed value.
				// thus, this node need to wait before responding to the leader's proposal.
				log.Debugf("receiving proposal but the client's command is not yet received cid=%s", cmdBallot)
				isBatchReceived = false
				clientCommands[i] = nil
				commands[i] = nil
			}
		}

		if e, exists := op.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				e.ballot = m.Ballot
				e.commit = false
				e.oriBallots = oriBallots
				e.commands = commands
				e.commandsHandler = clientCommands
			}
		} else {
			log.Debugf("create entry, first-cid:%s", m.CommandBatch[0])
			op.log[m.Slot] = &entry{
				ballot:          m.Ballot,
				oriBallots:      oriBallots,
				commands:        commands,
				commandsHandler: clientCommands,
				commit:          false,
			}
		}

		if !isBatchReceived {
			log.Debugf("receive proposal but not yet received the value from the client s=%d, first-cid=%s", m.Slot, m.CommandBatch[0])
			return
		}

		// update entry
		op.log[m.Slot].isBatchComplete = true
		op.persistAcceptedCommands(m.Slot, m.Ballot, clientCommands)

		// speculatively execute and return the result to the client
		op.speculativeExecution(op.log[m.Slot])
	}

	// reply to proposer
	op.Send(m.Ballot.ID(), P2b{
		Ballot: op.ballot,
		Slot:   m.Slot,
		ID:     op.ID(),
	})
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
			op.broadcastCommit(m.Slot, m.Ballot)

			// update execute slot idx
			op.exec()
		}
	}
}

// broadcastCommit sends clear command to fellow trusted nodes, but
// secret-shared command to untrusted nodes.
func (op *OPaxos) broadcastCommit(slot int, ballot paxi.Ballot) {
	log.Debugf("broadcasting commit, slot=%d b=%s", slot, ballot)
	op.Broadcast(P3{
		Ballot:       ballot,
		Slot:         slot,
		CommandBatch: op.log[slot].oriBallots,
	})
}

func (op *OPaxos) exec() {
	for {
		e, ok := op.log[op.execute]
		if !ok || !e.commit || !e.isBatchComplete {
			break
		}

		if len(e.commands) > 0 {
			log.Debugf("executing command in slot=%d first-cid=%s", op.execute, e.oriBallots[0])
			for i, cmd := range e.commands {
				cmdReply := op.execCommands(&cmd, op.execute, e, i)
				if e.commandsHandler != nil && len(e.commandsHandler) > i && e.commandsHandler[i] != nil {
					err := e.commandsHandler[i].Reply(cmdReply)
					if err != nil {
						log.Errorf("failed to send CommandReply: %v", err)
					}
					e.commandsHandler[i] = nil
				}
			}
		}
		// non-leader already speculatively execute the command after receiving the proposal
		// from the leader, so this non-leader node need to persist the previously
		// speculatively executed command.
		if !op.isLeader {
			// TODO: implement me!!!
		}

		// TODO clean up the log periodically
		// TODO clean up outstanding proposal, since it is committed already
		delete(op.log, op.execute)
		op.execute++
	}
}

// speculativeExecution speculatively executes and return the result to the client.
// This is a speculative execution since the value (command) is not committed yet.
func (op *OPaxos) speculativeExecution(e *entry) {
	// TODO: implement it like this instead
	// at any given time, the execution index is always less than or equal to the
	// index of speculative execution:
	//       op.execute <= op.specExecute
	// for {
	//	x, ok := op.log[op.specExecute]
	//	if !ok || !x.commit || op.specExecute {
	//		break
	//	}
	//
	//	op.specExecute++
	//}

	batchSize := len(e.oriBallots)
	for i := 0; i < batchSize; i++ {
		cmdHandler := e.commandsHandler[i]
		ccmd := e.commands[i]

		reply := &paxi.CommandReply{
			Code:   paxi.CommandReplyOK,
			SentAt: 0,
			Data:   nil,
		}

		cmdType := paxi.GetDBCommandTypeFromBuffer(ccmd)
		var cmd paxi.Command
		switch cmdType {
		case paxi.TypeDBGetCommand:
			dbCmd := paxi.DeserializeDBCommandGet(ccmd)
			cmd.Key = dbCmd.Key
			reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
			reply.CommandID = dbCmd.CommandID
		case paxi.TypeDBPutCommand:
			dbCmd := paxi.DeserializeDBCommandPut(ccmd)
			cmd.Key = dbCmd.Key
			cmd.Value = dbCmd.Value
			reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
			reply.CommandID = dbCmd.CommandID
			log.Debugf("executing put command with k=%d v=%v", cmd.Key, cmd.Value)
		default:
			log.Fatalf("unknown client db command %d", cmdType)
			reply.Code = paxi.CommandReplyErr
			reply.Data = []byte("unknown client db command")
		}

		// TODO: Implement execute on the snapshot of the state!
		// TODO: not on the actual state
		value := op.Execute(cmd)

		// data is the previous value for put request or the
		// stored value for read request
		// the data is in a secret shared form.
		if cmd.Value == nil {
			reply.Data = value
		}

		// send response to the client
		if cmdHandler != nil {
			if err := cmdHandler.Reply(reply); err != nil {
				log.Errorf("failed to send response to the client: %s", err.Error())
			}
			e.commandsHandler[i] = nil
		}
	}
}

// execCommands parse cmd since it can be any type of command
func (op *OPaxos) execCommands(byteCmd *paxi.BytesCommand, slot int, e *entry, cid int) *paxi.CommandReply {
	var cmd paxi.Command

	// by default, we do not send all the data, to make the response compact
	reply := &paxi.CommandReply{
		Code:   paxi.CommandReplyOK,
		SentAt: 0,
		Data:   nil,
	}

	if !op.isLeader {
		return reply
	}

	cmdType := paxi.GetDBCommandTypeFromBuffer(*byteCmd)
	switch cmdType {
	case paxi.TypeDBGetCommand:
		dbCmd := paxi.DeserializeDBCommandGet(*byteCmd)
		cmd.Key = dbCmd.Key
		reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
		reply.CommandID = dbCmd.CommandID
	case paxi.TypeDBPutCommand:
		dbCmd := paxi.DeserializeDBCommandPut(*byteCmd)
		cmd.Key = dbCmd.Key
		cmd.Value = dbCmd.Value
		reply.SentAt = dbCmd.SentAt // forward sentAt from client back to client
		reply.CommandID = dbCmd.CommandID
		log.Debugf("executing put command with k=%d v=%v", cmd.Key, cmd.Value)
	default:
		log.Errorf("unknown client db command")
		reply.Code = paxi.CommandReplyErr
		reply.Data = []byte("unknown client db command")
		return reply
	}

	value := op.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	if *paxi.GatherSecretShareTime || *paxi.ClientIsStateful || op.isLeader {
		reply.Metadata = make(map[byte]interface{})
	}

	if op.isLeader {
		reply.Metadata[paxi.MetadataLeaderAck] = true
	}
	if *paxi.ClientIsStateful {
		reply.Metadata[paxi.MetadataAcceptedBallot] = e.ballot
		reply.Metadata[paxi.MetadataSlot] = slot
	}

	log.Debugf("op=%d key=%v, value=%x", cmdType, cmd.Key, value)
	return reply
}

func (op *OPaxos) HandleCommitRequest(m P3) {
	op.slot = paxi.Max(op.slot, m.Slot)

	e, exist := op.log[m.Slot]
	if exist {
		e.commit = true
		e.oriBallots = m.CommandBatch
	} else {
		op.log[m.Slot] = &entry{
			ballot:     m.Ballot,
			oriBallots: m.CommandBatch,
			commands:   nil,
			commit:     true,
		}

		panic("unimplemented: when commit precede proposal")
	}

	op.exec()
}
