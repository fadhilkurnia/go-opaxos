package paxos

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"runtime"
	"time"
)

var isEncrypted = flag.Bool("encrypt", false, "encrypt the value before proposed")

const encryptionKey = "rahasiarahasiarahasiarahasia0123"

var encryptionKeyBytes = []byte(encryptionKey)

// entry in log
type entry struct {
	ballot          paxi.Ballot           // accepted ballot for the value
	commands        []paxi.BytesCommand   // a batch of commands (values)
	commandsHandler []*paxi.ClientCommand // corresponding handler for the commands, used to reply to client
	commit          bool                  // commit is true if the value is final/decided
	quorum          *paxi.Quorum          // quorum for phase 2
	encryptTime     []time.Duration       // corresponding encryption time for each commands, if isEncrypt is true
	cipherCommands  []paxi.BytesCommand   // the encrypted commands, if isEncrypted is true
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

	quorum               *paxi.Quorum             // phase 1 quorum
	requests             []*paxi.ClientCommand    // phase 1 pending requests
	protocolMessages     chan interface{}         // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientCommand // raw commands from clients
	pendingCommands      chan *paxi.ClientCommand // pending commands ready to be proposed
	onOffPendingCommands chan *paxi.ClientCommand // non nil pointer to pendingCommands after get response for phase 1

	Q1     func(*paxi.Quorum) bool
	Q2     func(*paxi.Quorum) bool
	buffer []byte // buffer used to persist ballot and accepted ballot

	// data for encrypted paxos
	numEncryptWorkers           int
	encryptJobs                 chan *paxi.ClientCommand
	pendingEncryptedCommands    chan *encryptedCommandData
	onOffPendingEncryptCommands chan *encryptedCommandData // non nil pointer to pendingEncryptedCommands after get response for phase 1

	// data for primary-backup mode: execution precede agreement
	isPrimaryBackupMode bool
	emulatedCmdData     map[uint32]*paxi.EmulatedCommandData
}

type encryptedCommandData struct {
	*paxi.ClientCommand
	cipherCommand []byte
	encTime       time.Duration
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	p := &Paxos{
		Node:                 n,
		log:                  make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:                 -1,
		quorum:               paxi.NewQuorum(),
		protocolMessages:     make(chan interface{}, paxi.GetConfig().ChanBufferSize),
		rawCommands:          make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize),
		pendingCommands:      make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize),
		onOffPendingCommands: nil,
		Q1:                   func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:                   func(q *paxi.Quorum) bool { return q.Majority() },
		buffer:               make([]byte, 18),
	}

	for _, opt := range options {
		opt(p)
	}

	if *isEncrypted {
		p.numEncryptWorkers = runtime.NumCPU()
		p.encryptJobs = make(chan *paxi.ClientCommand, paxi.GetConfig().ChanBufferSize)
		p.pendingEncryptedCommands = make(chan *encryptedCommandData, paxi.GetConfig().ChanBufferSize)
		p.onOffPendingEncryptCommands = nil

		// run encryption workers
		for i := 0; i < p.numEncryptWorkers; i++ {
			go func() {
				for cmd := range p.encryptJobs {
					startTime := time.Now()
					encryptedCommand, err := p.encrypt(cmd.RawCommand)
					encProcTime := time.Since(startTime)
					if err != nil {
						log.Errorf("failed to encrypt value: %v", err)
					}
					p.pendingEncryptedCommands <- &encryptedCommandData{
						ClientCommand: cmd,
						cipherCommand: encryptedCommand,
						encTime:       encProcTime,
					}
				}
			}()
		}
	}

	if *paxi.IsReqTracefile != "" {
		p.isPrimaryBackupMode = true
		traceData, err := paxi.ReadEmulatedCommandsFromTracefile(*paxi.IsReqTracefile)
		if err != nil {
			log.Errorf("failed to read tracefile: %s", err)
		}
		p.emulatedCmdData = traceData
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
			p.nonBlockingEnqueuePendingCommand(cmd)
			numRawCmd := len(p.rawCommands)
			for numRawCmd > 0 {
				cmd = <-p.rawCommands
				p.nonBlockingEnqueuePendingCommand(cmd)
				numRawCmd--
			}
			break

		// onOffPendingCommands is nil before this replica successfully running phase-1
		// see Paxos.HandleP1b for more detail
		case pCmd := <-p.onOffPendingCommands:
			p.P2a(pCmd)
			break

		// similar as in onOffPendingCommands, but for the encrypted mode
		case pCmd := <-p.onOffPendingEncryptCommands:
			p.P2aEncrypt(pCmd)
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

// nonBlockingEnqueuePendingCommand try to enqueue new command (value) to the pendingCommands
// channel, if the channel is full, goroutine is used to enqueue the commands. Thus
// this method *always* return, even if the channel is full.
func (p *Paxos) nonBlockingEnqueuePendingCommand(cmd *paxi.ClientCommand) {
	if *isEncrypted && *paxi.IsReqTracefile == "" {
		p.nonBlockingEnqueueEncJobsPendingCommand(cmd)
		return
	}
	isChannelFull := false
	if len(p.pendingCommands) == cap(p.pendingCommands) {
		log.Warningf("Channel for pending commands is full (len=%d)", len(p.pendingCommands))
		isChannelFull = true
	}

	if !isChannelFull {
		p.pendingCommands <- cmd
	} else {
		go func() {
			p.pendingCommands <- cmd
		}()
	}
}

func (p *Paxos) nonBlockingEnqueueEncJobsPendingCommand(cmd *paxi.ClientCommand) {
	isChannelFull := false
	if len(p.pendingCommands) == cap(p.pendingCommands) {
		log.Warningf("Channel for pending commands is full (len=%d)", len(p.pendingCommands))
		isChannelFull = true
	}

	if !isChannelFull {
		p.encryptJobs <- cmd
	} else {
		go func() {
			p.encryptJobs <- cmd
		}()
	}
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
func (p *Paxos) HandleRequest(r *paxi.ClientCommand) {
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

	p.persistHighestBallot(p.ballot)
	p.quorum.ACK(p.ID())

	p.Broadcast(P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *paxi.ClientCommand) {
	// prepare batch of commands to be proposed
	batchSize := len(p.onOffPendingCommands) + 1
	if batchSize > paxi.MaxBatchSize {
		batchSize = paxi.MaxBatchSize
	}
	commands := make([]paxi.BytesCommand, batchSize)
	commandsHandler := make([]*paxi.ClientCommand, batchSize)

	// put the first to-be-proposed command
	commands[0] = r.RawCommand
	commandsHandler[0] = r

	// put the remaining to-be-proposed commands
	for i := 1; i < batchSize; i++ {
		cmd := <-p.onOffPendingCommands
		commands[i] = cmd.RawCommand
		commandsHandler[i] = cmd
	}
	log.Debugf("batching %d commands", batchSize)

	// primary-backup approach: execute than agreement
	if *paxi.IsReqTracefile != "" {
		stateDiffs := p.emulateExecution(commandsHandler)
		// replace command with state-diffs (primary-backup approach)
		for i, diff := range stateDiffs {
			if *isEncrypted { // handle primary-backup + encryption mode
				var err error
				commands[i], err = p.encrypt(*diff)
				if err != nil {
					log.Errorf("failed to encrypt the state diffs: %s", err.Error())
				}
				continue
			}
			commands[i] = paxi.BytesCommand(*diff)
		}
	}

	// prepare the entry
	p.slot++
	p.log[p.slot] = &entry{
		ballot:          p.ballot,
		commands:        commands,
		commandsHandler: commandsHandler,
		quorum:          paxi.NewQuorum(),
		encryptTime:     nil,
		cipherCommands:  nil,
	}

	p.persistAcceptedValues(p.slot, p.ballot, commands)
	p.log[p.slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:   p.ballot,
		Slot:     p.slot,
		Commands: commands,
	}

	log.Infof("sending to all acceptor with data length of %d", len(commands[0]))

	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// P2aEncrypt is similar as in P2a, but for encrypted mode
func (p *Paxos) P2aEncrypt(r *encryptedCommandData) {
	// prepare batch of commands to be proposed
	batchSize := len(p.onOffPendingEncryptCommands) + 1
	if batchSize > paxi.MaxBatchSize {
		batchSize = paxi.MaxBatchSize
	}
	commands := make([]paxi.BytesCommand, batchSize)
	commandsHandler := make([]*paxi.ClientCommand, batchSize)
	cipherCommands := make([]paxi.BytesCommand, batchSize)
	encryptTimes := make([]time.Duration, batchSize)

	// put the first to-be-proposed command
	commands[0] = r.RawCommand
	commandsHandler[0] = r.ClientCommand
	encryptTimes[0] = r.encTime
	cipherCommands[0] = r.cipherCommand

	// put the remaining to-be-proposed commands
	for i := 1; i < batchSize; i++ {
		cmd := <-p.onOffPendingEncryptCommands
		commands[i] = cmd.RawCommand
		commandsHandler[i] = cmd.ClientCommand
		encryptTimes[i] = cmd.encTime
		cipherCommands[i] = cmd.cipherCommand
	}
	log.Debugf("batching %d commands", batchSize)

	// prepare the entry
	p.slot++
	p.log[p.slot] = &entry{
		ballot:          p.ballot,
		commands:        commands,
		commandsHandler: commandsHandler,
		quorum:          paxi.NewQuorum(),
		encryptTime:     encryptTimes,
		cipherCommands:  cipherCommands,
	}

	p.persistAcceptedValues(p.slot, p.ballot, commands)
	p.log[p.slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:   p.ballot,
		Slot:     p.slot,
		Commands: cipherCommands,
	}

	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// emulateExecution receives a batch of client command
func (p *Paxos) emulateExecution(emulatedCommands []*paxi.ClientCommand) []*[]byte {
	resultingStateDiffs := make([]*[]byte, len(emulatedCommands))
	for i, cmd := range emulatedCommands {
		if cmd.CommandType != paxi.TypeEmulatedCommand {
			log.Errorf(
				"expecting emulated command (type=%d), but got type=%d",
				paxi.TypeEmulatedCommand, cmd.CommandType)
			continue
		}

		eCmd, err := paxi.UnmarshalEmulatedCommand(cmd.RawCommand)
		if err != nil {
			log.Errorf("failed to unmarshal the emulated command: %s", err)
			continue
		}

		eCmdData := p.emulatedCmdData[eCmd.CommandID]
		if eCmdData == nil {
			log.Errorf("unrecognized command with id %d", eCmd.CommandID)
			continue
		}

		// emulate execution - exec time
		// the emulated execution time already include disk fsync
		log.Infof("emulating command %s for %v", eCmdData.CommandType, eCmdData.ExecTime)
		time.Sleep(eCmdData.ExecTime)

		// replace raw command with state diffs
		stateDiff := eCmdData.GenerateStateDiffsData()
		resultingStateDiffs[i] = &(stateDiff)
	}

	return resultingStateDiffs
}

// HandleP1a handles P1a message
func (p *Paxos) HandleP1a(m P1a) {
	// new leader
	if m.Ballot > p.ballot {
		p.persistHighestBallot(m.Ballot)
		p.ballot = m.Ballot
		p.active = false
	}

	l := make(map[int]CommandsBallot)
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandsBallot{p.log[s].commands, p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *Paxos) update(scb map[int]CommandsBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.commands = cb.Commands
				e.commandsHandler = nil
				if *isEncrypted {
					plainCmds := make([]paxi.BytesCommand, len(cb.Commands))
					for i, c := range cb.Commands {
						plain, err := p.decrypt(c)
						if err != nil {
							log.Errorf("failed to decrypt command: %v", err)
						}
						plainCmds[i] = plain
					}
					e.commands = plainCmds
					e.cipherCommands = cb.Commands
				}
			}
		} else {
			p.log[s] = &entry{
				ballot:          cb.Ballot,
				commands:        cb.Commands,
				commandsHandler: nil,
				commit:          false,
			}
			if *isEncrypted {
				plainCmds := make([]paxi.BytesCommand, len(cb.Commands))
				for i, c := range cb.Commands {
					plain, err := p.decrypt(c)
					if err != nil {
						log.Errorf("failed to decrypt command: %v", err)
					}
					plainCmds[i] = plain
				}
				p.log[s].commands = plainCmds
				p.log[s].cipherCommands = cb.Commands
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
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true

			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p2amsg := P2a{
					Ballot:   p.ballot,
					Slot:     i,
					Commands: p.log[i].commands,
				}
				if *isEncrypted {
					p2amsg.Commands = p.log[i].cipherCommands
				}
				p.Broadcast(p2amsg)
			}

			// propose pending commands
			if *isEncrypted && *paxi.IsReqTracefile == "" {
				p.onOffPendingEncryptCommands = p.pendingEncryptedCommands
			} else {
				p.onOffPendingCommands = p.pendingCommands
			}
		}
	}
}

// HandleP2a handles P2a message
func (p *Paxos) HandleP2a(m P2a) {
	if m.Ballot >= p.ballot {
		if m.Ballot != p.ballot {
			p.persistHighestBallot(m.Ballot)
		}

		p.ballot = m.Ballot
		p.active = false

		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)

		// update entry
		p.persistAcceptedValues(m.Slot, m.Ballot, m.Commands)
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				e.commands = m.Commands
				e.ballot = m.Ballot
				e.commandsHandler = nil
				if *isEncrypted {
					e.cipherCommands = m.Commands
				}
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:          m.Ballot,
				commands:        m.Commands,
				commandsHandler: nil,
				commit:          false,
			}
			if *isEncrypted {
				p.log[m.Slot].cipherCommands = m.Commands
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
	e, exist := p.log[m.Slot]
	if !exist || m.Ballot < e.ballot || e.commit {
		return
	}

	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.persistHighestBallot(m.Ballot)
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
				Ballot:   m.Ballot,
				Slot:     m.Slot,
				Commands: nil,
				// For optimization, we don't resend the values in the commit message in this prototype.
				// For production usage, the proposer need to send commit message with values to
				// acceptors whose P2b messages is not in the phase-2 quorum processed; or the acceptors
				// can contact back the proposer asking the committed value.
			})

			p.exec()
		}
	}
}

// HandleP3 handles phase 3 commit message
func (p *Paxos) HandleP3(m P3) {
	p.slot = paxi.Max(p.slot, m.Slot)

	e, exist := p.log[m.Slot]
	if exist {
		if m.Commands != nil {
			e.commands = m.Commands
		}
		e.commit = true
	} else {
		p.log[m.Slot] = &entry{
			ballot:          m.Ballot,
			commands:        m.Commands,
			commandsHandler: nil,
			commit:          true,
		}
		if *isEncrypted && m.Commands != nil {
			p.log[m.Slot].cipherCommands = m.Commands
		}
	}

	p.exec()
}

func (p *Paxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		log.Debugf("Replica %s execute [s=%d, cmds=%v]", p.ID(), p.execute, e.commands)

		for i, cmd := range e.commands {
			cmdReply := p.execCommands(i, &cmd, p.execute, e)
			if e.commandsHandler != nil && len(e.commandsHandler) > i && e.commandsHandler[i] != nil {
				err := e.commandsHandler[i].Reply(cmdReply)
				if err != nil {
					log.Errorf("failed to send CommandReply: %v", err)
				}
				e.commandsHandler[i] = nil
			}
		}

		// TODO: clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}

func (p *Paxos) execCommands(batchIdx int, byteCmd *paxi.BytesCommand, slot int, e *entry) *paxi.CommandReply {
	var cmd paxi.Command

	reply := &paxi.CommandReply{
		Code:   paxi.CommandReplyOK,
		SentAt: 0,
		Data:   nil,
	}

	if !p.IsLeader() {
		return reply
	}

	// primary-backup approach, the command is already executed before agreement
	// so here we directly return
	if *paxi.IsReqTracefile != "" {
		eCmd, err := paxi.UnmarshalEmulatedCommand(e.commandsHandler[batchIdx].RawCommand)
		if err != nil {
			log.Errorf("failed to unmarshal the emulated command: %s", err.Error())
			return reply
		}
		reply.SentAt = eCmd.SentAt // forward sentAt from client back to client
		return reply
	}

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

	value := p.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	if *paxi.GatherSecretShareTime || *paxi.ClientIsStateful {
		reply.Metadata = make(map[byte]interface{})
	}

	if *paxi.GatherSecretShareTime {
		if *isEncrypted {
			reply.Metadata[paxi.MetadataSecretSharingTime] = e.encryptTime[batchIdx]
		} else {
			reply.Metadata[paxi.MetadataSecretSharingTime] = time.Duration(0) * time.Second
		}
	}
	if *paxi.ClientIsStateful {
		reply.Metadata[paxi.MetadataAcceptedBallot] = e.ballot
		reply.Metadata[paxi.MetadataSlot] = slot
	}

	log.Debugf("op=%d key=%v, value=%x", cmdType, cmd.Key, value)
	return reply
}

func (p *Paxos) persistHighestBallot(b paxi.Ballot) {
	storage := p.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(p.buffer[:8], uint64(b))
	if _, err := storage.Write(p.buffer[:8]); err != nil {
		log.Errorf("failed to store max ballot %v", err)
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer: %v", err)
	}
}

func (p *Paxos) persistAcceptedValues(slot int, b paxi.Ballot, values []paxi.BytesCommand) {
	storage := p.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(p.buffer[:8], uint64(slot))
	binary.BigEndian.PutUint64(p.buffer[8:16], uint64(b))
	for i, val := range values {
		binary.BigEndian.PutUint16(p.buffer[16:18], uint16(i))
		if _, err := storage.Write(p.buffer[:18]); err != nil {
			log.Errorf("failed to store accepted ballot (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
		if _, err := storage.Write(val); err != nil {
			log.Errorf("failed to store accepted value (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer: %v", err)
	}
}

func (p *Paxos) encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKeyBytes)
	if err != nil {
		return nil, err
	}
	var iv [aes.BlockSize]byte
	stream := cipher.NewCTR(block, iv[:])
	ciphertext := make([]byte, len(plaintext))
	stream.XORKeyStream(ciphertext, plaintext)
	return ciphertext, nil
}

func (p *Paxos) decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(encryptionKeyBytes)
	if err != nil {
		return nil, err
	}
	var iv [aes.BlockSize]byte
	stream := cipher.NewCTR(block, iv[:])
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext)
	return plaintext, nil
}
