package opaxos

import (
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/shamir"
	"runtime"
	"strings"
	"time"
)

// entry in the log containing a batch of clear commands OR
// a batch of secret-share of commands, one for each command.
type entry struct {
	ballot    paxi.Ballot // the accepted ballot number
	oriBallot paxi.Ballot // oriBallot is the original ballot of the secret value
	commit    bool        // commit indicates whether this entry is already committed or not

	// field for acceptor
	sharesBatch []SecretShare // a batch secret-shares of each command in commands, one share per command. Used in an untrusted acceptor.

	// fields for trusted proposer
	// TODO: coalesce the fields below into a single pointer to a struct, this reduces the memory from 8*5=40 bytes to 8 bytes
	//metadata        *entryMetadata
	quorum          *paxi.Quorum          // phase-2 quorum
	commands        []paxi.BytesCommand   // a batch of clear commands, used in trusted node
	commandsHandler []*paxi.ClientCommand // corresponding handler for each command in commands, used in trusted node
	ssTime          []time.Duration       // time needed for secret-sharing process for each command in commands
	commandShares   []*CommandShare       // collection of command from multiple acceptors, with the same slot number
}

type entryMetadata struct {
	quorum          *paxi.Quorum          // phase-2 quorum
	commands        *paxi.Quorum          // a batch of clear commands, used in trusted node. correspond to entry.sharesBatch
	commandsHandler []*paxi.ClientCommand // corresponding handler for each command in commands, used in trusted node
	ssTime          []time.Duration       // time needed for secret-sharing process for each command in commands
	commandShares   []*CommandShare       // collection of command from multiple acceptors, with the same slot number
}

// OPaxos instance
type OPaxos struct {
	paxi.Node

	config        *Config      // OPaxos configuration
	algorithm     string       // secret-sharing algorithm: shamir or ssms
	T             int          // the minimum number of secret shares to make it reconstructive
	N             int          // the number of nodes
	isProposer    bool         // isProposer indicates whether this replica has proposer role or not
	isAcceptor    bool         // isAcceptor indicates whether this replica has acceptor role or not
	isLeader      bool         // IsLeader indicates whether this instance perceives itself as a leader or not
	isTrustedNode bool         // isTrustedNode is true if this node is a proposer (resides in the trusted node)
	quorum        *paxi.Quorum // quorum store all ack'd responses for phase-1 / leader election

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number

	numSSWorkers         int                       // number of workers to secret-share client's raw command
	defaultSSWorker      SecretSharingWorker       // secret-sharing worker for reconstruction only, used without channel
	protocolMessages     chan interface{}          // prepare, propose, commit, etc
	rawCommands          chan *paxi.ClientCommand  // raw commands from clients
	ssJobs               chan *paxi.ClientCommand  // raw commands ready to be secret-shared
	pendingCommands      chan *SecretSharedCommand // pending commands that will be proposed
	onOffPendingCommands chan *SecretSharedCommand // non nil pointer to pendingCommands after get response for phase 1

	buffer []byte // buffer used to persist ballot and accepted ballot

	Q1 func(*paxi.Quorum) bool
	Q2 func(*paxi.Quorum) bool

	trustedNodeIDs   map[paxi.ID]bool
	untrustedNodeIDs map[paxi.ID]bool

	// data for primary-backup mode: execution precede agreement
	isPrimaryBackupMode bool
	emulatedCmdData     map[uint32]*paxi.EmulatedCommandData
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
		quorum:               paxi.NewQuorum(),
		protocolMessages:     make(chan interface{}, cfg.ChanBufferSize),
		rawCommands:          make(chan *paxi.ClientCommand, cfg.ChanBufferSize),
		ssJobs:               make(chan *paxi.ClientCommand, cfg.ChanBufferSize),
		pendingCommands:      make(chan *SecretSharedCommand, cfg.ChanBufferSize),
		onOffPendingCommands: nil,
		numSSWorkers:         runtime.NumCPU(),
		T:                    cfg.Protocol.Threshold,
		N:                    n.GetConfig().N(),
		Q1:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum1) },
		Q2:                   func(q *paxi.Quorum) bool { return q.CardinalityBasedQuorum(cfg.Protocol.Quorum2) },
		buffer:               make([]byte, 32),
		trustedNodeIDs:       make(map[paxi.ID]bool),
		untrustedNodeIDs:     make(map[paxi.ID]bool),
	}

	for _, opt := range options {
		opt(op)
	}

	// parse roles for this node
	roles := strings.Split(cfg.Roles[n.ID()], ",")
	for _, r := range roles {
		if r == "proposer" {
			op.isProposer = true
			op.isTrustedNode = true
			op.initDefaultSecretSharingWorker()
		}
		if r == "acceptor" {
			op.isAcceptor = true
		}
	}

	// parse other trusted and untrusted node
	for nodeID, nodeRolesStr := range cfg.Roles {
		nodeRoles := strings.Split(nodeRolesStr, ",")
		isNodeTrusted := false
		for _, r := range nodeRoles {
			if r == "proposer" {
				op.trustedNodeIDs[nodeID] = true
				isNodeTrusted = true
			}
		}
		if !isNodeTrusted {
			op.untrustedNodeIDs[nodeID] = true
		}
	}

	if *paxi.IsReqTracefile != "" {
		op.isPrimaryBackupMode = true
		traceData, err := paxi.ReadEmulatedCommandsFromTracefile(*paxi.IsReqTracefile)
		if err != nil {
			log.Errorf("failed to read tracefile: %s", err)
		}
		op.emulatedCmdData = traceData
	}

	return op
}

func (op *OPaxos) initAndRunSecretSharingWorker() {
	numShares := op.N
	numThreshold := op.T

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2
	}

	worker := NewWorker(op.algorithm, numShares, numThreshold)
	worker.StartProcessingInput(op.ssJobs, op.pendingCommands)
}

func (op *OPaxos) initDefaultSecretSharingWorker() {
	numShares := op.N
	numThreshold := op.T

	if op.config.Thrifty {
		numShares = op.config.Protocol.Quorum2
	}

	op.defaultSSWorker = NewWorker(op.algorithm, numShares, numThreshold)
}

// run is the main event processing loop
// all commands from client and protocol messages processed here
func (op *OPaxos) run() {
	var err error
	for err == nil {
		select {
		case rCmd := <-op.rawCommands:
			// start phase-1 if this proposer has not started it previously
			if !op.isLeader && op.ballot.ID() != op.ID() {
				op.Prepare()
			}

			// secret-shares the first and the remaining raw commands
			op.nonBlockingEnqueueSSJobs(rCmd)
			numRawCmd := len(op.rawCommands)
			for numRawCmd > 0 {
				rCmd = <-op.rawCommands
				op.nonBlockingEnqueueSSJobs(rCmd)
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

	panic(fmt.Sprintf("opaxos exited its main loop: %v", err))
}

// nonBlockingEnqueueSSJobs try to enqueue new raw command (value) to the ssJobs
// channel, if the channel is full, goroutine is used to enqueue the commands. Thus,
// this method *always* return, even if the channel is full.
func (op *OPaxos) nonBlockingEnqueueSSJobs(rawCmd *paxi.ClientCommand) {
	// handle primary-backup emulation
	if *paxi.IsReqTracefile != "" {
		op.nonBlockingEnqueueEmptySSCmd(rawCmd)
		return
	}
	isChannelFull := false
	if len(op.ssJobs) == cap(op.ssJobs) {
		log.Warningf("Channel for ss jobs is full (len=%d)", len(op.ssJobs))
		isChannelFull = true
	}

	if !isChannelFull {
		op.ssJobs <- rawCmd
	} else {
		go func() {
			op.ssJobs <- rawCmd
		}()
	}
}

// nonBlockingEnqueueEmptySSCmd is for primary backup approach where we execute before agreement,
// thus we directly enqueue empty SecretSharedCommand directly into pendingCommands, without
// doing secret-sharing
func (op *OPaxos) nonBlockingEnqueueEmptySSCmd(rawCmd *paxi.ClientCommand) {
	isChannelFull := false
	if len(op.pendingCommands) == cap(op.pendingCommands) {
		log.Warningf("Channel for pendingCommands is full (len=%d)", len(op.pendingCommands))
		isChannelFull = true
	}

	if !isChannelFull {
		op.pendingCommands <- &SecretSharedCommand{
			ClientCommand: rawCmd,
			SSTime:        0,
			Shares:        nil,
		}
	} else {
		go func() {
			op.pendingCommands <- &SecretSharedCommand{
				ClientCommand: rawCmd,
				SSTime:        0,
				Shares:        nil,
			}
		}()
	}
}

func (op *OPaxos) handleProtocolMessages(pmsg interface{}) error {
	switch pmsg.(type) {
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

func (op *OPaxos) persistAcceptedShares(slot int, b paxi.Ballot, bori paxi.Ballot, shares []SecretShare) {
	storage := op.GetStorage()
	if storage == nil {
		return
	}

	binary.BigEndian.PutUint64(op.buffer[:8], uint64(slot))
	binary.BigEndian.PutUint64(op.buffer[8:16], uint64(b))
	binary.BigEndian.PutUint64(op.buffer[16:24], uint64(bori))
	for i, val := range shares {
		binary.BigEndian.PutUint16(op.buffer[24:26], uint16(i))
		if _, err := storage.Write(op.buffer[:26]); err != nil {
			log.Errorf("failed to store accepted ballot (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
		if _, err := storage.Write(val); err != nil {
			log.Errorf("failed to store accepted shares (s=%d, b=%s, i=%d): %v", slot, b, i, err)
		}
	}

	if err := storage.Flush(); err != nil {
		log.Errorf("failed to flush data to underlying file writer: %s", err)
	}
}

// Prepare initiates phase 1 of opaxos
func (op *OPaxos) Prepare() {
	if op.isLeader {
		return
	}
	op.ballot.Next(op.ID())
	op.quorum.Reset()
	op.selfPrepare()

	log.Debugf("broadcasting prepare message %s", op.ballot.String())
	op.Broadcast(P1a{Ballot: op.ballot})
}

func (op *OPaxos) selfPrepare() {
	// collect CommandShare from itself
	for i := op.execute; i <= op.slot; i++ {
		if op.log[i] == nil {
			continue
		}
		consensusInstance := op.log[i]

		consensusInstance.commandShares = nil
		if consensusInstance.sharesBatch != nil {
			newCommandShare := &CommandShare{
				Ballot:      consensusInstance.ballot,
				OriBallot:   consensusInstance.oriBallot,
				SharesBatch: consensusInstance.sharesBatch,
				ID:          op.ID(),
			}
			consensusInstance.commandShares = []*CommandShare{newCommandShare}

			log.Debugf("recovery - self promise, slot=%d acc=%s bacc=%s, bori=%s val=%x",
				i, op.ID(), consensusInstance.ballot, consensusInstance.oriBallot, consensusInstance.sharesBatch)
		}
	}

	// self ack
	op.persistHighestBallot(op.ballot)
	op.quorum.ACK(op.ID())
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

		l[s] = CommandShare{
			Ballot:      op.log[s].ballot,
			OriBallot:   op.log[s].oriBallot,
			SharesBatch: op.log[s].sharesBatch,
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
			log.Debugf("opening pending commands channel len=%d, %d %d",
				len(op.pendingCommands), len(op.ssJobs), len(op.rawCommands))
		}
	}
}

// updateLog updates the entry's accepted ballot and oriBallot, also collecting
// the secret-shares from the acceptors that later
// will be used in proposeUncommittedEntries() for recovery.
func (op *OPaxos) updateLog(acceptedCmdShares map[int]CommandShare) {
	if !op.isProposer {
		log.Fatalf("updateLog can only be executed by proposer in a trusted node")
	}
	if len(acceptedCmdShares) == 0 {
		return
	}

	for slot, cmdShare := range acceptedCmdShares {
		op.slot = paxi.Max(op.slot, slot)

		if e, exist := op.log[slot]; exist {
			if !e.commit && cmdShare.Ballot >= e.ballot {
				newCmdShare := acceptedCmdShares[slot]

				e.ballot = newCmdShare.Ballot
				e.oriBallot = newCmdShare.OriBallot
				e.commands = nil
				e.commandsHandler = nil
				e.commandShares = append(e.commandShares, &newCmdShare)
			}

			log.Debugf("recovery - get non empty promise, slot=%d acc=%s bacc=%s bori=%s val=%x",
				slot, cmdShare.ID, cmdShare.Ballot, cmdShare.OriBallot, cmdShare.SharesBatch)
		} else {
			newCmdShare := acceptedCmdShares[slot]

			op.log[slot] = &entry{
				ballot:        newCmdShare.Ballot,
				oriBallot:     newCmdShare.OriBallot,
				commandShares: []*CommandShare{&newCmdShare},
			}
		}

	}
}

// proposeUncommittedEntries does the recovery process
func (op *OPaxos) proposeUncommittedEntries() {
	log.Debugf("starting recovery process for slot:%d-%d", op.execute, op.slot)

	for i := op.execute; i <= op.slot; i++ {
		var entryCmdBatch []paxi.BytesCommand // a batch of command that will later be proposed
		var proposalShares [][]SecretShare    // N batch of secret-share of entryCmdBatch

		// If the entry is committed, for now we ignore that.
		// In a real system, we might need to send commit message
		// to other nodes.
		if op.log[i].commit {
			continue
		}

		// For now, we are ignoring a nil gap.
		// In a real system, we might need to also propose the nil gap or propose
		// any value from the pending commands.
		if op.log[i] == nil {
			continue
		}

		log.Debugf("starting recovery process for slot=%d", i)

		// Find secret-share which has the same original-ballot
		// as the original-ballot of the share with the highest ballot.
		// From updateLog(...), we got:
		// - op.log[i].ballot contains the highest ballot from the acceptors.
		// - op.log[i].oriBallot contains the corresponding original ballot.
		// We are going to collect share with same original-ballot (identifier)
		log.Debugf("recovery - highest accepted ballot: %s", op.log[i].ballot)
		log.Debugf("recovery - corresponding original ballot: %s", op.log[i].oriBallot)
		log.Debugf("recovery - # of collected shares: %d", len(op.log[i].commandShares))
		var recoveryShares []*CommandShare
		for sid, share := range op.log[i].commandShares {
			if share.OriBallot == op.log[i].oriBallot {
				recoveryShares = append(recoveryShares, share)
			}

			log.Debugf("recovery - slot=%d share#=%d acc=%s bacc=%s bori=%s ss=%x",
				i, sid, share.ID, share.Ballot, share.OriBallot, share.SharesBatch)
		}

		// Not enough shares to recovery a secret value whose one of the
		// share has the highest ballot. Theoretically, the proposer can
		// pick any value. In a real system, we might want to pick a value
		// from pending commands. But here, we just skip the slot and
		// make the slot nil.
		numRecoveryShares := len(recoveryShares)
		log.Debugf("recovery - number of recovered shares: %d, threshold: %d", numRecoveryShares, op.T)
		if numRecoveryShares < op.T {
			op.log[i] = nil
			continue
		}

		// all share must have the same batch size, otherwise it is
		// a byzantine behavior that we do not handle.
		batchSize := len(recoveryShares[0].SharesBatch)
		for _, share := range recoveryShares {
			if len(share.SharesBatch) != batchSize {
				log.Fatalf("found share with different batch size (s=%d, b=%s, ob=%s) from %s",
					i, share.Ballot, share.OriBallot, share.ID,
				)
			}
		}

		// The recovery process: regenerate the shares for each reconstructed command in the batch
		allSharesBatch := make([]*[][]byte, batchSize)
		entryCmdBatch = make([]paxi.BytesCommand, batchSize)
		for j := 0; j < batchSize; j++ {
			secretSharedCommand := make([][]byte, numRecoveryShares)
			for k := 0; k < numRecoveryShares; k++ {
				secretSharedCommand[k] = recoveryShares[k].SharesBatch[j]
			}
			// TODO: use ssWorker.DecodeShares()
			newShares, err := shamir.Regenerate(secretSharedCommand, op.N)
			if err != nil {
				log.Fatalf("failed to reconstruct command (s=%d, ob=%s): %v",
					i, op.log[i].oriBallot, err,
				)
			}

			plainCmd, err := shamir.Combine(newShares)
			if err != nil {
				log.Fatalf("failed to get the clear command (s=%d, ob=%s): %v",
					i, op.log[i].oriBallot, err,
				)
			}

			// check if the recovered command is valid or not
			log.Debugf("recovered command: %x", plainCmd)
			if er := op.checkCommandBufferValidity(plainCmd); er != nil {
				log.Fatalf("the reconstructed value is not a valid command: %v", err)
			}

			allSharesBatch[j] = &newShares
			entryCmdBatch[j] = plainCmd
		}

		// prepare places for new proposal for all, but this, acceptors (N-1)
		proposalShares = make([][]SecretShare, op.N-1)

		// put the recovered commands into the consensus instance (entry)
		op.log[i].commands = entryCmdBatch
		op.log[i].commandsHandler = nil
		op.log[i].commit = false
		op.log[i].ssTime = nil
		op.log[i].commandShares = nil
		op.log[i].sharesBatch = nil

		// cast [][]byte to []SecretShare for this and other acceptors
		for j := 0; j < batchSize; j++ {
			// for this acceptor
			op.log[i].sharesBatch = append(op.log[i].sharesBatch, (*allSharesBatch[j])[0])

			// for other (N-1) acceptors
			for k := 0; k < op.N-1; k++ {
				// we use allSharesBatch[j][k+1] since k=0 is already used for this acceptor
				proposalShares[k] = append(proposalShares[k], (*allSharesBatch[j])[k+1])
			}
		}

		// Start proposing the recovered secret value.
		// Update the accepted ballot to this proposer's ballot.
		// The commands and commandShare are already updated as well.
		op.log[i].ballot = op.ballot
		if op.log[i].quorum == nil {
			op.log[i].quorum = paxi.NewQuorum()
		}
		op.log[i].quorum.Reset()
		op.persistAcceptedShares(i, op.ballot, op.log[i].oriBallot, op.log[i].sharesBatch)
		op.log[i].quorum.ACK(op.ID())

		// preparing different proposal for each acceptor (N-1)
		proposeRequests := make([]interface{}, op.N-1)
		for j := 0; j < op.N-1; j++ {
			proposeRequests[j] = P2a{
				Ballot:      op.log[i].ballot,
				Slot:        i,
				SharesBatch: proposalShares[j],
				OriBallot:   op.log[i].oriBallot,
			}
		}

		op.MulticastUniqueMessage(proposeRequests)
	}
}

func (op *OPaxos) checkCommandBufferValidity(cmdBuff []byte) error {
	// handle nil command
	if len(cmdBuff) == 0 {
		return nil
	}
	cmdType, err := paxi.GetDBCommandTypeFromBufferVerbose(cmdBuff)
	log.Debugf("recovery - cmdType=%d cmd=%v", cmdType, cmdBuff)
	return err
}

// Propose initiates phase 2 of OPaxos
func (op *OPaxos) Propose(r *SecretSharedCommand) {
	// prepare places for proposal
	proposalShares := make([][]SecretShare, op.N-1)

	// prepare batch of commands to be proposed
	batchSize := len(op.onOffPendingCommands) + 1
	if batchSize > paxi.MaxBatchSize {
		batchSize = paxi.MaxBatchSize
	}
	commands := make([]paxi.BytesCommand, batchSize)
	commandsHandler := make([]*paxi.ClientCommand, batchSize)
	ssEncTimes := make([]time.Duration, batchSize)
	sharesBatch := make([]SecretShare, batchSize)

	// handle first command r in the batch
	commands[0] = r.RawCommand
	commandsHandler[0] = r.ClientCommand
	ssEncTimes[0] = r.SSTime
	if *paxi.IsReqTracefile == "" {
		sharesBatch[0] = r.Shares[0]
	}
	for i := 0; *paxi.IsReqTracefile == "" && i < op.N-1; i++ {
		proposalShares[i] = append(proposalShares[i], r.Shares[i+1])
	}

	// handle the remaining commands in the batch
	for i := 1; i < batchSize; i++ {
		cmd := <-op.onOffPendingCommands
		commands[i] = cmd.RawCommand
		commandsHandler[i] = cmd.ClientCommand
		ssEncTimes[i] = cmd.SSTime
		if *paxi.IsReqTracefile == "" {
			sharesBatch[i] = r.Shares[0]
		}
		for j := 0; *paxi.IsReqTracefile == "" && j < op.N-1; j++ {
			proposalShares[j] = append(proposalShares[j], cmd.Shares[j+1])
		}
	}
	log.Debugf("batching %d commands", batchSize)

	// primary-backup approach: execute than agreement
	if *paxi.IsReqTracefile != "" {
		// emulated in batch
		resultingStateDiffs := op.emulateExecution(commandsHandler)
		for i := 0; i < batchSize; i++ {
			// handle empty state diffs, for instance from a read command
			if len(resultingStateDiffs[i]) == 0 {
				sharesBatch[i] = nil
				var emptyShare SecretShare
				for j := 0; j < op.N-1; j++ {
					proposalShares[j] = append(proposalShares[j], emptyShare)
				}
				continue
			}

			// handle non-empty state diffs
			sharesBatch[i] = resultingStateDiffs[i][0]
			for j := 0; j < op.N-1; j++ {
				proposalShares[j] = append(proposalShares[j], resultingStateDiffs[i][j+1])
			}
		}
	}

	// prepare the entry that contains a batch of commands
	op.slot++
	op.log[op.slot] = &entry{
		ballot:          op.ballot,
		oriBallot:       op.ballot,
		sharesBatch:     sharesBatch,
		commit:          false,
		commands:        commands,
		commandsHandler: commandsHandler,
		quorum:          paxi.NewQuorum(),
		ssTime:          ssEncTimes,
	}

	log.Debugf("get batch of commands for slot %d", op.slot)

	op.persistAcceptedShares(op.slot, op.ballot, op.ballot, sharesBatch)
	op.log[op.slot].quorum.ACK(op.ID())

	// preparing different proposal for each acceptor
	proposeRequests := make([]interface{}, op.N-1)
	for i := 0; i < op.N-1; i++ {
		log.Infof("sending to acceptor %d with data length of %d", i, len(proposalShares[i][0]))
		proposeRequests[i] = P2a{
			Ballot:      op.ballot,
			Slot:        op.slot,
			SharesBatch: proposalShares[i],
			OriBallot:   op.ballot,
		}
	}

	// broadcast propose message to the acceptors
	op.MulticastUniqueMessage(proposeRequests)
}

// emulateExecution receives a batch of raw commands, execute them (in emulation),
// capture the state differences, and secret shares the state differences.
// emulateExecution returns the secret-shares of the state diffs.
// Check out paxos.emulateExecution() for the same emulation.
func (op OPaxos) emulateExecution(emulatedCommands []*paxi.ClientCommand) [][]SecretShare {
	ssResultingDiffs := make([][]SecretShare, len(emulatedCommands))
	for i, cmd := range emulatedCommands {
		if cmd .CommandType != paxi.TypeEmulatedCommand {
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

		eCmdData := op.emulatedCmdData[eCmd.CommandID]
		if eCmdData == nil {
			log.Errorf("unrecognized command with id %d", eCmd.CommandID)
			continue
		}

		// emulate execution - exec time
		// the emulated execution time already include disk fsync
		log.Infof("emulating command %s for %v", eCmdData.CommandType, eCmdData.ExecTime)
		time.Sleep(eCmdData.ExecTime)

		// get the state diffs
		stateDiff := eCmdData.GenerateStateDiffsData()

		// secret-shares the state diffs
		var ssStateDiff []SecretShare
		var errSS error
		if len(stateDiff) > 0 {
			ssStateDiff, _, errSS = op.defaultSSWorker.SecretShareCommand(stateDiff)
			if errSS != nil {
				log.Errorf("failed to secret-shares state diffs after executing command with id %d: %s", eCmd.CommandID, errSS.Error())
				continue
			}
		}

		ssResultingDiffs[i] = make([]SecretShare, len(ssStateDiff))
		copy(ssResultingDiffs[i], ssStateDiff)
	}
	return ssResultingDiffs
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

		// update entry
		op.persistAcceptedShares(m.Slot, m.Ballot, m.OriBallot, m.SharesBatch)
		if e, exists := op.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				e.sharesBatch = m.SharesBatch
				e.ballot = m.Ballot
				e.oriBallot = m.OriBallot
				e.commands = nil
				e.commandsHandler = nil
			}
		} else {
			op.log[m.Slot] = &entry{
				ballot:          m.Ballot,
				oriBallot:       m.OriBallot,
				commands:        nil,
				commandsHandler: nil,
				sharesBatch:     m.SharesBatch,
				commit:          false,
			}
		}
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

			// TODO: maybe swap the order of broadcast and exec (?) so we have lower execution latency

			// update execute slot idx
			op.exec()
		}
	}
}

// broadcastCommit sends clear command to fellow trusted nodes, but
// secret-shared command to untrusted nodes.
func (op *OPaxos) broadcastCommit(slot int, ballot paxi.Ballot) {
	log.Debugf("broadcasting commit, slot=%d b=%s", slot, ballot)

	// Clear commands (batch) is sent to fellow trusted nodes, so they
	// can also execute the command.
	for trustedNodeID := range op.trustedNodeIDs {
		if trustedNodeID == op.ID() {
			continue
		}
		op.Send(trustedNodeID, P3{
			Ballot:    ballot,
			Slot:      slot,
			Commands:  op.log[slot].commands,
			OriBallot: op.log[slot].oriBallot,
		})
	}
	for untrustedNodeID := range op.untrustedNodeIDs {
		op.Send(untrustedNodeID, P3{
			Ballot:      ballot,
			Slot:        slot,
			SharesBatch: nil,
		})
		// TODO: maybe send the OriBallot only (?)
		// For optimization, we don't resend the secret-share in the commit message in this prototype.
		// For production usage, the proposer might need to send commit message with secret-share to
		// acceptors whose P2b messages is not in the phase-2 quorum processed; or the acceptors
		// can contact back the proposer asking the committed secret-share.
	}
}

func (op *OPaxos) exec() {
	for {
		e, ok := op.log[op.execute]
		if !ok || !e.commit {
			break
		}

		// only the trusted node that can "execute" the clear command
		if op.isTrustedNode && len(e.commands) > 0 {
			log.Debugf("executing command in slot=%d", op.execute)
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

		// TODO clean up the log periodically
		delete(op.log, op.execute)
		op.execute++
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

	// primary-backup approach, the command is already executed before agreement
	// so here we directly return to client
	if *paxi.IsReqTracefile != "" {
		eCmd, err := paxi.UnmarshalEmulatedCommand(e.commandsHandler[cid].RawCommand)
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

	value := op.Execute(cmd)

	// reply with data for write operation
	if cmd.Value == nil {
		reply.Data = value
	}

	if *paxi.GatherSecretShareTime || *paxi.ClientIsStateful {
		reply.Metadata = make(map[byte]interface{})
	}

	if *paxi.GatherSecretShareTime {
		reply.Metadata[paxi.MetadataSecretSharingTime] = e.ssTime[cid]
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
		if m.SharesBatch != nil {
			e.sharesBatch = m.SharesBatch
		}
		if m.Commands != nil {
			e.commands = m.Commands
		}
		e.commit = true
		e.commandsHandler = nil
	} else {
		op.log[m.Slot] = &entry{
			ballot:      m.Ballot,
			oriBallot:   m.OriBallot,
			commands:    m.Commands,
			sharesBatch: m.SharesBatch,
			commit:      true,
		}
	}

	if len(m.Commands) > 0 {
		op.log[m.Slot].commands = m.Commands
	}

	op.exec()
}
