package fastopaxos

import (
	"errors"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
	"github.com/vmihailenco/msgpack/v5"
	"runtime"
	"sync/atomic"
	"time"
)

// Client implements paxi.Client interface
type Client struct {
	paxi.Client

	clientID    paxi.ID
	ballot      Ballot
	lastCmdID   int
	nodeClients map[paxi.ID]*paxi.TCPClient
	targetSlot  int

	// used to share the next slot among multiple clients
	useSharedTargetSlot bool
	sharedTargetSlot    *int64

	ssAlgorithm   string
	threshold     int
	numWorker     int
	ssJobs        chan *RawDirectCommandBallot
	broadcasts    chan *BroadcastDirectCommand
	coordinatorID paxi.ID
	nodeIDs       []paxi.ID

	defSSWorker ClientSSWorker

	// fields for AsyncClient
	responseChan chan *paxi.CommandReply
}

func NewClient() *Client {
	config := paxi.GetConfig()
	fastOPaxosCfg := opaxos.InitConfig(&config)
	clientID := paxi.NewID(99, time.Now().Nanosecond())
	algorithm := fastOPaxosCfg.Protocol.SecretSharing
	numNodes := config.N()
	threshold := fastOPaxosCfg.Protocol.Threshold

	// by default 1.1 is the coordinator
	coordID := paxi.NewID(1, 1)
	nodeIDs := make([]paxi.ID, 0)

	for id, _ := range config.PublicAddrs {
		nodeIDs = append(nodeIDs, id)
	}

	if len(nodeIDs) != numNodes {
		log.Errorf("there must be %d node address", numNodes)
	}

	client := &Client{
		clientID:      clientID,
		ballot:        NewBallot(0, false, clientID),
		targetSlot:    -1,
		lastCmdID:     0,
		nodeClients:   make(map[paxi.ID]*paxi.TCPClient),
		ssAlgorithm:   algorithm,
		threshold:     threshold,
		numWorker:     runtime.NumCPU(),
		defSSWorker:   NewWorker(algorithm, threshold, coordID, nodeIDs),
		nodeIDs:       nodeIDs,
		coordinatorID: coordID,
		ssJobs:        make(chan *RawDirectCommandBallot, config.ChanBufferSize),
		broadcasts:    make(chan *BroadcastDirectCommand, config.ChanBufferSize),
	}

	// initialize connection to all the nodes
	for id, _ := range config.PublicAddrs {
		client.nodeClients[id] = paxi.NewTCPClient(id).Start()
	}

	// start and run the secret-sharing workers
	client.initRunSSWorkers()
	go client.consumeSendSecretSharedCommand()

	client.getConsensusMetadata()

	return client
}

func (c *Client) initRunSSWorkers() {
	for i := 0; i < c.numWorker; i++ {
		w := NewWorker(c.ssAlgorithm, c.threshold, c.coordinatorID, c.nodeIDs)
		go w.StartProcessingInput(c.ssJobs, c.broadcasts)
	}
}

func (c *Client) consumeSendSecretSharedCommand() {
	for bcast := range c.broadcasts {
		for dcid, nid := range c.nodeIDs {
			directCmd := bcast.DirectCommands[dcid]
			err := c.nodeClients[nid].SendCommand(directCmd)
			if err != nil {
				log.Errorf("failed to send DirectCommand to %s: %s", nid, err)
			}
			dcid++
		}
	}
}

// getConsensusMetadata gets the next empty slot from the coordinator
func (c *Client) getConsensusMetadata() {
	err := c.nodeClients[c.coordinatorID].SendCommand(GetMetadataRequest{})
	if err != nil {
		log.Fatalf("failed to initialize client: %s", err)
	}
	respChannel := c.nodeClients[c.coordinatorID].GetResponseChannel()
	respRaw := <-respChannel
	if respRaw.Code != paxi.CommandReplyOK {
		log.Fatalf("failed to initialize client: %s", errors.New("error response from coordinator"))
	}

	metadataRaw := respRaw.Data
	metadata := GetMetadataResponse{}
	err = msgpack.Unmarshal(metadataRaw, &metadata)
	if err != nil {
		log.Fatalf("failed to initialize client: %s", err)
	}
	c.targetSlot = metadata.NextSlot

	log.Debugf("starting slot from %d", c.targetSlot+1)
	log.Infof("leader condition: s=%d exc=%d, entry=%v", metadata.NextSlot, metadata.NextSlot, metadata.LastEntry)
}

// Get implements paxi.Client interface
func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.lastCmdID++
	cmd := paxi.DBCommandGet{
		CommandID: uint32(c.lastCmdID),
		SentAt:    time.Now().UnixNano(),
		Key:       key,
	}
	cmdBuff := cmd.Serialize()
	ret, err := c.doDirectCommand(cmdBuff)
	if err != nil {
		return nil, err
	}
	return ret.Data, nil
}

// Put implements paxi.Client interface
func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.lastCmdID++
	cmd := paxi.DBCommandPut{
		CommandID: uint32(c.lastCmdID),
		SentAt:    time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}
	cmdBuff := cmd.Serialize()
	_, err := c.doDirectCommand(cmdBuff)
	return err
}

// Put2 implements paxi.Client interface
func (c *Client) Put2(key paxi.Key, value paxi.Value) (interface{}, error) {
	c.lastCmdID++
	cmd := paxi.DBCommandPut{
		CommandID: uint32(c.lastCmdID),
		SentAt:    time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}
	cmdBuff := cmd.Serialize()
	ret, err := c.doDirectCommand(cmdBuff)
	return ret, err
}

func (c *Client) doDirectCommand(cmdBuff []byte) (*paxi.CommandReply, error) {
	c.ballot.Next(c.clientID)

	if c.useSharedTargetSlot {
		c.targetSlot = int(atomic.AddInt64(c.sharedTargetSlot, 1))
	} else {
		c.targetSlot++
	}

	// secret-shares the command
	cmdShares, _, err := c.defSSWorker.SecretShareCommand(cmdBuff)
	if err != nil {
		log.Errorf("failed to secret-share the command: %s", err)
		return nil, err
	}

	// prepare DirectCommand for all the nodes
	directCmds := make(map[paxi.ID]*DirectCommand)
	sid := 0
	for id, _ := range c.nodeClients {
		dcmd := DirectCommand{
			Slot:      c.targetSlot,
			OriBallot: c.ballot,
			Share:     SecretShare(cmdShares[sid]),
			Command:   nil,
		}
		if c.coordinatorID == id {
			dcmd.Command = cmdBuff
		}
		directCmds[id] = &dcmd
		sid++
	}

	// send command directly to all the nodes
	sid = 0
	coordinatorResponseStream := c.nodeClients[c.coordinatorID].GetResponseChannel()
	for id, _ := range c.nodeClients {
		err = c.nodeClients[id].SendCommand(directCmds[id])
		if err != nil {
			log.Errorf("failed to send DirectCommand to %s: %s", id, err)
		}
	}
	timeoutChan := make(chan bool)
	go func() {
		time.Sleep(3 * time.Second)
		timeoutChan <- true
	}()

	// wait response from the coordinator, or timeout
	var ret *paxi.CommandReply
	select {
	case resp := <-coordinatorResponseStream:
		if resp.Code != paxi.CommandReplyOK {
			ret = resp
			err = errors.New(string(resp.Data))
			break
		}

		ret = resp
		err = nil

	case _ = <-timeoutChan:
		ret = nil
		err = errors.New("timeout")

	}

	return ret, err
}

func (c *Client) SendCommand(cmd paxi.SerializableCommand) error {
	cmdType := cmd.GetCommandType()
	switch cmdType {
	case paxi.TypeDBPutCommand:
	case paxi.TypeDBGetCommand:
	default:
		return errors.New("unknown command type")
	}

	return c.sendDirectCommand(cmd)
}

func (c *Client) GetResponseChannel() chan *paxi.CommandReply {
	return c.nodeClients[c.coordinatorID].GetResponseChannel()
}

func (c *Client) sendDirectCommand(cmd paxi.SerializableCommand) error {
	c.ballot.Next(c.clientID)

	if c.useSharedTargetSlot {
		c.targetSlot = int(atomic.AddInt64(c.sharedTargetSlot, 1))
	} else {
		c.targetSlot++
	}

	c.ssJobs <- &RawDirectCommandBallot{
		Slot:           c.targetSlot,
		OriginalBallot: c.ballot,
		Command:        cmd,
	}

	return nil
}

type ClientCreator struct {
	lastSlotNumber int64
	sequencerStarted bool
	//slotSequencer    chan int
	paxi.BenchmarkClientCreator
}

//func (f *ClientCreator) startSequencer(initialSlot int) {
//	f.sequencerStarted = true
//	f.slotSequencer = make(chan int, 100_000)
//	go func() {
//		currentSlot := initialSlot
//		for true {
//			f.slotSequencer <- currentSlot
//			currentSlot++
//		}
//	}()
//}

func (f *ClientCreator) Create() (paxi.Client, error) {
	panic("unimplemented")
}

func (f *ClientCreator) CreateAsyncClient() (paxi.AsyncClient, error) {
	newClient := NewClient()

	if !f.sequencerStarted {
		f.sequencerStarted = true
		atomic.StoreInt64(&f.lastSlotNumber, int64(newClient.targetSlot))
	}

	newClient.useSharedTargetSlot = false
	newClient.sharedTargetSlot = &f.lastSlotNumber

	return newClient, nil
}

func (f *ClientCreator) CreateCallbackClient() (paxi.AsyncCallbackClient, error) {
	panic("unimplemented")
}
