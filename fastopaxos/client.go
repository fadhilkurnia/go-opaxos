package fastopaxos

import (
	"errors"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
	"time"
)

// Client implements paxi.Client interface
type Client struct {
	paxi.Client

	clientID      paxi.ID
	ballot        Ballot
	lastCmdID     int
	nodeClients   map[paxi.ID]*paxi.TCPClient
	ssWorker      opaxos.SecretSharingWorker
	coordinatorID paxi.ID

	// fields for AsyncClient
	responseChan chan *paxi.CommandReply
}

func NewClient() *Client {
	config := paxi.GetConfig()
	fastOPaxosCfg := opaxos.InitConfig(&config)
	clientID := paxi.NewID(99, time.Now().Nanosecond())
	algorithm := fastOPaxosCfg.Protocol.SecretSharing
	numNodes := config.N()
	threshold := fastOPaxosCfg.Threshold

	client := &Client{
		clientID:    clientID,
		ballot:      NewBallot(0, false, clientID),
		lastCmdID:   0,
		nodeClients: make(map[paxi.ID]*paxi.TCPClient),
		ssWorker:    opaxos.NewWorker(algorithm, numNodes, int(threshold)),
	}

	// initialize connection to all the nodes
	for id, _ := range config.PublicAddrs {
		client.nodeClients[id] = paxi.NewTCPClient(id).Start()
	}

	// by default 1.1 is the coordinator
	client.coordinatorID = paxi.NewID(1, 1)

	return client
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

	// secret-shares the command
	cmdShares, _, err := c.ssWorker.SecretShareCommand(cmdBuff)
	if err != nil {
		log.Errorf("failed to secret-share the command: %s", err)
		return nil, err
	}

	// prepare DirectCommand for all the nodes
	directCmds := make(map[paxi.ID]*DirectCommand)
	sid := 0
	for id, _ := range c.nodeClients {
		dcmd := DirectCommand{
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

	return c.sendDirectCommand(cmd.Serialize())
}

func (c *Client) GetResponseChannel() chan *paxi.CommandReply {
	return c.responseChan
}

func (c *Client) sendDirectCommand(cmdBuff []byte) error {
	c.ballot.Next(c.clientID)

	// secret-shares the command
	cmdShares, _, err := c.ssWorker.SecretShareCommand(cmdBuff)
	if err != nil {
		log.Errorf("failed to secret-share the command: %s", err)
		return err
	}

	// prepare DirectCommand for all the nodes
	directCmds := make(map[paxi.ID]*DirectCommand)
	sid := 0
	for id, _ := range c.nodeClients {
		dcmd := DirectCommand{
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
	if c.responseChan == nil {
		c.responseChan = c.nodeClients[c.coordinatorID].GetResponseChannel()
	}
	for id, _ := range c.nodeClients {
		err = c.nodeClients[id].SendCommand(directCmds[id])
		if err != nil {
			log.Errorf("failed to send DirectCommand to %s: %s", id, err)
		}
	}

	return nil
}

// ClientCreator TODO: implement this for benchmark purposes!
type ClientCreator struct {
	paxi.BenchmarkClientCreator
}

func (f ClientCreator) Create() (paxi.Client, error) {
	panic("unimplemented")
}

func (f ClientCreator) CreateAsyncClient() (paxi.AsyncClient, error) {
	newClient := NewClient()
	newClient.responseChan = newClient.nodeClients[newClient.coordinatorID].GetResponseChannel()
	return newClient, nil
}

func (f ClientCreator) CreateCallbackClient() (paxi.AsyncCallbackClient, error) {
	panic("unimplemented")
}
