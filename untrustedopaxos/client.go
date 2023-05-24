package untrustedopaxos

import (
	"errors"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
	"time"
)

type Client struct {
	paxi.Client

	config   paxi.Config
	opConfig opaxos.Config

	clientID         paxi.ID
	curBallot        ClientOriginalBallot
	ssWorker         opaxos.SecretSharingWorker
	nodeClients      map[paxi.ID]*paxi.TCPClient // nodeClients is a client instance for each node
	nodeResponseChan chan *paxi.CommandReply     // this channel subscribes to the responses from ALL the nodes
	responseChan     chan *paxi.CommandReply     // this channel is for paxi.AsyncClient user

	outstandingRequests map[ClientOriginalBallot]*requestMetadata // sending time of requests
}

type requestMetadata struct {
	startTime      int64
	numResponse    int
	isLeaderAck    bool
	reqType        byte
	responseShares []opaxos.SecretShare
}

func NewClient() *Client {
	log.Debugf("initializing new client for the untrusted mode")
	config := paxi.GetConfig()
	untrustedOPaxosConfig := opaxos.InitConfig(&config)
	clientID := paxi.NewID(99, time.Now().Nanosecond()%32767)

	log.Debugf("client id %s", clientID)
	log.Debugf("client initial ballot %s", NewBallot(0, clientID))

	c := &Client{
		clientID:            clientID,
		config:              config,
		opConfig:            untrustedOPaxosConfig,
		curBallot:           NewBallot(0, clientID),
		nodeClients:         make(map[paxi.ID]*paxi.TCPClient),
		outstandingRequests: make(map[ClientOriginalBallot]*requestMetadata),
		nodeResponseChan:    make(chan *paxi.CommandReply, config.ChanBufferSize),
		ssWorker: opaxos.NewWorker(
			untrustedOPaxosConfig.Protocol.SecretSharing,
			untrustedOPaxosConfig.N(),
			untrustedOPaxosConfig.Protocol.Threshold),
	}

	if len(config.PublicAddrs) != config.N() || len(config.PublicAddrs) == 0 {
		log.Fatalf("there must be %d>0 node address", config.N())
	}

	// initialize connection to all the untrusted nodes
	for nid, _ := range config.PublicAddrs {
		c.nodeClients[nid] = paxi.NewTCPClient(nid).Start()
		// redirect responses from all the nodes to a single destination channel
		go func(respChan chan *paxi.CommandReply, destChan chan *paxi.CommandReply) {
			for r := range respChan {
				destChan <- r
			}
		}(c.nodeClients[nid].GetResponseChannel(), c.nodeResponseChan)
	}

	// make a single node as the leader
	// by default 1.1 is the consensus coordinator
	leaderID := paxi.NewID(1, 1)
	err := c.nodeClients[leaderID].SendCommand(paxi.BeLeaderRequest{})
	if err != nil {
		log.Fatalf("fail to ask a node to be a leader: %s", err.Error())
	}
	time.Sleep(1 * time.Second) // wait till the leader got elected

	return c
}

func (c *Client) increaseBallot() {
	log.Debugf("prev ballot: %s", c.curBallot)
	c.curBallot.Next()
	log.Debugf("new ballot: %s", c.curBallot)
}

// Get implements paxi.Client interface for untrustedopaxos.Client
// this is a blocking interface
func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	cmd := paxi.DBCommandGet{
		CommandID: uint32(c.curBallot),
		Key:       key,
	}
	ret, err := c.doDirectCommand(&cmd)
	if err != nil {
		return nil, err
	}
	return ret.Data, nil
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	log.Debugf("executing put command %d %s", key, value)
	cmd := paxi.DBCommandPut{
		CommandID: uint32(c.curBallot),
		Key:       key,
		Value:     value,
	}
	_, err := c.doDirectCommand(&cmd)
	if err != nil {
		log.Errorf("failed to invoke command: %s", err.Error())
		return err
	}
	return nil
}

// warning doDirectCommand is not thread safe, this is used for a blocking client.
func (c *Client) doDirectCommand(cmd paxi.SerializableCommand) (*paxi.CommandReply, error) {
	if err := c.sendDirectCommand(cmd); err != nil {
		return nil, err
	}

	// wait for t response AND ack from the leader
	cmdID := c.curBallot
	rm := c.outstandingRequests[cmdID]
	for true {
		resp := <-c.nodeResponseChan
		log.Debugf("receiving a response for %s: %v", ClientOriginalBallot(resp.CommandID), resp)
		if resp.Code != paxi.CommandReplyOK {
			log.Errorf("receiving err response: %s", string(resp.Data))
			continue
		}
		if resp.CommandID != uint32(cmdID) {
			log.Debugf("ignoring response for different command %s", ClientOriginalBallot(resp.CommandID))
			continue
		}
		if len(resp.Data) == 0 {
			log.Debugf("receiving empty response")
		}

		rm.startTime = resp.SentAt
		rm.numResponse++
		rm.responseShares = append(rm.responseShares, opaxos.SecretShare(resp.Data))
		log.Debugf("receiving a response: %v", resp.Data)

		if resp.Metadata[paxi.MetadataLeaderAck] != nil {
			rm.isLeaderAck = true
		}

		if rm.numResponse >= c.opConfig.Protocol.Threshold && rm.isLeaderAck {
			log.Debugf("received responses: %v", rm.responseShares)
			break
		}

	}

	// reconstruct the response
	var decVal []byte
	var err error
	if cmd.GetCommandType() == paxi.TypeDBGetCommand {
		var filteredShares []opaxos.SecretShare
		for _, rs := range rm.responseShares {
			if len(rs) != 0 {
				filteredShares = append(filteredShares, rs)
			}
		}
		decVal, err = c.ssWorker.DecodeShares(filteredShares)
		if err != nil {
			log.Errorf("failed to decode the shares: %v", err.Error())
			return nil, err
		}
	}

	ret := &paxi.CommandReply{
		CommandID: uint32(cmdID),
		SentAt:    rm.startTime,
		Code:      paxi.CommandReplyOK,
		Data:      decVal,
		Metadata:  nil,
	}

	return ret, nil
}

// sendDirectCommand sends the command directly to all the nodes, without waiting for
// the response. The response later can be collected from responseChan.
// Note that ballot number increments can only be done here!
func (c *Client) sendDirectCommand(cmd paxi.SerializableCommand) error {
	if cmd.GetCommandType() != paxi.TypeDBGetCommand && cmd.GetCommandType() != paxi.TypeDBPutCommand {
		return errors.New("unrecognized command type for untrusted mode")
	}

	// increase the client ballot number
	// replace the CommandID given by the user
	c.increaseBallot()
	cmdID := uint32(c.curBallot)

	// secret-share the command, only for Put command
	var secretShares []opaxos.SecretShare
	if cmd.GetCommandType() == paxi.TypeDBPutCommand {
		log.Debugf("secret-sharing the command")
		x := cmd.(*paxi.DBCommandPut)
		ss, _, err := c.ssWorker.SecretShareCommand(x.Value)
		if err != nil {
			log.Errorf("failed to secret-shares the command: %s", err.Error())
			return err
		}
		secretShares = make([]opaxos.SecretShare, c.config.N())
		for i, s := range ss {
			secretShares[i] = make([]byte, len(s))
			copy(secretShares[i], s)
		}
	} else if cmd.GetCommandType() == paxi.TypeDBGetCommand {

	} else {
		return errors.New("unrecognized command type")
	}

	// store the request metadata
	rm := &requestMetadata{
		startTime:   0,
		numResponse: 0,
		isLeaderAck: false,
		reqType:     cmd.GetCommandType(),
	}
	c.outstandingRequests[c.curBallot] = rm

	// Broadcast command to all the nodes
	sendTime := time.Now()
	rm.startTime = sendTime.UnixNano()
	nid := 0
	var nonNilErr error
	for id, nc := range c.nodeClients {
		log.Debugf("directCommand: sending secret-shared command %s to %s", ClientOriginalBallot(cmdID), id)
		var err error
		if cmd.GetCommandType() == paxi.TypeDBGetCommand {
			x := cmd.(*paxi.DBCommandGet)
			err = nc.SendCommand(&paxi.DBCommandGet{
				CommandID: cmdID,
				SentAt:    sendTime.UnixNano(),
				Key:       x.Key,
			})
		} else if cmd.GetCommandType() == paxi.TypeDBPutCommand {
			x := cmd.(*paxi.DBCommandPut)
			log.Debugf("sending command to %s: %v", id, secretShares[nid])
			log.Debugf("replacing cmd-id=%d with %s", x.CommandID, ClientOriginalBallot(cmdID))
			err = nc.SendCommand(&paxi.DBCommandPut{
				CommandID: cmdID,
				SentAt:    sendTime.UnixNano(),
				Key:       x.Key,
				Value:     paxi.Value(secretShares[nid]),
			})
		}
		if err != nil {
			log.Errorf("failed to send command to %s: %s", id, err)
			nonNilErr = err
		}
		nid++
	}

	return nonNilErr
}

func (c *Client) Put2(key paxi.Key, value paxi.Value) (interface{}, error) {
	panic("unimplemented")
}

// SendCommand implements paxi.AsyncClient interface for opaxos.Client
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
	return c.responseChan
}

type ClientCreator struct {
	paxi.BenchmarkClientCreator
}

func (cc *ClientCreator) Create() {
	panic("unimplemented")
}

func (cc *ClientCreator) CreateAsyncClient() (paxi.AsyncClient, error) {
	c := NewClient()
	c.responseChan = make(chan *paxi.CommandReply, c.config.ChanBufferSize)

	// consume the response from all the nodes
	// TODO: make this thread safe
	go func(cli *Client) {
		for true {
			resp := <-cli.nodeResponseChan
			log.Debugf("receiving a response %v", resp)
			if resp.Code != paxi.CommandReplyOK {
				log.Errorf("receiving err response: %s", string(resp.Data))
				continue
			}
			if resp.CommandID != uint32(c.curBallot) {
				log.Debugf("ignoring response for different command %s", ClientOriginalBallot(resp.CommandID))
				continue
			}

			rm := cli.outstandingRequests[ClientOriginalBallot(resp.CommandID)]
			if rm == nil {
				log.Errorf("no data for outstanding request %s", ClientOriginalBallot(resp.CommandID))
				continue
			}

			rm.numResponse++
			rm.responseShares = append(rm.responseShares, opaxos.SecretShare(resp.Data))

			if resp.Metadata[paxi.MetadataLeaderAck] != nil {
				rm.isLeaderAck = true
			}

			if rm.numResponse >= c.opConfig.Protocol.Threshold && rm.isLeaderAck {
				log.Debugf("received responses: %v", rm.responseShares)
				decResp, err := cli.DecodeResponse(ClientOriginalBallot(resp.CommandID), rm)
				if err != nil {
					log.Errorf("failed to decode response cid=%s : %s",
						ClientOriginalBallot(resp.CommandID), err.Error())
				}
				c.responseChan <- decResp
			}
		}
	}(c)

	return c, nil
}

func (c *Client) DecodeResponse(cmdID ClientOriginalBallot, rm *requestMetadata) (*paxi.CommandReply, error) {
	// reconstruct the response
	var decVal []byte
	var err error
	if rm.reqType == paxi.TypeDBGetCommand {
		var filteredShares []opaxos.SecretShare
		for _, rs := range rm.responseShares {
			if len(rs) != 0 {
				filteredShares = append(filteredShares, rs)
			}
		}
		if len(filteredShares) != 0 {
			decVal, err = c.ssWorker.DecodeShares(filteredShares)
			if err != nil {
				log.Errorf("failed to decode the shares: %v", err.Error())
				return nil, err
			}
		}
	}

	ret := &paxi.CommandReply{
		CommandID: uint32(cmdID),
		SentAt:    rm.startTime,
		Code:      paxi.CommandReplyOK,
		Data:      decVal,
		Metadata:  nil,
	}

	return ret, nil
}

func (cc *ClientCreator) CreateCallbackClient() (paxi.AsyncCallbackClient, error) {
	panic("unimplemented")
}
