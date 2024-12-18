package paxi

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type RPCClientFactory struct {
	serverID ID
	isAsync  bool
}

func (RPCClientFactory) Init() *RPCClientFactory {
	return &RPCClientFactory{}
}

func (r *RPCClientFactory) WithServerID(id ID) *RPCClientFactory {
	r.serverID = id
	return r
}

func (r *RPCClientFactory) Async() *RPCClientFactory {
	r.isAsync = true
	return r
}

func (r *RPCClientFactory) Create() (DBClient, error) {
	if r.isAsync {
		return NewAsyncRPCClient(r.serverID)
	}
	return NewRPClient(r.serverID)
}

type RPCClient struct {
	DBClient

	connection net.Conn
	buffWriter *bufio.Writer
	buffReader *bufio.Reader
	reqID      uint32
	isAsync    bool

	outstandingRequests map[uint32]*RPCMessageMetadata
	mapLock             sync.RWMutex

	// inputChan and outputChan are used for asyncClient
	inputChan  chan []byte
	outputChan chan RPCMessage
	ring       *ring
}

func NewRPClient(serverID ID) (*RPCClient, error) {
	var err error
	ret := &RPCClient{}

	ret.connection, err = net.Dial("tcp", GetConfig().GetRPCHost(serverID))
	if err != nil {
		return nil, err
	}
	ret.buffWriter = bufio.NewWriter(ret.connection)
	ret.buffReader = bufio.NewReader(ret.connection)
	ret.reqID = 0
	ret.isAsync = false
	ret.outstandingRequests = make(map[uint32]*RPCMessageMetadata)
	ret.mapLock = sync.RWMutex{}

	if !ret.isAsync {
		go ret.gatherResponse()
	}

	return ret, nil
}

// gatherResponse read bytes from socket, reply to the related rpc caller
// used for blocking caller, so it can receive the response after blocking.
func (c *RPCClient) gatherResponse() {
	defer c.connection.Close()

	for {
		serverResponseFirstByte, err := c.buffReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Fatal("server is closing the connection.")
				break
			}
			log.Fatal("fail to read byte from server, terminating the connection. %v", err)
			break
		}

		if serverResponseFirstByte == COMMAND {
			msg, err := NewRPCMessage(c.buffReader)
			if err != nil {
				break
			}

			c.mapLock.RLock()
			meta, exist := c.outstandingRequests[msg.MessageID]
			c.mapLock.RUnlock()
			if exist {
				meta.procTime = time.Since(meta.startTime).Nanoseconds()

				// send response to the caller
				if meta.ch != nil {
					meta.ch <- msg.Data
					close(meta.ch)
				}

				c.mapLock.Lock()
				delete(c.outstandingRequests, msg.MessageID)
				c.mapLock.Unlock()
			}
		}
	}
}

// Do block the caller until it receives response, if the request require a response.
// Otherwise, it will return nil response.
func (c *RPCClient) Do(rpcType uint8, request []byte) ([]byte, error) {
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(request)))

	// handle rpc that does not require response
	if rpcType == COMMAND_NOREPLY || rpcType == CRASH || rpcType == DROP {
		if err := c.buffWriter.WriteByte(rpcType); err != nil {
			return nil, err
		}
		msg := &RPCMessage{
			MessageID:  0,
			MessageLen: 0,
		}
		if err := msg.Serialize(c.buffWriter); err != nil {
			return nil, err
		}

		return nil, nil
	}

	msg := RPCMessage{
		MessageID:  atomic.AddUint32(&c.reqID, 1),
		MessageLen: uint32(len(request)),
		Data:       request,
	}

	log.Debugf("sending message: %s", msg.String())

	meta := RPCMessageMetadata{
		startTime: time.Now(),
		ch:        make(chan []byte, 1),
	}

	c.mapLock.Lock()
	c.outstandingRequests[msg.MessageID] = &meta
	c.mapLock.Unlock()

	// send the command type, then the command itself
	if err := c.buffWriter.WriteByte(rpcType); err != nil {
		return nil, err
	}
	if err := msg.Serialize(c.buffWriter); err != nil {
		return nil, err
	}

	return <-meta.ch, nil
}

// NewAsyncRPCClient start an async RPC Client
func NewAsyncRPCClient(serverID ID) (*RPCClient, error) {
	var err error
	ret := &RPCClient{}

	ret.connection, err = net.Dial("tcp", GetConfig().GetRPCHost(serverID))
	if err != nil {
		return nil, err
	}
	ret.buffWriter = bufio.NewWriter(ret.connection)
	ret.buffReader = bufio.NewReader(ret.connection)
	ret.reqID = 0
	ret.isAsync = true
	ret.ring = newRing()

	if ret.isAsync {
		// keep writing to the connection, and keep reading from the connection
		go ret.spinWriter()
		go ret.spinReader()
	}

	return ret, nil
}

func (c *RPCClient) spinWriter() {
	if !c.isAsync {
		panic("this method is only for an async rpc client")
	}

	var err error = nil
	var data []byte
	var req RPCMessage
	var ok bool

	for err == nil {
		if req, ok = c.getNextMessageToSend(); ok {
			if data, err = req.ToBytes(); err != nil {
				break
			}
			_, err = c.buffWriter.Write(append([]byte{COMMAND}, data...))
		}
		if !ok {
			err = c.buffWriter.Flush()
		}
	}

	panic("writer loop exit!")
}

func (c *RPCClient) spinReader() {
	if !c.isAsync {
		panic("this method is only for an async rpc client")
	}

	var err error
	var firstByte byte

	for {
		firstByte, err = c.buffReader.ReadByte()
		if err != nil {
			break
		}

		if firstByte == COMMAND {
			msg, err := NewRPCMessage(c.buffReader)
			if err != nil {
				break
			}

			c.replyMessage(*msg)
		}
	}

	if err == io.EOF {
		log.Fatal("server is closing the connection")
		return
	}

	panic("reader loop exit! " + err.Error())
}

func (c *RPCClient) enqueueMessage(msg RPCMessage) chan RPCMessage {
	s := &c.ring.slots[atomic.AddUint64(&c.ring.write, 1)&mask]
	s.cond.L.Lock()
	for s.mark != 0 {
		s.cond.Wait()
	}
	s.req = msg
	s.mark = 1
	s.cond.L.Unlock()
	return s.respCh
}

func (c *RPCClient) getNextMessageToSend() (req RPCMessage, ok bool) {
	c.ring.read1++
	s := &c.ring.slots[(c.ring.read1)&mask]
	s.cond.L.Lock()
	if ok = s.mark == 1; ok {
		s.mark = 2
		req = s.req
	} else {
		c.ring.read1--
	}
	s.cond.L.Unlock()
	return
}

func (c *RPCClient) replyMessage(resp RPCMessage) {
	c.ring.read2++
	s := &c.ring.slots[(c.ring.read2)&mask]
	s.cond.L.Lock()
	if s.mark == 2 {
		s.mark = 0
		s.respCh <- resp
	} else {
		panic(fmt.Sprintf("out-of-band response should not be passed in: slot=%d mark=%d", c.ring.read2, s.mark))
	}
	s.cond.L.Unlock()
	s.cond.Signal()
	return
}

// ===== start interface implementation for paxi.Client interface ======

func (c *RPCClient) Get(key Key) (Value, error) {
	cmd := Command{
		Key:       key,
		Value:     nil,
		ClientID:  "",
		CommandID: 0,
	}
	resp, err := c.Do(COMMAND, cmd.ToBytesCommand())
	if err != nil {
		return nil, err
	}
	reply := UnmarshalCommandReply(resp)
	return reply.Value, nil
}

func (c *RPCClient) Put(key Key, value Value) error {
	cmd := Command{
		Key:       key,
		Value:     value,
		ClientID:  "",
		CommandID: 0,
	}
	_, err := c.Do(COMMAND, cmd.ToBytesCommand())
	if err != nil {
		return err
	}
	return nil
}

func (c *RPCClient) Put2(key Key, value Value) (interface{}, error) {
	cmd := Command{
		Key:       key,
		Value:     value,
		ClientID:  "",
		CommandID: 0,
	}
	responseRaw, err := c.Do(COMMAND, cmd.ToBytesCommand())
	if err != nil {
		return nil, err
	}
	response := &CommandReply{}
	err = msgpack.Unmarshal(responseRaw, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// ===== end interface implementation for Client interface =============

// ===== start interface implementation for AdminClient interface ======
// ===== end interface implementation for AdminClient interface ========

// ===== start interface implementation for DBClient interface ===============

func (c *RPCClient) Init() error {
	panic("unimplemented")
}

func (c *RPCClient) Stop() error {
	panic("unimplemented")
}

func (c *RPCClient) AsyncRead(key []byte, callback func(*CommandReply)) {
	keyInt := binary.BigEndian.Uint32(key)
	cmd := Command{
		Key:       Key(keyInt),
		Value:     nil,
		ClientID:  "",
		CommandID: 0,
	}
	resp, err := c.Do(COMMAND, cmd.ToBytesCommand())
	if err != nil {
		callback(&CommandReply{
			OK: false,
		})
		return
	}
	callback(UnmarshalCommandReply(resp))
}

func (c *RPCClient) AsyncWrite(key, value []byte, callback func(*CommandReply)) {
	cmd := GenericCommand{
		Operation: OP_WRITE,
		Key:       key,
		Value:     value,
	}
	cmdBytes := cmd.ToBytesCommand()
	msg := RPCMessage{
		MessageID:  atomic.AddUint32(&c.reqID, 1),
		MessageLen: uint32(len(cmdBytes)),
		Data:       cmdBytes,
	}

	ch := c.enqueueMessage(msg)
	resp := <-ch

	cr := UnmarshalCommandReply(resp.Data)
	callback(cr)
}

func (c *RPCClient) Read(key []byte) (interface{}, interface{}, error) {
	panic("unimplemented")
}

func (c *RPCClient) Write(key, value []byte) (interface{}, error) {
	cmd := GenericCommand{
		CommandID: 0,
		Operation: OP_WRITE,
		Key:       key,
		Value:     value,
	}
	resp, err := c.Do(COMMAND, cmd.ToBytesCommand())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// ===== end interface implementation for DB interface =================
