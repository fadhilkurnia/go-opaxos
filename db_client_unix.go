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

// UnixClient implements Client, AdminClient, AsyncClient, and AsyncCallbackClient
// interfaces with Unix Domain Socket (UDS) connection, thus the node server and client
// need to be in the same machine.
type UnixClient struct {
	Client
	AsyncClient
	AsyncCallbackClient
	AdminClient

	hostID     ID
	connection net.Conn
	buffWriter *bufio.Writer
	buffReader *bufio.Reader

	// used for blocking client behaviour
	curCmdID uint32

	// used for non-blocking client behaviour
	isAsync    bool
	responseCh chan *CommandReply
}

func NewUnixClient(id ID) *UnixClient {
	var err error
	c := new(UnixClient)

	serverPort := GetConfig().GetPublicHostPort(id)
	if serverPort == "" {
		log.Fatalf("unknown public port for client-node communication")
	}

	c.hostID = id
	socketAddress := fmt.Sprintf("/tmp/rpc_%s.sock", serverPort)
	log.Debugf("connecting to the host with udp socket file: %s", socketAddress)
	c.connection, err = net.Dial("unix", socketAddress)
	if err != nil {
		log.Errorf("failed to connect to the server: %v", err)
		return c
	}
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.responseCh = make(chan *CommandReply, GetConfig().ChanBufferSize)

	return c
}

// ==============================================================================================
// ======== Starting the UnixClient's implementation for (blocking) Client interface ============
// ==============================================================================================

// Get implements the method required in the Client interface
func (c *UnixClient) Get(key Key) (Value, error) {
	c.curCmdID++
	cmd := &DBCommandGet{
		CommandID: c.curCmdID,
		SentAt:    time.Now().UnixNano(),
		Key:       key,
	}

	resp, err := c.do(cmd)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

// Put implements the method required in the Client interface
func (c *UnixClient) Put(key Key, val Value) error {
	c.curCmdID++
	cmd := &DBCommandPut{
		CommandID: c.curCmdID,
		SentAt:    time.Now().UnixNano(),
		Key:       key,
		Value:     val,
	}

	_, err := c.do(cmd)
	if err != nil {
		return err
	}

	return nil
}

// Put2 implements the method required in the Client interface
func (c *UnixClient) Put2(key Key, val Value) (interface{}, error) {
	panic("unimplemented")
}

// do is a blocking interface, the client send a command request
// and wait until it receives the first response
// WARNING: this assumes the response are FIFO (ordered)
func (c *UnixClient) do(cmd SerializableCommand) (*CommandReply, error) {
	if c.isAsync {
		log.Fatal("Using blocking method in a non-blocking client!")
	}

	cmdBytes := cmd.Serialize()
	buff := make([]byte, 5)
	buff[0] = cmd.GetCommandType()
	cmdLen := uint32(len(cmdBytes))
	binary.BigEndian.PutUint32(buff[1:], cmdLen)
	buff = append(buff, cmdBytes...)

	// send request
	log.Debugf("sending command type=%d len=%d", buff[0], cmdLen)
	_, err := c.buffWriter.Write(buff)
	if err != nil {
		return nil, err
	}
	c.buffWriter.Buffered()
	err = c.buffWriter.Flush()

	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte

	// wait for response
	for {
		var msgBuff []byte
		var resp *CommandReply

		firstByte, err = c.buffReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Fatal("server is closing the connection.")
				break
			}
			log.Fatalf("fail to read byte from server, terminating the connection. %s", err.Error())
			break
		}

		if firstByte == TypeCommandReply {
			_, err = io.ReadAtLeast(c.buffReader, respLenByte[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}

			respLen = binary.BigEndian.Uint32(respLenByte[:])
			msgBuff = make([]byte, respLen)
			_, err = io.ReadAtLeast(c.buffReader, msgBuff, int(respLen))
			if err != nil {
				log.Errorf("fail to read response data %v", err)
				break
			}

			resp, err = DeserializeCommandReply(msgBuff[:respLen])
			if err != nil {
				log.Errorf("fail to deserialize CommandReply %v, %x", err, msgBuff)
				break
			}

			return resp, err
		} else {
			log.Errorf("unknown command reply type: %d", firstByte)
		}

		break
	}

	return nil, err
}

// ==============================================================================================
// ========== End of the UnixClient's implementation for (blocking) Client interface ============
// ==============================================================================================

// Start starts listening response from server
// to use this: NewUnixClient(id).Start()
func (c *UnixClient) Start() *UnixClient {
	go c.putResponseToChannel()
	return c
}

func (c *UnixClient) putResponseToChannel() {
	defer c.connection.Close()

	var err error = nil
	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte

	//	get response from wire, parse, put to channel
	for err == nil {
		var msgBuff []byte
		var resp *CommandReply

		firstByte, err = c.buffReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Fatal("server is closing the connection.")
				break
			}
			log.Fatalf("fail to read byte from server, terminating the connection. %s", err.Error())
			break
		}

		if firstByte == TypeCommandReply {
			_, err = io.ReadAtLeast(c.buffReader, respLenByte[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}

			respLen = binary.BigEndian.Uint32(respLenByte[:])
			msgBuff = make([]byte, respLen)
			_, err = io.ReadAtLeast(c.buffReader, msgBuff, int(respLen))
			if err != nil {
				log.Errorf("fail to read response data %v", err)
				break
			}

			resp, err = DeserializeCommandReply(msgBuff[:respLen])
			if err != nil {
				log.Errorf("fail to deserialize CommandReply %v, %x", err, msgBuff)
				break
			}

			c.responseCh <- resp
		}
	}
}

// ==============================================================================================
// ========== Starting the UnixClient's implementation for AsyncClient interface ================
// ==============================================================================================

// SendCommand implements the method required in the AsyncClient interface
func (c *UnixClient) SendCommand(cmd SerializableCommand) error {
	cmdBytes := cmd.Serialize()

	buff := make([]byte, 5)
	buff[0] = cmd.GetCommandType()
	binary.BigEndian.PutUint32(buff[1:], uint32(len(cmdBytes)))

	buff = append(buff, cmdBytes...)

	_, err := c.buffWriter.Write(buff)
	if err != nil {
		return err
	}

	c.buffWriter.Buffered()

	return c.buffWriter.Flush()
}

// GetResponseChannel implements the method required in the AsyncClient interface
func (c *UnixClient) GetResponseChannel() chan *CommandReply {
	return c.responseCh
}

// ==============================================================================================
// ========== End of the UnixClient's implementation for AsyncClient interface ==================
// ==============================================================================================

// ==============================================================================================
// ====== Starting the UnixClient's implementation for AsyncCallbackClient interface ============
// ==============================================================================================
// TODO: complete the implementations
// ==============================================================================================
// ======= End of the UnixClient's implementation for AsyncCallbackClient interface =============
// ==============================================================================================

// ==============================================================================================
// ====== Starting the UnixClient's implementation for AdminClient interface ====================
// ==============================================================================================

func (c *UnixClient) Consensus(key Key) bool {
	panic("unimplemented")
}

func (c *UnixClient) Crash(target ID, duration int) {
	if c.hostID != target {
		log.Errorf("invalid hostID, try to use new client instead")
		return
	}

	cmd := &AdminCommandCrash{
		Duration: uint32(duration),
	}

	if !c.isAsync {
		_, err := c.do(cmd)
		if err != nil {
			log.Errorf("failed to send crash command: %v", err)
		}
		return
	}

	err := c.SendCommand(cmd)
	if err != nil {
		log.Errorf("failed to send crash command: %v", err)
	}
}

func (c *UnixClient) Drop(from ID, to ID, duration int) {
	panic("unimplemented")
}

func (c *UnixClient) Partition(int, ...ID) {
	panic("unimplemented")
}

// ==============================================================================================
// ======= End of the UnixClient's implementation for AdminClient interface =====================
// ==============================================================================================

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

func (r *RPCClientFactory) Create() (BenchmarkClient, error) {
	if r.isAsync {
		return NewAsyncRPCClient(r.serverID)
	}
	return NewRPClient(r.serverID)
}

type RPCClient struct {
	BenchmarkClient

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

	ret.connection, err = net.Dial("tcp", GetConfig().GetPublicHostAddress(serverID))
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
			log.Fatalf("fail to read byte from server, terminating the connection. %v", err)
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

	ret.connection, err = net.Dial("tcp", GetConfig().GetPublicHostAddress(serverID))
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
	reply, err := UnmarshalCommandReply(resp)
	if err != nil {
		return nil, err
	}
	return reply.Data, nil
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

// ===== start interface implementation for BenchmarkClient interface ===============

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
	reply, _ := UnmarshalCommandReply(resp)
	callback(reply)
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

	cr, _ := UnmarshalCommandReply(resp.Data)
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

// ========================================================================================================================================

type NonBlockingDBClient interface {
	Init(id ID) (NonBlockingDBClient, error)
	SendCommand(interface{}) error
	GetReceiverChannel() chan *CommandReply
}

type DefaultDBClientFactory struct {
	serverID ID
}

func (DefaultDBClientFactory) Init() *DefaultDBClientFactory {
	return &DefaultDBClientFactory{}
}

func (r *DefaultDBClientFactory) WithServerID(id ID) *DefaultDBClientFactory {
	r.serverID = id
	return r
}

func (r *DefaultDBClientFactory) Create() (NonBlockingDBClient, error) {
	return NewDefaultDBClient(r.serverID)
}

// DefaultDBClient implements NonBlockingDBClient interface (client_type = "generic")
// it does not store per-request metadata in the client side, put the metadata
// in the message for analytical purposes.
// pros: no state management in client
// cons: message become bigger with additional metadata (sentAt, encodeTime, etc.)
// request message: paxi.GenericCommand
// response message: paxi.CommandReply
type DefaultDBClient struct {
	NonBlockingDBClient

	connection net.Conn
	buffWriter *bufio.Writer
	buffReader *bufio.Reader

	responseCh chan *CommandReply
}

func NewDefaultDBClient(serverID ID) (NonBlockingDBClient, error) {
	var err error
	c := new(DefaultDBClient)

	c.connection, err = net.Dial("tcp", GetConfig().GetPublicHostAddress(serverID))
	if err != nil {
		return nil, err
	}
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.responseCh = make(chan *CommandReply, GetConfig().ChanBufferSize)

	go c._putResponseToChannel()

	return c, nil
}

func (c *DefaultDBClient) _putResponseToChannel() {
	defer c.connection.Close()

	var err error = nil
	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte

	//	get response from wire, parse, put to channel
	for err == nil {
		var msgBuff []byte
		var resp *CommandReply

		firstByte, err = c.buffReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Fatal("server is closing the connection.")
				break
			}
			log.Fatalf("fail to read byte from server, terminating the connection. %s", err.Error())
			break
		}

		if firstByte == COMMAND {
			_, err = io.ReadAtLeast(c.buffReader, respLenByte[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}

			respLen = binary.BigEndian.Uint32(respLenByte[:])
			msgBuff = make([]byte, respLen)

			_, err = io.ReadAtLeast(c.buffReader, msgBuff, int(respLen))
			if err != nil {
				log.Errorf("fail to read response data %v", err)
				break
			}

			log.Debugf("len=%x(%d) data=%x", respLenByte, respLen, msgBuff)

			resp, err = UnmarshalCommandReply(msgBuff[:respLen])
			if err != nil {
				log.Errorf("fail to unmarshal CommandReply %v, %x", err, msgBuff)
				break
			}

			if len(c.responseCh) >= GetConfig().ChanBufferSize {
				log.Warningf("receiver channel is full (len=%d)", len(c.responseCh))
			}

			c.responseCh <- resp
		}
	}
}

// SendCommand sends paxi.GenericCommand to rpc server
// request message: GenericCommand
// response message: CommandResponse
// check paxi.node.handleGenericCommand for the receiver implementation
func (c *DefaultDBClient) SendCommand(req interface{}) error {
	cmd := req.(GenericCommand)
	cmdBytes := cmd.Marshal()

	buff := make([]byte, 5)
	buff[0] = COMMAND
	binary.BigEndian.PutUint32(buff[1:], uint32(len(cmdBytes)))

	buff = append(buff, cmdBytes...)

	_, err := c.buffWriter.Write(buff)
	if err != nil {
		return err
	}

	return c.buffWriter.Flush()
}

func (c *DefaultDBClient) GetReceiverChannel() chan *CommandReply {
	return c.responseCh
}

// ========================================================================================================================================

type UDSDBClientFactory struct {
	serverID ID
}

func (UDSDBClientFactory) Init() *UDSDBClientFactory {
	return &UDSDBClientFactory{}
}

func (r *UDSDBClientFactory) WithServerID(id ID) *UDSDBClientFactory {
	r.serverID = id
	return r
}

func (r *UDSDBClientFactory) Create() (NonBlockingDBClient, error) {
	return NewUDSDBClient(r.serverID)
}

// UDSDBClient implements NonBlockingDBClient interface (client_type = "generic")
// it does not store per-request metadata in the client side, put the metadata
// in the message for analytical purposes.
// pros: no state management in client
// cons: message become bigger with additional metadata (sentAt, encodeTime, etc.)
// request message: paxi.GenericCommand
// response message: paxi.CommandReply
type UDSDBClient struct {
	NonBlockingDBClient

	connection net.Conn
	buffWriter *bufio.Writer
	buffReader *bufio.Reader

	responseCh chan *CommandReply
}

func NewUDSDBClient(serverID ID) (NonBlockingDBClient, error) {
	var err error
	c := new(UDSDBClient)

	socketAddress := fmt.Sprintf("/tmp/rpc_%s.sock", GetConfig().GetPublicHostPort(serverID))

	c.connection, err = net.Dial("unix", socketAddress)
	if err != nil {
		return nil, err
	}
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.responseCh = make(chan *CommandReply, GetConfig().ChanBufferSize)

	go c._putResponseToChannel()

	return c, nil
}

func (c *UDSDBClient) _putResponseToChannel() {
	defer c.connection.Close()

	var err error = nil
	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte

	//	get response from wire, parse, put to channel
	for err == nil {
		var msgBuff []byte
		var resp *CommandReply

		firstByte, err = c.buffReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Fatal("server is closing the connection.")
				break
			}
			log.Fatalf("fail to read byte from server, terminating the connection. %s", err.Error())
			break
		}

		if firstByte == COMMAND {
			_, err = io.ReadAtLeast(c.buffReader, respLenByte[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}

			respLen = binary.BigEndian.Uint32(respLenByte[:])
			msgBuff = make([]byte, respLen)
			_, err = io.ReadAtLeast(c.buffReader, msgBuff, int(respLen))
			if err != nil {
				log.Errorf("fail to read response data %v", err)
				break
			}

			resp, err = UnmarshalCommandReply(msgBuff[:respLen])
			if err != nil {
				log.Errorf("fail to unmarshal CommandReply %v, %x", err, msgBuff)
				break
			}

			c.responseCh <- resp
		}
	}
}

// SendCommand sends paxi.GenericCommand to rpc server
// request message: GenericCommand
// response message: CommandReply
// check paxi.node.handleGenericCommand for the receiver implementation
func (c *UDSDBClient) SendCommand(req interface{}) error {
	cmd := req.(GenericCommand)
	cmdBytes := cmd.Marshal()

	buff := make([]byte, 5)
	buff[0] = COMMAND
	binary.BigEndian.PutUint32(buff[1:], uint32(len(cmdBytes)))

	buff = append(buff, cmdBytes...)

	_, err := c.buffWriter.Write(buff)
	if err != nil {
		return err
	}

	c.buffWriter.Buffered()

	return c.buffWriter.Flush()
}

func (c *UDSDBClient) GetReceiverChannel() chan *CommandReply {
	return c.responseCh
}