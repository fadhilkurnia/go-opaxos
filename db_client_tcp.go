package paxi

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ailidani/paxi/log"
	"io"
	"net"
	"time"
)

// TCPClient implements Client, AsyncClient, AsyncCallbackClient, and AdminClient
// interface with plain TCP connection
type TCPClient struct {
	Client
	AsyncClient
	AsyncCallbackClient
	AdminClient

	hostID     ID
	connection net.Conn
	buffReader *bufio.Reader
	buffWriter *bufio.Writer
	sendChan   chan SerializableCommand // sendChan is the input for writer to the buffWriter

	delay uint64 // (in ns) currently only work for async client interface

	// used for blocking client behaviour
	curCmdID uint32

	// used for non-blocking client behaviour
	isAsync    bool
	responseCh chan *CommandReply
}

// NewTCPClient creates a new client for node with id=id
func NewTCPClient(id ID) *TCPClient {
	var err error
	c := new(TCPClient)

	nodeAddress := GetConfig().GetPublicHostAddress(id)
	if nodeAddress == "" {
		errStr := fmt.Sprintf("unknown %s node address for client-node communication", id)
		log.Fatal(errStr)
		return nil
	}

	c.connection, err = net.Dial("tcp", nodeAddress)
	if err != nil {
		log.Error(err)
		return c
	}

	c.sendChan = make(chan SerializableCommand)
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.isAsync = false
	c.responseCh = make(chan *CommandReply, GetConfig().ChanBufferSize)

	// set the delay
	delayMap, ok := GetConfig().Delays["clients"]
	if ok {
		delay, ok2 := delayMap[string(id)]
		if ok2 {
			// convert ms to ns
			c.delay = uint64(delay * float64(1_000_000))
		}
	}

	// start the sender to the server
	go func() {
		for cmd := range c.sendChan {
			cmdBytes := cmd.Serialize()
			buff := make([]byte, 5)
			buff[0] = cmd.GetCommandType()
			cmdLen := uint32(len(cmdBytes))
			binary.BigEndian.PutUint32(buff[1:], cmdLen)
			buff = append(buff, cmdBytes...)

			// send request
			log.Debugf("sending command type=%d len=%d", buff[0], cmdLen)
			nn, werr := c.buffWriter.Write(buff)
			if werr != nil {
				log.Error(werr)
			}
			if nn != len(buff) {
				log.Errorf("short write: %d, expected %d", nn, len(buff))
			}
			err = c.buffWriter.Flush()
		}
	}()

	return c
}

// Start starts listening response from server
// to use this: NewTCPClient(id).Start()
func (c *TCPClient) Start() *TCPClient {
	go c.putResponseToChannel()
	return c
}

func (c *TCPClient) putResponseToChannel() {
	defer func() {
		if c.connection != nil {
			err := c.connection.Close()
			if err != nil {
				log.Error(err)
			}
		}
	}()

	var err error = nil
	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte
	var nn int

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
			nn, err = io.ReadAtLeast(c.buffReader, respLenByte[:], 4)
			if err != nil {
				log.Errorf("fail to read command length %v", err)
				break
			}
			if nn != 4 {
				log.Errorf("short read, expected 4 but only %d", nn)
				break
			}

			respLen = binary.BigEndian.Uint32(respLenByte[:])
			msgBuff = make([]byte, respLen)
			nn, err = io.ReadAtLeast(c.buffReader, msgBuff, int(respLen))
			if err != nil {
				log.Errorf("fail to read response data %v", err)
				break
			}
			if nn != int(respLen) {
				log.Errorf("short read, expected %d but only %d", respLen, nn)
				break
			}

			resp, err = DeserializeCommandReply(msgBuff[:respLen])
			if err != nil {
				log.Errorf("fail to deserialize CommandReply %v, %x", err, msgBuff)
				break
			}

			c.responseCh <- resp
		} else {
			log.Errorf("unknown first byte sent by the server: %d", firstByte)
		}
	}

	log.Fatalf("exiting client's response consumer loop: %v", err)
}

// ==============================================================================================
// ======== Starting the TCPClient's implementation for (blocking) Client interface ============
// ==============================================================================================

// Get implements the method required in the Client interface
func (c *TCPClient) Get(key Key) (Value, error) {
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
func (c *TCPClient) Put(key Key, val Value) error {
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
func (c *TCPClient) Put2(key Key, val Value) (interface{}, error) {
	panic("unimplemented")
}

// do is a blocking interface, the client send a command request
// and wait until it receives the first response
// WARNING: this assumes the response are FIFO (ordered)
func (c *TCPClient) do(cmd SerializableCommand) (*CommandReply, error) {
	if c.isAsync {
		log.Fatal("Using blocking method in a non-blocking client!")
	}

	// send request to the server
	c.sendChan <- cmd

	var firstByte byte
	var respLen uint32
	var respLenByte [4]byte
	var err error

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
// ========== End of the TCPClient's implementation for (blocking) Client interface ============
// ==============================================================================================

// ==============================================================================================
// ========== Starting the TCPClient's implementation for AsyncClient interface =================
// ==============================================================================================

// SendCommand implements the method required in the AsyncClient interface
func (c *TCPClient) SendCommand(cmd SerializableCommand) error {
	if c.delay > 0 {
		return c.deferSendCommand(cmd, c.delay)
	}

	c.sendChan <- cmd
	return nil
}

func (c *TCPClient) deferSendCommand(cmd SerializableCommand, delay uint64) error {
	go func() {
		time.Sleep(time.Duration(delay))
		c.sendChan <- cmd
	}()
	return nil
}

// GetResponseChannel implements the method required in the AsyncClient interface
func (c *TCPClient) GetResponseChannel() chan *CommandReply {
	return c.responseCh
}

// ==============================================================================================
// ========== End of the TCPClient's implementation for AsyncClient interface ===================
// ==============================================================================================

// ==============================================================================================
// ====== Starting the TCPClient's implementation for AsyncCallbackClient interface ============
// ==============================================================================================
// TODO: complete the implementations
// ==============================================================================================
// ======= End of the TCPClient's implementation for AsyncCallbackClient interface =============
// ==============================================================================================

// ==============================================================================================
// ====== Starting the TCPClient's implementation for AdminClient interface ====================
// ==============================================================================================

func (c *TCPClient) Consensus(key Key) bool {
	panic("unimplemented")
}

func (c *TCPClient) Crash(target ID, duration int) {
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

func (c *TCPClient) Drop(from ID, to ID, duration int) {
	panic("unimplemented")
}

func (c *TCPClient) Partition(int, ...ID) {
	panic("unimplemented")
}

// ==============================================================================================
// ======= End of the UnixClient's implementation for AdminClient interface =====================
// ==============================================================================================
