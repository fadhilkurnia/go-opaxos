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
	log.Debugf("connecting to the host with uds file: %s", socketAddress)
	c.connection, err = net.Dial("unix", socketAddress)
	if err != nil {
		log.Errorf("failed to connect to the server: %v", err)
		return c
	}
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.responseCh = make(chan *CommandReply, GetConfig().Benchmark.BufferSize)

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
