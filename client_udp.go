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

// UDPClient implements Client interface with
// Unix Domain Socket (UDP) connection, the node server and client
// need to be in the same machine.
type UDPClient struct {
	Client

	connection net.Conn
	buffWriter *bufio.Writer
	buffReader *bufio.Reader
	curCmdID   uint32

	responseCh chan *CommandReply
}

func NewUDPClient(id ID) *UDPClient {
	var err error
	c := new(UDPClient)

	rpcPort := GetConfig().GetRPCPort(id)
	socketAddress := fmt.Sprintf("/tmp/rpc_%s.sock", rpcPort)
	log.Debugf("connect to the server with udp socket: %s", socketAddress)

	c.connection, err = net.Dial("unix", socketAddress)
	if err != nil {
		log.Errorf("failed to connect to server: %v", err)
		return c
	}
	c.buffWriter = bufio.NewWriter(c.connection)
	c.buffReader = bufio.NewReader(c.connection)
	c.responseCh = make(chan *CommandReply, GetConfig().ChanBufferSize)

	return c
}

// Start starts listening response from server
// to use this: NewUDPClient(id).Start()
func (c *UDPClient) Start() *UDPClient {
	go c.putResponseToChannel()
	return c
}

func (c *UDPClient) putResponseToChannel() {
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

func (c *UDPClient) SendCommand(req interface{}) error {
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

func (c *UDPClient) GetReceiverChannel() chan *CommandReply {
	return c.responseCh
}

// do is a blocking interface, the client send a command request
// and wait until it receives the first response
func (c *UDPClient) do(req GenericCommand) (*CommandReply, error) {
	c.curCmdID++
	req.CommandID = c.curCmdID
	req.SentAt = time.Now().Unix()

	cmdBytes := req.Marshal()
	buff := make([]byte, 5)
	buff[0] = COMMAND
	binary.BigEndian.PutUint32(buff[1:], uint32(len(cmdBytes)))
	buff = append(buff, cmdBytes...)

	// send request
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

			return resp, err
		}

		break
	}

	return nil, err
}

func (c *UDPClient) Get(key Key) (Value, error) {
	cmd := GenericCommand{
		Operation: OP_READ,
		Key:       key.ToBytes(),
	}

	resp, err := c.do(cmd)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func (c *UDPClient) Put(key Key, val Value) error {
	cmd := GenericCommand{
		Operation: OP_WRITE,
		Key:       key.ToBytes(),
		Value:     val,
	}
	log.Debugf("sending generic commands: %s", cmd.String())

	_, err := c.do(cmd)
	if err != nil {
		return err
	}

	return nil
}

func (c *UDPClient) Put2(key Key, val Value) (interface{}, error) {
	panic("unimplemented")
}
