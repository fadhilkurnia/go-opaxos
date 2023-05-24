package paxi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"sync/atomic"
	"time"
)

// This file contains all type of command used for communication in
// the tcp and unix based client and server. For http based client/server
// check out db_command_http.go.
// For now, we have these structs:
// - RPCMessage
// - RPCMessageMetadata
// - BytesCommand
// - GenericCommand
// - CommandReply
// - Command -> used for database interface
// - ClientBytesCommand -> replaced with ClientCommand
// - CrashMessage
// TODO: They need to be consolidated to avoid confusion
// ---> Command (generic with bytes slice: type (1 byte), length (4 bytes), ... buffer )
// -------> DBCommand: Get, Put (done)
// -------> AdminCommand: Crash, Delay, Slow, etc (done)
// ---> ClientCommand (Command with reply writer)
// ---> CommandReply (the reply for command, generic with bytes slice)

// type of commands sent as the header in the tcp/uds stream
const (
	COMMAND         byte = iota // generic command
	COMMAND_NOREPLY             // generic command, no reply
	CRASH                       // crash this node for t seconds, no reply
	DROP                        // drop messages to node x for t seconds, no reply

	TypeDBGetCommand
	TypeDBPutCommand
	TypeAdminCrashCommand
	TypeAdminDropCommand
	TypeAdminDelayCommand
	TypeAdminSlowCommand
	TypeAdminPartitionCommand

	TypeOtherCommand       // consensus-protocol specific command, used for Fast-OPaxos
	TypeGetMetadataCommand // used in Fast-OPaxos' client

	TypeEmulatedCommand // used when --req-tracefile is specified to emulate arbitrary command execution, check at db_command_emulation.go

	TypeBeLeaderCommand // used in untrusted mode

	TypeCommandReply
)

var AdminCommandTypes = lib.NewNonEmptySet(
	TypeAdminCrashCommand,
	TypeAdminDropCommand,
	TypeAdminDelayCommand,
	TypeAdminSlowCommand,
	TypeAdminPartitionCommand,
	TypeBeLeaderCommand,
)

// type of responses for command
const (
	CommandReplyOK byte = iota
	CommandReplyErr
)

// type of metadata in the command's reply
const (
	MetadataSecretSharingTime byte = iota
	MetadataAcceptedBallot
	MetadataSlot
	MetadataCurrentBallot
	MetadataLeaderAck
)

var ServerToClientDelay uint64 = 0

type SerializableCommand interface {
	Serialize() []byte
	GetCommandType() byte
}

type DBCommandGet struct {
	CommandID uint32
	SentAt    int64
	Key       Key
}

type DBCommandPut struct {
	CommandID uint32
	SentAt    int64
	Key       Key
	Value     Value
}

type AdminCommandCrash struct {
	Duration uint32 // Duration is the crash duration in second
}

type AdminCommandDrop struct {
	Duration   uint32 // Duration is the drop duration in second
	TargetNode ID     // All messages to TargetNode will be dropped for Duration second
}

type ClientCommand struct {
	CommandType   byte
	RawCommand    []byte
	ReplyStream chan *CommandReply
}

// CommandReply is the reply for both DBCommand and AdminCommand.
// It includes several meta-data for measurement and debugging purposes.
type CommandReply struct {
	CommandID uint32               `msgpack:"i"`           // CommandID the ID of the command replayed by this struct
	SentAt    int64                `msgpack:"t"`           // the time (in unixnano) when client sent the command replayed
	Code      byte                 `msgpack:"c"`           // Code is either CommandReplyOK or CommandReplyErr
	Data      []byte               `msgpack:"d"`           // any data, if this is reply for put command, the Data contains the value
	Metadata  map[byte]interface{} `msgpack:"m,omitempty"` // metadata for measurements, the key is Metadata constant
}

var respCounter uint64 = 0

func (c *ClientCommand) Reply(r *CommandReply) error {

	if r.SentAt == 123 {
		atomic.StoreUint64(&respCounter, 0)
	}
	rc := atomic.AddUint64(&respCounter, 1)
	log.Infof("reply counter %d", rc)

	// defer sending reply to the client
	if ServerToClientDelay > 0 {
		go func() {
			time.Sleep(time.Duration(ServerToClientDelay))
			c.ReplyStream <- r
		}()
		return nil
	}

	// send reply to the client
	c.ReplyStream <- r

	return nil
}

func (c *ClientCommand) String() string {
	return fmt.Sprintf("ClientCommand: type=%d raw=%v", c.CommandType, c.RawCommand)
}

// Serialize makes a bytes array: CommandID (uint32: 4 bytes), SentAt (int64: 8 bytes)
// Key (int32: 4 bytes), ValueLen (uint32: 4 bytes), Value (ValueLen bytes).
// Since this is a Get command, ValueLen is always 0.
func (c *DBCommandGet) Serialize() []byte {
	b := make([]byte, 20)

	// writing CommandID
	binary.BigEndian.PutUint32(b[0:4], c.CommandID)

	// writing SentAt
	binary.BigEndian.PutUint64(b[4:12], uint64(c.SentAt))

	// writing Key
	binary.BigEndian.PutUint32(b[12:16], uint32(c.Key))

	// writing ValueLen that is always 0 for Get command
	binary.BigEndian.PutUint32(b[16:20], uint32(0))

	return b
}

func (c *DBCommandGet) GetCommandType() byte {
	return TypeDBGetCommand
}

// DeserializeDBCommandGet creates Get command from buffer
func DeserializeDBCommandGet(buff []byte) *DBCommandGet {
	// len should be at least 20:
	// CommandID (4 bytes), SentAt (8 bytes), Key (4 bytes), ValueLen (4 bytes)
	if len(buff) < 20 {
		log.Errorf("potentially wrong DB command's buffer, len=%d should be at least 20", len(buff))
	}

	c := &DBCommandGet{}
	c.CommandID = binary.BigEndian.Uint32(buff[0:4])
	c.SentAt = int64(binary.BigEndian.Uint64(buff[4:12]))
	c.Key = Key(binary.BigEndian.Uint32(buff[12:16]))

	return c
}

// Serialize makes a bytes array: CommandID (uint32: 4 bytes), SentAt (int64: 8 bytes)
// Key (int32: 4 bytes), ValueLen (uint32: 4 bytes), Value (ValueLen bytes).
func (c *DBCommandPut) Serialize() []byte {
	valLen := uint32(len(c.Value))
	b := make([]byte, 20+valLen)

	if valLen == 0 {
		log.Warningf("value is empty for key=%d", c.Key)
	}

	// writing CommandID
	binary.BigEndian.PutUint32(b[0:4], c.CommandID)

	// writing SentAt
	binary.BigEndian.PutUint64(b[4:12], uint64(c.SentAt))

	// writing Key
	binary.BigEndian.PutUint32(b[12:16], uint32(c.Key))

	// writing ValueLen
	binary.BigEndian.PutUint32(b[16:20], valLen)

	// writing value
	if valLen > 0 {
		copy(b[20:20+valLen], c.Value)
	}

	return b
}

func (c *DBCommandPut) GetCommandType() byte {
	return TypeDBPutCommand
}

// DeserializeDBCommandPut creates Put command from buffer
func DeserializeDBCommandPut(buff []byte) *DBCommandPut {
	// len should be at least 20:
	// CommandID (4 bytes), SentAt (8 bytes), Key (4 bytes), ValueLen (4 bytes)
	if len(buff) < 20 {
		log.Errorf("potentially wrong DB command's buffer, len=%d should be at least 20", len(buff))
	}

	c := &DBCommandPut{}
	c.CommandID = binary.BigEndian.Uint32(buff[0:4])
	c.SentAt = int64(binary.BigEndian.Uint64(buff[4:12]))
	c.Key = Key(binary.BigEndian.Uint32(buff[12:16]))
	valLen := binary.BigEndian.Uint32(buff[16:20])
	if valLen > 0 {
		c.Value = buff[20 : 20+valLen]
	}

	return c
}

// GetDBCommandTypeFromBuffer returns either TypeDBPutCommand or TypeDBGetCommand
// based on the encoded value length in the buffer
func GetDBCommandTypeFromBuffer(buff []byte) byte {
	// len should be at least 20:
	// CommandID (4 bytes), SentAt (8 bytes), Key (4 bytes), ValueLen (4 bytes)
	if len(buff) < 20 {
		log.Errorf("potentially wrong DB command's buffer, len=%d should be at least 20", len(buff))
		return TypeDBGetCommand
	}

	valLenBytes := buff[16:20]
	valLen := binary.BigEndian.Uint32(valLenBytes)
	if valLen == 0 {
		return TypeDBGetCommand
	}
	return TypeDBPutCommand
}

// GetDBCommandTypeFromBufferVerbose is similar with GetDBCommandTypeFromBuffer, but
// returning the error if there is any
func GetDBCommandTypeFromBufferVerbose(buff []byte) (byte, error) {
	// len should be at least 20:
	// CommandID (4 bytes), SentAt (8 bytes), Key (4 bytes), ValueLen (4 bytes)
	if len(buff) < 20 {
		err := errors.New(fmt.Sprintf("potentially wrong DB command's buffer, len=%d should be at least 20", len(buff)))
		return TypeDBGetCommand, err
	}

	valLenBytes := buff[16:20]
	valLen := binary.BigEndian.Uint32(valLenBytes)
	if valLen == 0 {
		return TypeDBGetCommand, nil
	}

	if len(buff) != 20+int(valLen) {
		err := errors.New(fmt.Sprintf("potentially wrong DB command's buffer, len=%d should be %d", len(buff), 20+int(valLen)))
		return TypeDBPutCommand, err
	}

	return TypeDBPutCommand, nil
}

func (c *AdminCommandCrash) Serialize() []byte {
	buff, err := msgpack.Marshal(c)
	if err != nil {
		log.Errorf("failed to serialize crash command: %v", err)
		return nil
	}

	return buff
}

func (c *AdminCommandCrash) GetCommandType() byte {
	return TypeAdminCrashCommand
}

func DeserializeAdminCommandCrash(buff []byte) *AdminCommandCrash {
	c := &AdminCommandCrash{}
	if err := msgpack.Unmarshal(buff, c); err != nil {
		log.Errorf("failed to deserialize crash command: %v", err)
		return nil
	}

	return c
}

func (c *AdminCommandDrop) Serialize() []byte {
	buff, err := msgpack.Marshal(c)
	if err != nil {
		log.Errorf("failed to serialize drop command: %v", err)
		return nil
	}

	return buff
}

func (c *AdminCommandDrop) GetCommandType() byte {
	return TypeAdminDropCommand
}

func DeserializeAdminCommandDrop(buff []byte) *AdminCommandDrop {
	c := &AdminCommandDrop{}
	if err := msgpack.Unmarshal(buff, c); err != nil {
		log.Errorf("failed to deserialize drop command: %v", err)
		return nil
	}

	return c
}

func (m *CommandReply) Serialize() []byte {
	b, err := msgpack.Marshal(m)
	if err != nil {
		log.Errorf("failed to serialize command's reply: %v", err)
	}
	return b
}

func DeserializeCommandReply(buff []byte) (*CommandReply, error) {
	cr := &CommandReply{}
	err := msgpack.Unmarshal(buff, cr)
	return cr, err
}

// TODO: deprecate this, replaced with DeserializeCommandReply
func UnmarshalCommandReply(buffer []byte) (*CommandReply, error) {
	cr := &CommandReply{}
	err := msgpack.Unmarshal(buffer, cr)
	return cr, err
}

// ============================================= End of new implementation
// =======================================================================
// =======================================================================

// type of operations for command
const (
	OP_READ uint8 = iota
	OP_WRITE
)

type RPCMessage struct {
	MessageID  uint32
	MessageLen uint32
	Data       []byte

	Reply *bufio.Writer // used in rpc-server, goroutine safe for concurrent write (net.Conn)
}

type RPCMessageMetadata struct {
	startTime time.Time   // start time
	procTime  int64       // processing time in ns
	ch        chan []byte // channel to the caller
}

func NewRPCMessage(wire *bufio.Reader) (*RPCMessage, error) {
	ret := &RPCMessage{}

	msgIDLenBytes := make([]byte, 8)
	if _, err := io.ReadAtLeast(wire, msgIDLenBytes[:4], 4); err != nil {
		return nil, err
	}
	ret.MessageID = binary.BigEndian.Uint32(msgIDLenBytes[:4])
	if _, err := io.ReadAtLeast(wire, msgIDLenBytes[4:], 4); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(msgIDLenBytes[4:])

	var data []byte
	if msgLen > 0 {
		data = make([]byte, msgLen)
		if _, err := io.ReadAtLeast(wire, data, int(msgLen)); err != nil {
			return nil, err
		}
	}

	ret.MessageLen = msgLen
	ret.Data = data

	return ret, nil
}

func NewRPCMessageWithReply(buffer *bufio.Reader, reply *bufio.Writer) (*RPCMessage, error) {
	ret, err := NewRPCMessage(buffer)
	if err != nil {
		return nil, err
	}
	ret.Reply = reply

	return ret, err
}

func (m *RPCMessage) Serialize(wire *bufio.Writer) (err error) {
	buff, err := m.ToBytes()
	if err != nil {
		return err
	}

	if _, err = wire.Write(buff); err != nil {
		return err
	}
	return wire.Flush()
}

func (m *RPCMessage) ToBytes() ([]byte, error) {
	buffer := bytes.Buffer{}
	msgIDLenBytes := make([]byte, 8)
	binary.BigEndian.PutUint32(msgIDLenBytes[:4], m.MessageID)
	binary.BigEndian.PutUint32(msgIDLenBytes[4:], m.MessageLen)
	if n, err := buffer.Write(msgIDLenBytes[:4]); err != nil || n != 4 {
		if n != 4 {
			return nil, errors.New(fmt.Sprintf("failed to write messageID to buffer: expected 4 bytes, but only %d bytes", n))
		}
		return nil, err
	}
	if n, err := buffer.Write(msgIDLenBytes[4:8]); err != nil || n != 4 {
		if n != 4 {
			return nil, errors.New(fmt.Sprintf("failed to write messageID to buffer: expected 4 bytes, but only %d bytes", n))
		}
		return nil, err
	}
	if m.MessageLen > 0 {
		if n, err := buffer.Write(m.Data); err != nil || n != int(m.MessageLen) {
			if n != 4 {
				return nil, errors.New(fmt.Sprintf("failed to write messageID to buffer: expected %d bytes, but only %d bytes", m.MessageLen, n))
			}
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (m *RPCMessage) SendReply(result []byte) error {
	response := RPCMessage{
		MessageID:  m.MessageID,
		MessageLen: uint32(len(result)),
		Data:       result,
	}
	if err := m.Reply.WriteByte(COMMAND); err != nil {
		return err
	}
	return response.Serialize(m.Reply)
}

// SendBytesReply do not encapsulate the result with RPCMessage
// length + raw bytes data
func (m *RPCMessage) SendBytesReply(result []byte) error {
	resLenBuff := make([]byte, 4)
	resLen := len(result)

	if err := m.Reply.WriteByte(COMMAND); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(resLenBuff, uint32(resLen))
	if _, err := m.Reply.Write(resLenBuff); err != nil {
		return err
	}
	if _, err := m.Reply.Write(result); err != nil {
		return err
	}
	return m.Reply.Flush()
}

func (m *RPCMessage) String() string {
	return fmt.Sprintf("RPCMessage{id: %d, len: %d, data: %x}", m.MessageID, m.MessageLen, m.Data)
}

// BytesCommand is command in bytes, consecutively:
// CommandID (4 bytes), Operation (1 byte), KeyLen (2 bytes),
// Key (KeyLen bytes), ValueLen (4 bytes), Value (ValueLen bytes)
type BytesCommand []byte

// GenericCommand uses []byte as Key and Value
type GenericCommand struct {
	CommandID uint32
	Operation uint8
	Key       []byte
	Value     []byte

	SentAt int64 // timestamp in ns, filled and read by client only
}

func (b *BytesCommand) ToCommand() Command {
	var cmd Command
	bc := []byte(*b)

	// read 4 bytes of CommandID
	cmd.CommandID = int(binary.BigEndian.Uint32(bc[0:4]))

	// skip 1 byte of Operation
	_ = bc[4]

	// read 2 bytes of KeyLen
	keyLen := binary.BigEndian.Uint16(bc[5:7])

	// read KeyLen bytes of Key
	cmd.Key = Key(binary.BigEndian.Uint32(bc[7 : 7+keyLen]))

	// read 4 bytes of ValLen
	valLen := binary.BigEndian.Uint32(bc[7+keyLen : 7+keyLen+4])

	// read valLen bytes of Val
	if valLen != 0 {
		cmd.Value = bc[7+keyLen+4 : 7+uint32(keyLen)+4+valLen]
	}

	return cmd
}

func (b *BytesCommand) ToGenericCommand() GenericCommand {
	var cmd GenericCommand
	bc := []byte(*b)

	// read 4 bytes of CommandID
	cmd.CommandID = binary.BigEndian.Uint32(bc[:4])

	// read 1 byte of Operation
	cmd.Operation = bc[4]

	// read 2 bytes of KeyLen
	keyLen := binary.BigEndian.Uint16(bc[5:7])

	// read KeyLen bytes of Key
	cmd.Key = bc[7 : 7+keyLen]

	// read 4 bytes of ValLen
	valLen := binary.BigEndian.Uint32(bc[7+keyLen : 7+keyLen+4])

	// read valLen bytes of Val
	if valLen != 0 {
		cmd.Value = bc[7+keyLen+4 : 7+uint32(keyLen)+4+valLen]
	}

	return cmd
}

func UnmarshalGenericCommand(buffer []byte) (*GenericCommand, error) {
	cmd := &GenericCommand{}
	err := msgpack.Unmarshal(buffer, cmd)
	return cmd, err
}

func (g *GenericCommand) ToBytesCommand() BytesCommand {
	// CommandID (4 bytes), Operation (1 byte), KeyLen (2 bytes),
	// Key (KeyLen bytes), ValueLen (4 bytes), Value (ValueLen bytes)
	b := make([]byte, 4+1+2+len(g.Key)+4+len(g.Value))

	// 4 bytes CommandID
	binary.BigEndian.PutUint32(b, g.CommandID)

	// 1 byte Operation
	b[4] = g.Operation

	// 2 bytes KeyLen
	binary.BigEndian.PutUint16(b[5:], uint16(len(g.Key)))

	// KeyLen bytes Key
	copy(b[7:], g.Key)

	// 4 bytes ValLen
	binary.BigEndian.PutUint32(b[7+len(g.Key):], uint32(len(g.Value)))

	// ValLen bytes Val
	copy(b[7+len(g.Key)+4:], g.Value)

	return b
}

func (g *GenericCommand) Marshal() []byte {
	ret, _ := msgpack.Marshal(g)
	return ret
}

func (c *Command) ToBytesCommand() BytesCommand {
	// CommandID (4 bytes), Operation (1 byte), KeyLen (2 bytes),
	// Key (KeyLen bytes / 4 bytes since key is int in Command),
	// ValueLen (4 bytes), Value (ValueLen bytes)
	lenVal := len(c.Value)
	b := make([]byte, 4+1+2+4+4+lenVal)

	// writing CommandID
	binary.BigEndian.PutUint32(b[0:4], uint32(c.CommandID))
	// writing operation
	b[4] = OP_WRITE
	if len(c.Value) == 0 {
		b[4] = OP_READ
	}
	// writing keyLen
	binary.BigEndian.PutUint16(b[5:7], 4)
	// writing key
	binary.BigEndian.PutUint32(b[7:11], uint32(c.Key))
	// writing valLen
	binary.BigEndian.PutUint32(b[11:15], uint32(lenVal))
	// writing value
	if lenVal > 0 {
		copy(b[15:15+lenVal], c.Value)
	}
	return b
}

func (g *GenericCommand) Empty() bool {
	if g.CommandID == 0 && len(g.Key) == 0 && len(g.Value) == 0 {
		return true
	}
	return false
}

func (g *GenericCommand) IsRead() bool {
	return g.Operation == OP_READ
}

func (g *GenericCommand) IsWrite() bool {
	return g.Operation == OP_WRITE
}

func (g *GenericCommand) Equal(a *GenericCommand) bool {
	return bytes.Equal(g.Key, a.Key) && bytes.Equal(g.Value, a.Value) && g.CommandID == a.CommandID
}

func (g *GenericCommand) String() string {
	if len(g.Value) == 0 {
		return fmt.Sprintf("Get{cid=%d key=%x}", g.CommandID, g.Key)
	}
	return fmt.Sprintf("Put{cid=%d key=%x value=%x", g.CommandID, g.Key, g.Value)
}

// Command of key-value database
type Command struct {
	Key       Key
	Value     Value
	ClientID  ID
	CommandID int
}

func (c Command) Empty() bool {
	if c.Key == 0 && c.Value == nil && c.ClientID == "" && c.CommandID == 0 {
		return true
	}
	return false
}

func (c Command) IsRead() bool {
	return len(c.Value) == 0
}

func (c Command) IsWrite() bool {
	return c.Value != nil
}

func (c Command) Equal(a Command) bool {
	return c.Key == a.Key && bytes.Equal(c.Value, a.Value) && c.ClientID == a.ClientID && c.CommandID == a.CommandID
}

func (c Command) String() string {
	if c.Value == nil {
		return fmt.Sprintf("Get{key=%v id=%s cid=%d}", c.Key, c.ClientID, c.CommandID)
	}
	return fmt.Sprintf("Put{key=%v value=%x id=%s cid=%d}", c.Key, c.Value, c.ClientID, c.CommandID)
}

// message format expected by rpc server:
// message type (1 byte), message_id (4 bytes), message_length (4 bytes), data (message_length bytes)
// message format returned by rpc server:
// message type (1 byte), message_id (4 bytes), message_length (4 bytes), data (message_length bytes)

// ClientBytesCommand wraps BytesCommand ([]byte) with pointer to RPCMessage
type ClientBytesCommand struct {
	*BytesCommand
	*RPCMessage
}


type BeLeaderRequest struct{}

func (r BeLeaderRequest) Serialize() []byte {
	buff, _ := msgpack.Marshal(r)
	return buff
}

func (r BeLeaderRequest) GetCommandType() byte {
	return TypeBeLeaderCommand
}