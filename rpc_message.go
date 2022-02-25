package paxi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// messages types handled in this rpc server
const (
	COMMAND         byte = iota // generic rsm command
	COMMAND_NOREPLY             // generic rsm command, no reply
	CRASH                       // crash this node for t seconds, no reply
	DROP                        // drop messages to node x for t seconds, no reply
)

const (
	RESPONSE_OK byte = iota
	RESPONSE_ERR
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

func (m *RPCMessage) String() string {
	return fmt.Sprintf("RPCMessage{id: %d, len: %d, data: %x}", m.MessageID, m.MessageLen, m.Data)
}

const ringSize = 4096
const mask = ringSize-1

type ring struct {
	_     [8]uint64
	write uint64
	_     [7]uint64
	read1 uint64
	_     [7]uint64
	read2 uint64
	_     [7]uint64
	mask  uint64
	_     [7]uint64
	slots [ringSize]slot
}

type slot struct {
	cond   *sync.Cond
	mark   uint32
	req    RPCMessage
	respCh chan RPCMessage
}

func newRing() *ring {
	r := &ring{}
	for i := range r.slots {
		r.slots[i] = slot{respCh: make(chan RPCMessage, 0)}
		r.slots[i].cond = &sync.Cond{}
		r.slots[i].cond.L = &sync.Mutex{}
	}
	return r
}