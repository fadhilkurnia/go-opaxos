package paxi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"time"
)

const (
	OP_READ uint8 = iota
	OP_WRITE
)

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

	SentAt time.Time // timestamp filled and read by client only
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
