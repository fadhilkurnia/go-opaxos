package paxi

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// BytesCommand is command in bytes, consecutively:
// CommandID (4 bytes), KeyLen (4 bytes), Key (KeyLen bytes)
// ValueLen (4 bytes), Value (ValueLen bytes)
type BytesCommand []byte

type GenericCommand struct {
	CommandID uint32
	Key       []byte
	Value     []byte
}

func (b *BytesCommand) ToCommand() Command {
	var cmd Command
	bc := []byte(*b)
	cmd.CommandID = int(binary.BigEndian.Uint32(bc[:4]))
	keyLen := binary.BigEndian.Uint32(bc[4:8])
	cmd.Key = Key(binary.BigEndian.Uint32(bc[8:12]))
	valLen := binary.BigEndian.Uint32(bc[keyLen+8 : keyLen+8+4])
	if valLen != 0 {
		cmd.Value = bc[keyLen+8+4 : keyLen+8+4+valLen]
	}
	return cmd
}

func (b *BytesCommand) ToGenericCommand() GenericCommand {
	var cmd GenericCommand
	bc := []byte(*b)
	cmd.CommandID = binary.BigEndian.Uint32(bc[:4])
	keyLen := binary.BigEndian.Uint32(bc[4:8])
	cmd.Key = bc[8 : keyLen+8]
	valLen := binary.BigEndian.Uint32(bc[keyLen+8 : keyLen+8+4])
	if valLen != 0 {
		cmd.Value = bc[keyLen+8+4 : keyLen+8+4+valLen]
	}
	return cmd
}

func (g *GenericCommand) ToBytesCommand() BytesCommand {
	b := make([]byte, 4+4+len(g.Key)+4+len(g.Value))
	binary.BigEndian.PutUint32(b, g.CommandID)
	binary.BigEndian.PutUint32(b[4:], uint32(len(g.Key)))
	copy(b[8:], g.Key)
	binary.BigEndian.PutUint32(b[8+len(g.Key):], uint32(len(g.Value)))
	copy(b[8+len(g.Key)+4:], g.Value)
	return b
}

func (g *GenericCommand) Empty() bool {
	if g.CommandID == 0 && len(g.Key) == 0 && len(g.Value) == 0 {
		return true
	}
	return false
}

func (g *GenericCommand) IsRead() bool {
	return len(g.Value) == 0
}

func (g *GenericCommand) IsWrite() bool {
	return len(g.Value) > 0
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
