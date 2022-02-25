package paxi

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeGenericCommand(t *testing.T) {
	gc := GenericCommand{
		CommandID: 100,
		Operation: OP_WRITE,
		Key:       []byte{1},
		Value:     []byte{100, 100, 100},
	}

	bc := gc.ToBytesCommand()
	gc2 := bc.ToGenericCommand()

	if gc.CommandID != gc2.CommandID {
		t.Errorf("unmatch commandID %d %d", gc.CommandID, gc2.CommandID)
	}
	if !bytes.Equal(gc.Key, gc2.Key) {
		t.Errorf("unmatch key %v %v", gc.Key, gc2.Key)
	}
	if !bytes.Equal(gc.Value, gc2.Value) {
		t.Errorf("unmatch value %v %v", gc.Value, gc2.Value)
	}
}

func TestSerializeCommand(t *testing.T) {
	gc := GenericCommand{
		CommandID: 100,
		Operation: OP_WRITE,
		Key:       []byte{1,1,1,1},
		Value:     []byte{100, 100, 100},
	}

	bc := gc.ToBytesCommand()
	gc2 := bc.ToCommand()

	if gc.CommandID != uint32(gc2.CommandID) {
		t.Errorf("unmatch commandID %d %d", gc.CommandID, gc2.CommandID)
	}
	if gc2.Key != Key(binary.BigEndian.Uint32(gc.Key)) {
		t.Errorf("unmatch key %v %v", gc.Key, gc2.Key)
	}
	if !bytes.Equal(gc.Value, gc2.Value) {
		t.Errorf("unmatch value %v %v", gc.Value, gc2.Value)
	}
}