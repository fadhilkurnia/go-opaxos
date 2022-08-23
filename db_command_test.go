package paxi

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestSerializeDeserializeDBCommandGet(t *testing.T) {
	c := &DBCommandGet{
		CommandID: 313,
		SentAt:    time.Now().UnixNano(),
		Key:       354,
	}

	buff := c.Serialize()
	if GetDBCommandTypeFromBuffer(buff) != TypeDBGetCommand {
		t.Error("buffer must be a get command")
		return
	}

	nc := DeserializeDBCommandGet(buff)
	if nc.CommandID != c.CommandID || nc.SentAt != c.SentAt || nc.Key != c.Key {
		t.Errorf("get different get command %v, suppose to be %v", nc, c)
		return
	}
}

func TestSerializeDeserializeDBCommandPut(t *testing.T) {
	c := &DBCommandPut{
		CommandID: 313,
		SentAt:    time.Now().UnixNano(),
		Key:       354,
		Value:     []byte("oblivious paxos, a novel privacy-preserving consensus"),
	}

	buff := c.Serialize()
	if GetDBCommandTypeFromBuffer(buff) != TypeDBPutCommand {
		t.Error("buffer must be a put command")
		return
	}

	nc := DeserializeDBCommandPut(buff)
	if nc.CommandID != c.CommandID || nc.SentAt != c.SentAt || nc.Key != c.Key {
		t.Errorf("get different put command %v, suppose to be %v", nc, c)
		return
	}
	if !bytes.Equal(nc.Value, c.Value) {
		t.Errorf("git different value %v, suppose to be %v", nc.Value, c.Value)
		return
	}
}

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
		Key:       []byte{1, 1, 1, 1},
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
