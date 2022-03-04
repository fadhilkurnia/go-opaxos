package encoder

import (
	"bufio"
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
	"reflect"
	"sync"
	"testing"
)

type Dummy struct {
	A string
	B []byte
	C int64
}

func TestMsgPackInterface(t *testing.T) {
	dummy := Dummy{
		A: "asdasd",
		B: []byte("asdasd"),
		C: 123,
	}

	var dummyInterface interface{}
	dummyInterface = dummy

	rawData, err := msgpack.Marshal(dummyInterface)
	if err != nil {
		t.Error(err)
	}

	receiverInterface := reflect.New(reflect.TypeOf(dummyInterface)).Interface()
	err = msgpack.Unmarshal(rawData, &receiverInterface)
	if err != nil {
		t.Error(err)
	}

	t.Log(reflect.TypeOf(receiverInterface))
}

func TestEncodeDecode(t *testing.T) {
	dummy := Dummy{
		A: "asdasd",
		B: []byte("asdasd"),
		C: 123,
	}

	Register(Dummy{})

	buffw := bytes.NewBuffer([]byte{})
	bww := bufio.NewWriter(buffw)
	if err := Encode(bww, dummy); err != nil {
		t.Errorf("%v\n", err)
	}

	buffr := bytes.NewBuffer(buffw.Bytes())
	bwr := bufio.NewReader(buffr)

	var res interface{}
	err := Decode(bwr, &res)
	if err != nil {
		t.Error(err)
	}
	t.Log(registeredTypes)

	if res.(Dummy).A != dummy.A || res.(Dummy).C != dummy.C || !bytes.Equal(res.(Dummy).B, dummy.B) {
		t.Errorf("failed to decode the original struct\n")
	}
}

func TestEncodeDecodePointer(t *testing.T) {
	dummy := &Dummy{
		A: "asdasd",
		B: []byte("asdasd"),
		C: 123,
	}

	Register(&Dummy{})

	buffw := bytes.NewBuffer([]byte{})
	bww := bufio.NewWriter(buffw)
	if err := Encode(bww, dummy); err != nil {
		t.Errorf("%v\n", err)
	}

	buffr := bytes.NewBuffer(buffw.Bytes())
	bwr := bufio.NewReader(buffr)

	var res interface{}
	err := Decode(bwr, &res)
	if err != nil {
		t.Error(err)
	}
	t.Log(registeredTypes)

	if res.(*Dummy).A != dummy.A || res.(*Dummy).C != dummy.C || !bytes.Equal(res.(*Dummy).B, dummy.B) {
		t.Errorf("failed to decode the original struct\n")
	}
}

func TestEncodeDecodeMultiGoroutine(t *testing.T) {
	dummy := Dummy{
		A: "asdasd",
		B: []byte("asdasd"),
		C: 123,
	}

	Register(Dummy{})

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		buffw := bytes.NewBuffer([]byte{})
		bww := bufio.NewWriter(buffw)
		if err := Encode(bww, dummy); err != nil {
			t.Error(err)
		}

		buffr := bytes.NewBuffer(buffw.Bytes())
		bwr := bufio.NewReader(buffr)

		var res interface{}
		err := Decode(bwr, &res)
		if err != nil {
			t.Error(err)
		}

		if res.(Dummy).A != dummy.A || res.(Dummy).C != dummy.C || !bytes.Equal(res.(Dummy).B, dummy.B) {
			t.Error("failed to decode the original struct")
		}
	}()

	go func() {
		defer wg.Done()
		buffw := bytes.NewBuffer([]byte{})
		bww := bufio.NewWriter(buffw)
		if err := Encode(bww, dummy); err != nil {
			t.Error(err)
		}

		buffr := bytes.NewBuffer(buffw.Bytes())
		bwr := bufio.NewReader(buffr)

		var res interface{}
		err := Decode(bwr, &res)
		if err != nil {
			t.Error(err)
		}

		if res.(Dummy).A != dummy.A || res.(Dummy).C != dummy.C || !bytes.Equal(res.(Dummy).B, dummy.B) {
			t.Error("failed to decode the original struct")
		}
	}()

	wg.Wait()
}
