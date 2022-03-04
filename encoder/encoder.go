package encoder

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"math"
	"reflect"
)

// WARNING: encoder is not thread safe. Register the struct at the beginning
// before using this encoder.

var registeredTypes map[byte]reflect.Type
var registeredTypeCode map[reflect.Type]byte // reverse of registeredType
var countRegisteredType byte

func init()  {
	registeredTypes = make(map[byte]reflect.Type)
	registeredTypeCode = make(map[reflect.Type]byte)
	countRegisteredType = 0
}

func Register(structType interface{})  {
	if countRegisteredType == 255 {
		panic("encoder: can only handle 256 different type")
	}
	registeredTypes[countRegisteredType] = reflect.TypeOf(structType)
	registeredTypeCode[reflect.TypeOf(structType)] = countRegisteredType
	countRegisteredType++
}

func Decode(buffer *bufio.Reader, destination interface{}) error {

	if reflect.ValueOf(destination).Kind() != reflect.Ptr {
		return errors.New("encoder: destination should be a pointer")
	}

	firstByte, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	if _, ok := registeredTypes[firstByte]; !ok {
		return errors.New("encoder: can not decode unregistered types")
	}
	lenBytes := make([]byte, 4)
	n, err := io.ReadAtLeast(buffer, lenBytes, 4)
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("encoder: failed to read 4 bytes message length")
	}
	length := binary.BigEndian.Uint32(lenBytes)
	data := make([]byte, length)
	n, err = io.ReadAtLeast(buffer, data, int(length))
	if err != nil {
		return err
	}
	if uint32(n) != length {
		return errors.New(fmt.Sprintf("encoder: failed to read %d bytes message data", length))
	}

	rt := registeredTypes[firstByte]
	v := reflect.New(rt).Interface()

	err = msgpack.Unmarshal(data, v)
	if err != nil {
		return err
	}

	// change value where the destination pointer point to.
	reflect.ValueOf(destination).Elem().Set(reflect.ValueOf(v).Elem())

	return nil
}

func Encode(buffer *bufio.Writer, v interface{}) error {
	if _, ok := registeredTypeCode[reflect.TypeOf(v)]; !ok {
		return errors.New(fmt.Sprintf("encoder: can not encode unregistered type %v", reflect.TypeOf(v)))
	}

	data, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}

	if len(data) > math.MaxUint32 {
		return errors.New("encoder: marshaled data is too long")
	}
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(data)))

	lenBytes = append([]byte{registeredTypeCode[reflect.TypeOf(v)]}, lenBytes...)
	data = append(lenBytes, data...)
	n, err := buffer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New(fmt.Sprintf("encoder: failed to read %d bytes of typeID, length, and data", len(data)))
	}

	return buffer.Flush()
}