package paxi

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi/encoder"
)

func init() {
	gob.Register(Request{})
	gob.Register(BytesRequest{})
	gob.Register(Reply{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(Transaction{})
	gob.Register(TransactionReply{})
	gob.Register(Register{})
	gob.Register(Config{})


	encoder.Register(Request{})
	encoder.Register(BytesRequest{})
	encoder.Register(Reply{})
	encoder.Register(Read{})
	encoder.Register(ReadReply{})
	encoder.Register(Transaction{})
	encoder.Register(TransactionReply{})
	encoder.Register(Register{})
	encoder.Register(Config{})
}

// http request header names
const (
	HTTPClientID  = "Id"
	HTTPCommandID = "Cid"
	HTTPTimestamp = "Timestamp"
	HTTPNodeID    = "Id"
)

/***************************
 * Client-Replica Commands *
 ***************************/

// Request is client request with http response channel
type Request struct {
	Command    Command
	Properties map[string]string
	Timestamp  int64
	NodeID     ID         // forward by node
	c          chan Reply // reply channel created by request receiver
}

// GenericRequest is Request but with generic []bytes command
type GenericRequest struct {
	*Request
	GenericCommand []byte
}

// BytesRequest is Request but with arbitrary []bytes command
type BytesRequest struct {
	Timestamp  int64
	NodeID     ID
	c          chan Reply
	Properties map[string]string
	Command    BytesCommand
}

// Reply replies to current client session
func (r *Request) Reply(reply Reply) {
	r.c <- reply
}

func (r Request) String() string {
	return fmt.Sprintf("Request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

func (r *Request) ToGenericRequest() GenericRequest {
	genericCmdBuff := bytes.Buffer{}
	encoder := gob.NewEncoder(&genericCmdBuff)
	_ = encoder.Encode(r.Command)
	return GenericRequest{r, genericCmdBuff.Bytes()}
}

func (r *Request) ToBytesRequest() BytesRequest {
	return BytesRequest{
		Timestamp:  r.Timestamp,
		NodeID:     r.NodeID,
		c:          r.c,
		Properties: r.Properties,
		Command:    r.Command.ToBytesCommand(),
	}
}

func (r *BytesRequest) Reply(reply Reply) {
	r.c <- reply
}

func (r *BytesRequest) String() string {
	return fmt.Sprintf("BytesRequest {nid=%v cmd=%v}", r.Command, r.NodeID)
}

// Reply includes all info that might reply to back the client for the corresponding request
type Reply struct {
	Command    Command
	Value      Value
	Properties map[string]string
	Timestamp  int64
	Err        error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%x prop=%v}", r.Command, r.Value, r.Properties)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID int
	Key       Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID int
	Value     Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%x}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
// TODO read-only or write-only transactions
type Transaction struct {
	Commands  []Command
	Timestamp int64

	c chan TransactionReply
}

// Reply replies to current client session
func (t *Transaction) Reply(r TransactionReply) {
	t.c <- r
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {cmds=%v}", t.Commands)
}

// TransactionReply is the result of transaction struct
type TransactionReply struct {
	OK        bool
	Commands  []Command
	Timestamp int64
	Err       error
}

/**************************
 *     Config Related     *
 **************************/

// Register message type is used to register self (node or client) with master node
type Register struct {
	Client bool
	ID     ID
	Addr   string
}
