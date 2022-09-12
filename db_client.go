package paxi

import "flag"

var ClientType = flag.String("client_type", "unix", "client type that sending command to the server, options: unix (default), tcp, http")
var ClientAction = flag.String("client_action", "block", "how the client communicate with the server, options: block (default), pipeline, callback, throughput_collector, tracefile")
var ClientIsStateful = flag.Bool("client_stateful", false, "whether the client store the request metadata or not, by default all the metadata (e.g timestamp) stored in the request & response")

// Client interface provides get and put for key-value store client
type Client interface {
	Get(Key) (Value, error)
	Put(Key, Value) error
	Put2(Key, Value) (interface{}, error)
}

// AdminClient interface provides fault injection operation
type AdminClient interface {
	Consensus(Key) bool
	Crash(ID, int)    // Crash simulates a crashed node for certain second
	Drop(ID, ID, int) // Drop drops all message from a node to another node
	Partition(int, ...ID)

	// TODO: GetHistory
}

// see the implementation in these files:
// - db_client_http.go
// - db_client_tcp.go
// - db_client_unix.go

type AsyncClient interface {
	SendCommand(SerializableCommand) error
	GetResponseChannel() chan *CommandReply
}

type AsyncCallbackClient interface {
	Get(Key, func(reply *CommandReply))
	Put(Key, Value, func(reply *CommandReply))
}

// DB is general interface implemented by client to call client library
// used for benchmark purposes.
type DB interface {
	Init() error

	// Read Write Write2 and Write3 are synchronous operation (blocking)
	Read(key int) (int, error)
	Write(key, value int) error
	Write2(key, value int) (interface{}, error)
	Write3(key int, value []byte) (interface{}, error)

	Stop() error
}

type DBClientFactory interface {
	Create() (BenchmarkClient, error)
}

type NonBlockingDBClientFactory interface {
	Create() (NonBlockingDBClient, error)
}

type BenchmarkClient interface {
	// AsyncRead and AsyncWrite are asynchronous operation (non-blocking).
	// they are used to saturate the server with as many request as possible with limited connection.
	AsyncRead(key []byte, callback func(*CommandReply))
	AsyncWrite(key, value []byte, callback func(*CommandReply))

	// Read and Write are the typical blocking operation
	Read(key []byte) (interface{}, interface{}, error) // return value, metadata, and error
	Write(key, value []byte) (interface{}, error)      // return metadata and error
}

// TODO: consolidate DB and BenchmarkClient