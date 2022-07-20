package paxi

// Client interface provides get and put for key-value store client
type Client interface {
	Get(Key) (Value, error)
	Put(Key, Value) error
	Put2(Key, Value) (interface{}, error)
}

// AdminClient interface provides fault injection operation
type AdminClient interface {
	Consensus(Key) bool
	Crash(ID, int)
	Drop(ID, ID, int)
	Partition(int, ...ID)
}

// see the implementation in these files:
// - client_http.go
// - client_tcp.go
// - client_udp.go