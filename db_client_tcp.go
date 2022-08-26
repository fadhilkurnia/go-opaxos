package paxi

// TCPClient implements Client interface with plain TCP connection
type TCPClient struct {

}

func NewTCPClient(id ID) *TCPClient {
	return &TCPClient{}
}