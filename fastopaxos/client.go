package fastopaxos

import "github.com/ailidani/paxi"

// Client implements paxi.Client interface
type Client struct {
	paxi.Client

	nodeClients map[paxi.ID]*paxi.TCPClient
}

func NewClient() *Client {
	config := paxi.GetConfig()
	client := &Client{}

	// initialize connection to all the nodes
	for id, _ := range config.PublicAddrs {
		client.nodeClients[id] = paxi.NewTCPClient(id)
	}

	return client
}

func (c *Client) Get()  {

}
