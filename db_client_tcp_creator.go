package paxi

type TCPClientCreator struct {
	BenchmarkClientCreator
	targetHostID ID
}

func (c TCPClientCreator) WithHostID(id ID) TCPClientCreator {
	c.targetHostID = id
	return c
}

func (c TCPClientCreator) Create() (Client, error) {
	panic("unimplemented")
}

func (c TCPClientCreator) CreateAsyncClient() (AsyncClient, error) {
	newClient := NewTCPClient(c.targetHostID).Start()
	return newClient, nil
}

func (c TCPClientCreator) CreateCallbackClient() (AsyncCallbackClient, error) {
	panic("unimplemented")
}
