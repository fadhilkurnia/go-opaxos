package paxi

type UnixClientCreator struct {
	BenchmarkClientCreator
	targetHostID ID
}

func (c UnixClientCreator) WithHostID(id ID) UnixClientCreator {
	c.targetHostID = id
	return c
}

func (c UnixClientCreator) Create() (Client, error) {
	panic("unimplemented")
}

func (c UnixClientCreator) CreateAsyncClient() (AsyncClient, error) {
	newClient := NewUnixClient(c.targetHostID).Start()
	return newClient, nil
}

func (c UnixClientCreator) CreateCallbackClient() (AsyncCallbackClient, error) {
	panic("unimplemented")
}