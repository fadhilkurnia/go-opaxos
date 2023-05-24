package main

import (
	"flag"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/fastopaxos"
	"github.com/ailidani/paxi/untrustedopaxos"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")

//// db implements Paxi.DB interface for benchmarking
//type db struct {
//	paxi.Client
//}
//
//func (d *db) Init() error {
//	return nil
//}
//
//func (d *db) Stop() error {
//	return nil
//}
//
//func (d *db) Read(k int) (int, error) {
//	key := paxi.Key(k)
//	v, err := d.Get(key)
//	if len(v) == 0 {
//		return 0, nil
//	}
//	x, _ := binary.Uvarint(v)
//	return int(x), err
//}
//
//func (d *db) Write(k, v int) error {
//	key := paxi.Key(k)
//	value := make([]byte, binary.MaxVarintLen64)
//	binary.PutUvarint(value, uint64(v))
//	err := d.Put(key, value)
//	return err
//}
//
//func (d *db) Write2(k, v int) (interface{}, error) {
//	key := paxi.Key(k)
//	value := make([]byte, binary.MaxVarintLen64)
//	binary.PutUvarint(value, uint64(v))
//	ret, err := d.Put2(key, value)
//	return ret, err
//}
//
//func (d *db) Write3(k int, v []byte) (interface{}, error) {
//	ret, err := d.Put2(paxi.Key(k), v)
//	return ret, err
//}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	//d := new(db)
	//switch *algorithm {
	//case "paxosx":
	//	d.Client = paxos.NewClient(paxi.ID(*id))
	//case "opaxosx":
	//	d.Client = opaxos.NewClient(paxi.ID(*id))
	//default:
	//	d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	//}

	var bench *paxi.Benchmark
	bench = paxi.NewBenchmark()

	if *paxi.ClientType == "unix" {
		bench.ClientCreator = paxi.UnixClientCreator{}.WithHostID(paxi.ID(*id))
	} else if *paxi.ClientType == "tcp" {
		bench.ClientCreator = paxi.TCPClientCreator{}.WithHostID(paxi.ID(*id))
	} else {
		panic("unknown client type")
	}

	if *algorithm == "fastopaxos" {
		bench.ClientCreator = &fastopaxos.ClientCreator{}
	}

	if *algorithm == "untrustedopaxos" {
		bench.ClientCreator = &untrustedopaxos.ClientCreator{}
	}

	if *load {
		bench.Load()
	} else {
		bench.Run()
	}
}
