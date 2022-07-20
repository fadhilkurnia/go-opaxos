package main

import (
	"flag"
	"github.com/ailidani/paxi/fastopaxos"
	"github.com/ailidani/paxi/fastpaxos"
	"github.com/ailidani/paxi/opaxos"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/abd"
	"github.com/ailidani/paxi/blockchain"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/dynamo"
	"github.com/ailidani/paxi/epaxos"
	"github.com/ailidani/paxi/hpaxos"
	//"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	//"github.com/ailidani/paxi/m2paxos"
	"github.com/ailidani/paxi/paxos"
	//"github.com/ailidani/paxi/paxos_group"
	"github.com/ailidani/paxi/sdpaxos"
	"github.com/ailidani/paxi/vpaxos"
	"github.com/ailidani/paxi/wankeeper"
	//"github.com/ailidani/paxi/wpaxos"
)

var algorithm = flag.String("algorithm", "paxos", "Distributed algorithm")
var id = flag.String("id", "", "ID in format of Zone.Node.")
var simulation = flag.Bool("sim", false, "simulation mode")

var master = flag.String("master", "", "Master address.")

func replica(id paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("node %v starting...", id)

	switch *algorithm {

	case "paxos":
		paxos.NewReplica(id).RunWithChannel()

	case "epaxos":
		epaxos.NewReplica(id).Run()

	case "sdpaxos":
		sdpaxos.NewReplica(id).Run()

	case "wpaxos":
		panic("wpaxos is unimplemented")

	case "abd":
		abd.NewReplica(id).Run()

	case "chain":
		chain.NewReplica(id).Run()

	case "vpaxos":
		vpaxos.NewReplica(id).Run()

	case "wankeeper":
		wankeeper.NewReplica(id).Run()

	case "kpaxos":
		panic("kpaxos is unimplemented")

	case "paxos_groups":
		panic("paxos_groups is unimplemented")

	case "dynamo":
		dynamo.NewReplica(id).Run()

	case "blockchain":
		blockchain.NewMiner(id).Run()

	case "m2paxos":
		panic("m2paxos is unimplemented")

	case "hpaxos":
		hpaxos.NewReplica(id).Run()

	case "opaxos":
		opaxos.NewReplica(id).RunWithWorker()

	case "fastpaxos":
		fastpaxos.NewReplica(id).RunWithChannel()

	case "fastopaxos":
		fastopaxos.NewReplica(id).RunWithWorker()

	default:
		panic("Unknown algorithm")
	}
}

func main() {
	paxi.Init()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		paxi.Simulation()
		for id := range paxi.GetConfig().Addrs {
			n := id
			go replica(n)
		}
		wg.Wait()
	} else {
		replica(paxi.ID(*id))
	}
}
