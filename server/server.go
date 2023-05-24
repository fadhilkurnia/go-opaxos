package main

import (
	"flag"
	"github.com/ailidani/paxi/fastopaxos"
	"github.com/ailidani/paxi/fastpaxos"
	"github.com/ailidani/paxi/opaxos"
	"github.com/ailidani/paxi/untrustedopaxos"
	"sync"

	"github.com/ailidani/paxi"
	//"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	//"github.com/ailidani/paxi/m2paxos"
	"github.com/ailidani/paxi/paxos"

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

	case "opaxos":
		opaxos.NewReplica(id).RunWithWorker()

	case "fastpaxos":
		fastpaxos.NewReplica(id).RunWithChannel()

	case "fastopaxos":
		fastopaxos.NewReplica(id).RunWithWorker()

	case "untrustedpaxos":
		panic("unimplemented")

	case "untrustedopaxos":
		untrustedopaxos.NewUntrustedServer(id).Run()

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
