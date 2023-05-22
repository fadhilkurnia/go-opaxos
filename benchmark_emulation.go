package paxi

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ailidani/paxi/log"
	"os"
	"time"
)

// flags usage: --client_action=tpcc
// flags usage: --req_tracefile=<tracefile_loc>

var IsReqTracefile = flag.String("req-tracefile", "",
	"tracefile containing request id, the execution time, and the list of state diff, note that"+
		"this is different with `tracefile` option originally implemented in Paxi")

func (b *Benchmark) RunClientWithEmulatedCommands() {
	if *ClientAction != "tpcc" || *IsReqTracefile == "" {
		log.Fatalf("please use both '-client_action tpcc' and '-req_tracefile <trace_location>' arguments!")
	}

	emulatedCommands, err := ReadEmulatedCommandsFromTracefile(*IsReqTracefile)
	if err != nil {
		log.Fatalf("failed to read the csv tracefile: %s", err)
	}

	c, err := b.ClientCreator.CreateAsyncClient()
	if err != nil {
		log.Fatalf("failed to initialize a tpcc client emulator: %s", err.Error())
	}
	receiverCh := c.GetResponseChannel()

	// prepare output file
	file, err := os.Create("tpcc_latency.csv")
	if err != nil {
		log.Fatalf("failed to create file: %s", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	fmt.Fprintln(w, "algorithm,tx_type,latency(ms)")

	// send first command to initiate phase 1
	cmd := emulatedCommands[16730]
	log.Infof("first - sending command %s", cmd.CommandType)
	sendErr := c.SendCommand(&EmulatedCommand{
		CommandID:   uint32(cmd.CommandID),
		CommandType: cmd.CommandType,
		Queries:     cmd.Queries,
		SentAt:      time.Now().UnixNano(),
	})
	if sendErr != nil {
		log.Error(sendErr)
	}
	// wait for response
	resp := <-receiverCh
	firstLatency := time.Now().Sub(time.Unix(0, resp.SentAt))
	log.Infof("  latency %v", firstLatency)

	log.Infof("second - sending command %s", cmd.CommandType)
	sendErr = c.SendCommand(&EmulatedCommand{
		CommandID:   uint32(cmd.CommandID),
		CommandType: cmd.CommandType,
		Queries:     cmd.Queries,
		SentAt:      time.Now().UnixNano(),
	})
	if sendErr != nil {
		log.Error(sendErr)
	}
	resp = <-receiverCh
	firstLatency = time.Now().Sub(time.Unix(0, resp.SentAt))
	log.Infof("  latency %v", firstLatency)

	// start emulating client. Here, we are assuming complete command IDs
	numCmd := uint32(len(emulatedCommands))
	for cmdID := uint32(0); cmdID < numCmd; cmdID++ {
		cmd := emulatedCommands[cmdID]

		// send tpcc command
		log.Infof("sending command %s", cmd.CommandType)
		sendErr := c.SendCommand(&EmulatedCommand{
			CommandID:   uint32(cmdID),
			CommandType: cmd.CommandType,
			Queries:     cmd.Queries,
			SentAt:      time.Now().UnixNano(),
		})
		if sendErr != nil {
			log.Error(sendErr)
		}

		// wait for response
		resp := <-receiverCh
		latency := time.Now().Sub(time.Unix(0, resp.SentAt))
		log.Infof("  latency %v", latency)
		fmt.Fprintf(w, "paxos,%s,%f\n", cmd.CommandType, float64(latency.Nanoseconds())/1000000.0)
	}

}
