package paxi

import (
	"bufio"
	"flag"
	"github.com/ailidani/paxi/log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var BenchmarkTracefile = flag.String("trace", "", "location of the tracefile for benchmark purpose")

type traceCommand struct {
	time uint32 // in nanosecond
	op   string // either "read" or "write"
	key  int
	val  []byte
}

func (b *Benchmark) RunClientTracefile() {
	if *BenchmarkTracefile == "" {
		log.Fatalf("tracefile is required")
	}

	// load the tracefile
	tracefile, err := os.Open(*BenchmarkTracefile)
	if err != nil {
		log.Fatalf("failed to open the provided tracefile (%s): %s", *BenchmarkTracefile, err)
	}
	defer tracefile.Close()

	sc := bufio.NewScanner(tracefile)
	commands := make([]traceCommand, 0)
	for sc.Scan() {
		rawCmdStr := sc.Text()
		rawCmdArr := strings.Fields(rawCmdStr)
		if len(rawCmdArr) < 3 {
			log.Warningf("wrong tracefile format: %s", rawCmdStr)
			continue
		}

		cmdTime, terr := strconv.Atoi(rawCmdArr[0])
		if terr != nil {
			log.Warning(terr)
			continue
		}

		cmdOp := rawCmdArr[1]
		if cmdOp != "read" && cmdOp != "write" {
			log.Warningf("wrong tracefile format: %s", rawCmdStr)
			continue
		}

		cmdKey, kerr := strconv.Atoi(rawCmdArr[2])
		if kerr != nil {
			log.Warning(kerr)
			continue
		}

		var cmdVal []byte

		if len(rawCmdArr) > 3 {
			cmdVal = []byte(rawCmdArr[3])
		}

		op := traceCommand{
			time: uint32(cmdTime),
			op:   cmdOp,
			key:  cmdKey,
			val:  cmdVal,
		}

		commands = append(commands, op)
	}

	latencies := make(chan time.Duration, 100_000)

	// gather the latencies from all clients
	latWriterWaiter := sync.WaitGroup{}
	latWriterWaiter.Add(1)
	go func() {
		defer latWriterWaiter.Done()
		for t := range latencies {
			b.latency = append(b.latency, t)
		}
	}()

	// initialize all the clients, limiters, and key generators
	clients := make([]AsyncClient, b.BenchmarkConfig.Concurrency)
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		// initialize client
		c, cerr := b.ClientCreator.CreateAsyncClient()
		if cerr != nil {
			log.Fatalf("failed to initialize db client: %s", cerr.Error())
		}
		clients[i] = c
	}

	clientWaiter := sync.WaitGroup{}
	clientID := 0
	b.startTime = time.Now()
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		dbClient := clients[i]

		// run each client in a separate goroutine
		clientID++
		clientWaiter.Add(1)
		go func(clientID int, dbClient AsyncClient) {
			defer clientWaiter.Done()

			var clientErr error = nil
			isClientFinished := false

			// gather all responses from server
			requestWaiter := sync.WaitGroup{}
			requestWaiter.Add(1)
			clientFinishFlag := make(chan int)
			go func() {
				defer requestWaiter.Done()
				receiverCh := dbClient.GetResponseChannel()
				totalMsgSent := -1
				respCounter := 0

				for respCounter != totalMsgSent {
					select {
					case totalMsgSent = <-clientFinishFlag:
						log.Infof("finish sending, received %d from %d", respCounter, totalMsgSent)
						clientFinishFlag = nil
						break

					case resp := <-receiverCh:
						if resp.Code != CommandReplyOK {
							log.Error("receive non-ok response")
						}

						latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
						respCounter++

						// empty the receiver channel
						nResp := len(receiverCh)
						for nResp > 0 {
							nResp--
							resp = <-receiverCh
							if resp.Code != CommandReplyOK {
								log.Error("receive non-ok response")
							}
							latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
							respCounter++
						}
						break

					}
				}
			}()

			// send command to server until finished
			clientStartTime := time.Now()
			reqCounter := 0
			successReq := 0
			for !isClientFinished {
				cmd := commands[reqCounter]
				var sentTime time.Time

				// issuing write request
				if cmd.op == "write" {
					// SendCommand is a non-blocking method, it returns immediately
					// without waiting for the response
					sentTime = time.Now()
					log.Debugf("sending write command at %v", sentTime.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandPut{
						CommandID: uint32(reqCounter),
						SentAt:    sentTime.UnixNano(),
						Key:       Key(cmd.key),
						Value:     cmd.val,
					})
				} else if cmd.op == "read" { // issuing read request
					sentTime = time.Now()
					log.Debugf("sending read command at %v", sentTime.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandGet{
						CommandID: uint32(reqCounter),
						SentAt:    sentTime.UnixNano(),
						Key:       Key(cmd.key),
					})
				} else {
					log.Error("unknown operation")
				}

				if clientErr != nil {
					log.Errorf("failed to send command %v", clientErr)
				} else {
					successReq++
				}

				// stop if this client already send all commands
				if reqCounter == len(commands) {
					isClientFinished = true
					continue
				}

				reqCounter++
				if reqCounter >= len(commands) {
					isClientFinished = true
					continue
				}

				// we are assuming the inter-command time is always
				// greater than the command execution latency
				nextCmd := commands[reqCounter]
				interCmdTime := int64(nextCmd.time - cmd.time)
				waitTime := interCmdTime - time.Now().Sub(sentTime).Nanoseconds()
				if waitTime > 0 {
					time.Sleep(time.Duration(waitTime))
				}
			}

			clientEndTime := time.Now()
			log.Infof("Client-%d runtime = %v", clientID, clientEndTime.Sub(clientStartTime))
			log.Infof("Client-%d request-rate = %f", clientID, float64(reqCounter)/clientEndTime.Sub(clientStartTime).Seconds())
			clientFinishFlag <- successReq // inform the number of request sent to the response consumer
			requestWaiter.Wait()           // wait until all the requests are responded
		}(clientID, dbClient)
	}

	clientWaiter.Wait()    // wait until all the clients finish accepting responses
	close(latencies)       // closing the latencies channel
	latWriterWaiter.Wait() // wait until all latencies are recorded

	t := time.Now().Sub(b.startTime)

	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	_ = stat.WriteFile("latency")
	stat2 := Statistic(b.encodeTime)
	_ = stat2.WriteFile("encode_time")
}
