package paxi

import (
	"github.com/ailidani/paxi/lib"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

type BenchmarkClientCreator interface {
	CreateClient() (Client, error)
	CreateAsyncClient() (AsyncClient, error)
	CreateCallbackClient() (AsyncCallbackClient, error)
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	BenchmarkConfig
	ClientCreator BenchmarkClientCreator
	History       *History

	db DB // TODO: deprecate this, by default we do the benchmark in async with AsyncClient interface

	latency    []time.Duration // latency per operation from all clients
	encodeTime []time.Duration // encoding time
	startTime  time.Time

	wait sync.WaitGroup // waiting for all generated keys to complete
}

func NewBenchmark() *Benchmark {
	b := new(Benchmark)
	//b.db = db
	b.BenchmarkConfig = config.Benchmark
	b.History = NewHistory()
	if b.T == 0 && b.N == 0 {
		log.Fatal("please set benchmark time T or number of operation N")
	}
	return b
}

// Load will create all K keys to DB
func (b *Benchmark) Load() {
	latencies := make(chan time.Duration, b.BenchmarkConfig.K)
	dbClient, err := b.ClientCreator.CreateCallbackClient()
	if err != nil {
		log.Fatal("failed to initialize db client")
	}

	b.startTime = time.Now()

	// gather the latencies
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := b.Min; j < b.BenchmarkConfig.Min+b.BenchmarkConfig.K; j++ {
			b.latency = append(b.latency, <-latencies)
		}
	}()

	// issue the write request
	for j := b.Min; j < b.BenchmarkConfig.Min+b.BenchmarkConfig.K; j++ {
		val := make([]byte, 100)
		rand.Read(val)

		reqStartTime := time.Now()
		dbClient.Put(Key(j), val, func(reply *CommandReply) {
			if reply.Code == CommandReplyOK {
				latencies <- time.Since(reqStartTime)
			} else {
				log.Error("get non ok response from database server")
			}
		})
	}

	// wait until all the latencies are gathered
	wg.Wait()
	close(latencies)

	t := time.Since(b.startTime)
	stat := Statistic(b.latency)
	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	if *ClientAction == "pipeline" {
		b.RunPipelineClient()
		*ClientIsStateful = false
		return
	}
	if *ClientAction == "callback" {
		*ClientIsStateful = true
		b.RunCallbackClient()
		return
	}
	if *ClientAction == "throughput_collector" {
		*ClientIsStateful = false
		b.RunThroughputCollectorClients()
		return
	}
	if *ClientAction == "tracefile" {
		*ClientIsStateful = false
		b.RunClientTracefile()
		return
	}
	if *ClientAction == "tpcc" {
		*ClientIsStateful = false
		b.RunClientWithEmulatedCommands()
		return
	}

	// handle the default --client_action=block
	*ClientIsStateful = false
	b.RunBlockingClient()
	return
}

// RunReadWriteClient uses the read and write interface implemented by the database.
// It is the original implementation in Paxi. The read and write interface is mainly used
// for cmd (Paxi's cmd client).
func (b *Benchmark) RunReadWriteClient() {
	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	b.latency = make([]time.Duration, 0)
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, GetConfig().ChanBufferSize)
	defer close(latencies)
	go b.collect(latencies)

	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}

	b.db.Init()
	keygen := NewKeyGenerator(b)
	b.startTime = time.Now()
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T))
	loop:
		for {
			select {
			case <-timer.C:
				break loop
			default:
				b.wait.Add(1)
				keys <- keygen.next()
			}
		}
	} else {
		for i := 0; i < b.N; i++ {
			b.wait.Add(1)
			keys <- keygen.next()
		}
		b.wait.Wait()
	}
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	stat.WriteFile("latency")
	b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

func (b *Benchmark) RunCallbackClient() {
	latencies := make(chan time.Duration, 100_000)
	b.startTime = time.Now()

	// gather the latencies from all clients
	latWriterWaiter := sync.WaitGroup{}
	latWriterWaiter.Add(1)
	go func() {
		defer latWriterWaiter.Done()
		for t := range latencies {
			b.latency = append(b.latency, t)
		}
	}()

	clientWaiter := sync.WaitGroup{}

	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		var limiter *lib.Limiter
		if b.Throttle > 0 {
			limiter = lib.NewLimiter(b.Throttle)
		}

		dbClient, err := b.ClientCreator.CreateCallbackClient()
		if err != nil {
			log.Fatalf("failed to initialize db client: %s", err.Error())
		}

		keyGen := NewKeyGenerator(b)

		// run each client in a separate goroutine
		clientWaiter.Add(1)
		go func(dbClient AsyncCallbackClient, kg *KeyGenerator, rl *lib.Limiter) {
			defer clientWaiter.Done()

			isClientFinished := false
			timesUpFlag := make(chan bool, 1)

			if b.T != 0 {
				go func() {
					time.Sleep(time.Duration(b.T) * time.Second)
					timesUpFlag <- true
				}()
			}

			reqCounter := 0
			requestWaiter := sync.WaitGroup{}
			for !isClientFinished {
				key := kg.next()
				value := make([]byte, b.Size)
				rand.Read(value)

				op := new(operation)
				requestWaiter.Add(1)

				// issuing write request
				if rand.Float64() < b.W {
					reqStartTime := time.Now()

					dbClient.Put(Key(key), value, func(reply *CommandReply) {
						reqEndTime := time.Now()

						latencies <- reqEndTime.Sub(reqStartTime)

						if reply.Code == CommandReplyErr {
							log.Error("get non ok response from database server")
						}

						op = new(operation)
						op.input = key
						op.start = reqStartTime.Sub(b.startTime).Nanoseconds()
						op.end = reqEndTime.Sub(b.startTime).Nanoseconds()

						b.History.AddOperation(key, op)
						requestWaiter.Done()
					})

				} else { // issuing read request
					reqStartTime := time.Now()
					dbClient.Get(Key(key), func(reply *CommandReply) {
						reqEndTime := time.Now()

						latencies <- reqEndTime.Sub(reqStartTime)

						if reply.Code == CommandReplyErr {
							log.Error("get non ok response from database server")
						}

						op = new(operation)
						op.output = reply.Data
						op.start = reqStartTime.Sub(b.startTime).Nanoseconds()
						op.end = reqEndTime.Sub(b.startTime).Nanoseconds()

						b.History.AddOperation(key, op)
						requestWaiter.Done()
					})
				}

				reqCounter++

				// stop if this client already send N request
				if b.N > 0 && reqCounter >= b.N {
					isClientFinished = true
					continue
				}

				// stop if the timer is up, non-blocking checking
				if b.T != 0 {
					select {
					case _ = <-timesUpFlag:
						isClientFinished = true
						continue
					default:
					}
				}

				// wait before issuing next request, if limiter is active
				if limiter != nil {
					limiter.Wait()
				}
			}

			requestWaiter.Wait() // wait until all the requests are responded
		}(dbClient, keyGen, limiter)

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
	_ = b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

// RunPipelineClient simple client, we do not gather history
func (b *Benchmark) RunPipelineClient() {
	latencies := make(chan time.Duration, 1_000_000)

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
	limiters := make([]*lib.Limiter, b.BenchmarkConfig.Concurrency)
	keyGenerators := make([]*KeyGenerator, b.BenchmarkConfig.Concurrency)
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		// initialize client
		c, err := b.ClientCreator.CreateAsyncClient()
		if err != nil {
			log.Fatalf("failed to initialize db client: %s", err.Error())
		}
		clients[i] = c

		// initialize limiter
		if b.Throttle > 0 {
			limiters[i] = lib.NewLimiter(b.Throttle)
		}

		// initialize key generator
		keyGenerators[i] = NewKeyGenerator(b)
	}

	clientWaiter := sync.WaitGroup{}
	clientID := 0
	b.startTime = time.Now()
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		dbClient := clients[i]
		limiter := limiters[i]
		keyGen := keyGenerators[i]

		// run each client in a separate goroutine
		clientID++
		clientWaiter.Add(1)
		go func(clientID int, dbClient AsyncClient, kg *KeyGenerator, rl *lib.Limiter) {
			defer clientWaiter.Done()

			isClientFinished := false
			var clientErr error = nil
			timesUpFlag := make(chan bool)

			if b.T != 0 {
				go func() {
					time.Sleep(time.Duration(b.T) * time.Second)
					timesUpFlag <- true
				}()
			}

			// gather all responses from server
			requestWaiter := sync.WaitGroup{}
			requestWaiter.Add(1)
			clientFinishFlag := make(chan int)
			go func() {
				defer requestWaiter.Done()
				receiverCh := clients[clientID-1].GetResponseChannel()
				totalMsgSent := -1
				respCounter := 0

				if receiverCh == nil {
					log.Errorf("client's receiver channel is null")
				}

				for respCounter != totalMsgSent {
					select {
					case totalMsgSent = <-clientFinishFlag:
						log.Infof("finish sending, received %d from %d, incoming response: %d", respCounter, totalMsgSent, len(receiverCh))
						clientFinishFlag = nil

						break

					case resp := <-receiverCh:
						latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
						respCounter++

						// empty the receiver channel
						nResp := len(receiverCh)
						for nResp > 0 {
							nResp--
							resp = <-receiverCh
							latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
							respCounter++
						}
						break

					}
				}
			}()

			// wait before starting a client, reducing the chance of multiple clients start
			// at the same time
			if limiter != nil {
				limiter.Wait()
			}

			// send command to server until finished
			clientStartTime := time.Now()
			reqCounter := 0
			for !isClientFinished {
				key := kg.next()
				value := make([]byte, b.Size)
				rand.Read(value)

				// issuing write request
				if rand.Float64() < b.W {
					// SendCommand is a non-blocking method, it returns immediately
					// without waiting for the response
					now := time.Now()
					clientErr = dbClient.SendCommand(&DBCommandPut{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
						Value:     value,
					})
				} else { // issuing read request
					now := time.Now()
					clientErr = dbClient.SendCommand(&DBCommandGet{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
					})
				}

				if clientErr == nil {
					reqCounter++
				} else {
					log.Errorf("failed to send command %v", clientErr)
				}

				// stop if this client already send N requests
				if b.N > 0 && reqCounter == b.N {
					isClientFinished = true
					continue
				}

				// stop if the timer is up, non-blocking checking
				if b.T != 0 {
					select {
					case _ = <-timesUpFlag:
						isClientFinished = true
						log.Debugf("stopping client-%d", clientID)
						continue
					default:
					}
				}

				// wait before issuing next request, if limiter is active
				if limiter != nil {
					limiter.Wait()
				}
			}

			clientEndTime := time.Now()
			log.Infof("Client-%d runtime = %v", clientID, clientEndTime.Sub(clientStartTime))
			log.Infof("Client-%d request-rate = %f", clientID, float64(reqCounter)/clientEndTime.Sub(clientStartTime).Seconds())
			clientFinishFlag <- reqCounter // inform the number of request sent to the response consumer
			requestWaiter.Wait()           // wait until all the requests are responded
		}(clientID, dbClient, keyGen, limiter)
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
}

// RunBlockingClient initiates clients that do one outstanding request at a time.
// It uses pipelined client interface under the hood.
func (b *Benchmark) RunBlockingClient() {
	latencies := make(chan time.Duration, 100_000)
	encodeTimes := make(chan time.Duration, 100_000)

	// gather the latencies from all clients
	latWriterWaiter := sync.WaitGroup{}
	latWriterWaiter.Add(1)
	go func() {
		defer latWriterWaiter.Done()
		for t := range latencies {
			b.latency = append(b.latency, t)
		}
	}()
	latWriterWaiter.Add(1)
	go func() {
		defer latWriterWaiter.Done()
		for t := range encodeTimes {
			b.encodeTime = append(b.encodeTime, t)
		}
	}()

	// initialize all the clients, limiters, and key generators
	clients := make([]AsyncClient, b.BenchmarkConfig.Concurrency)
	limiters := make([]*lib.Limiter, b.BenchmarkConfig.Concurrency)
	keyGenerators := make([]*KeyGenerator, b.BenchmarkConfig.Concurrency)
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		// initialize client
		c, err := b.ClientCreator.CreateAsyncClient()
		if err != nil {
			log.Fatalf("failed to initialize db client: %s", err.Error())
		}
		clients[i] = c

		// initialize limiter
		if b.Throttle > 0 {
			limiters[i] = lib.NewLimiter(b.Throttle)
		}

		// initialize key generator
		keyGenerators[i] = NewKeyGenerator(b)
	}

	clientWaiter := sync.WaitGroup{}
	clientID := 0
	b.startTime = time.Now()
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		dbClient := clients[i]
		limiter := limiters[i]
		keyGen := keyGenerators[i]

		// run each client in a separate goroutine
		clientID++
		clientWaiter.Add(1)
		go func(clientID int, dbClient AsyncClient, kg *KeyGenerator, rl *lib.Limiter) {
			defer clientWaiter.Done()

			var clientErr error = nil
			isClientFinished := false
			timesUpFlag := make(chan bool)
			receiverCh := dbClient.GetResponseChannel()

			if b.T != 0 {
				go func() {
					time.Sleep(time.Duration(b.T) * time.Second)
					timesUpFlag <- true
				}()
			}

			// send command to server until finished
			clientStartTime := time.Now()
			reqCounter := 0
			for !isClientFinished {
				key := kg.next()
				value := make([]byte, b.Size)
				rand.Read(value)

				// issuing write request
				if rand.Float64() < b.W {
					// SendCommand is a non-blocking method, it returns immediately
					// without waiting for the response
					now := time.Now()
					log.Debugf("sending write command at %v", now.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandPut{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
						Value:     value,
					})
				} else { // issuing read request
					now := time.Now()
					log.Debugf("sending read command at %v", now.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandGet{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
					})
				}

				if clientErr == nil {
					reqCounter++
				} else {
					log.Errorf("failed to send command %v", clientErr)
				}

				// wait for the response
				resp := <-receiverCh
				if resp.Code == CommandReplyOK {
					latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
					if ssTimeRaw, exist := resp.Metadata[MetadataSecretSharingTime]; exist {
						if ssTimeInt, ok := ssTimeRaw.(int64); ok {
							encodeTimes <- time.Duration(ssTimeInt)
						} else {
							log.Error("encoding (secret-sharing) time must be an int64 time.Duration")
						}
					}
				} else {
					log.Error("receive non-ok response")
				}

				// stop if this client already send N requests
				if b.N > 0 && reqCounter == b.N {
					isClientFinished = true
					continue
				}

				// stop if the timer is up, non-blocking checking
				if b.T != 0 {
					select {
					case _ = <-timesUpFlag:
						isClientFinished = true
						continue
					default:
					}
				}

				// wait before issuing next request, if limiter is active
				if limiter != nil {
					limiter.Wait()
				}
			}

			clientEndTime := time.Now()
			log.Infof("Client-%d runtime = %v", clientID, clientEndTime.Sub(clientStartTime))
			log.Infof("Client-%d request-rate = %f", clientID, float64(reqCounter)/clientEndTime.Sub(clientStartTime).Seconds())
		}(clientID, dbClient, keyGen, limiter)
	}

	clientWaiter.Wait()    // wait until all the clients finish accepting responses
	close(latencies)       // closing the latencies channel
	close(encodeTimes)     // closing encodeTimes channel
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

func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	for key := range keys {
		var s time.Time
		var e time.Time
		var v int
		var err error
		op := new(operation)
		if rand.Float64() < b.W {
			val := make([]byte, b.Size)
			rand.Read(val)
			s = time.Now()
			ret, errx := b.db.Write3(key, val)
			err = errx
			e = time.Now()
			op.input = val
			op.output = ret
		} else {
			s = time.Now()
			v, err = b.db.Read(key)
			e = time.Now()
			op.output = v
		}
		op.start = s.Sub(b.startTime).Nanoseconds()
		if err == nil {
			op.end = e.Sub(b.startTime).Nanoseconds()
			result <- e.Sub(s)
		} else {
			op.end = math.MaxInt64
			log.Error(err)
		}
		b.History.AddOperation(key, op)
	}
}

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done()
	}
}

// RunThroughputCollectorClients runs clients that periodically gather the throughput
// every 1 second, then dump the result in an external file. The implementation
// is based on the RunBlockingClient().
func (b *Benchmark) RunThroughputCollectorClients() {
	latencies := make(chan time.Duration, 100_000)

	// gather the latencies from all the clients
	// collect the number of response every 1 second (throughput)
	latWriterWaiter := sync.WaitGroup{}
	latWriterWaiter.Add(1)
	go func() {
		defer latWriterWaiter.Done()
		isLatencyChannelOpen := true
		secondID := 0
		for isLatencyChannelOpen {
			time.Sleep(1 * time.Second)
			numResp := len(latencies)
			log.Infof("At second-%d, throughput: %d reqs/s", secondID, numResp)
			for numResp > 0 {
				respLat := <-latencies
				b.latency = append(b.latency, respLat)
				numResp--
			}

			// exit the loop if the latencies channel is already closed
			select {
			case respLat, more := <-latencies:
				if more {
					b.latency = append(b.latency, respLat)
					break
				}
				// the channel is closed
				log.Debugf("%d exit the latency & throughput gatherer goroutine...", len(latencies))
				isLatencyChannelOpen = false
			default:
			}
			secondID++
		}
	}()

	// initialize all the clients, limiters, and key generators
	clients := make([]AsyncClient, b.BenchmarkConfig.Concurrency)
	limiters := make([]*lib.Limiter, b.BenchmarkConfig.Concurrency)
	keyGenerators := make([]*KeyGenerator, b.BenchmarkConfig.Concurrency)
	for i := 0; i < b.BenchmarkConfig.Concurrency; i++ {
		// initialize client
		c, err := b.ClientCreator.CreateAsyncClient()
		if err != nil {
			log.Fatalf("failed to initialize db client: %s", err.Error())
		}
		clients[i] = c

		// initialize limiter
		if b.Throttle > 0 {
			limiters[i] = lib.NewLimiter(b.Throttle)
		}

		// initialize key generator
		keyGenerators[i] = NewKeyGenerator(b)
	}

	// run all the clients, set the number of clients from "concurrency"
	// in the configuration file
	clientWaiter := sync.WaitGroup{}
	b.startTime = time.Now()
	for cid := 0; cid < b.BenchmarkConfig.Concurrency; cid++ {
		db := clients[cid]
		limiter := limiters[cid]
		kg := keyGenerators[cid]

		// run clients, each in a separate goroutines
		clientWaiter.Add(1)
		go func(clientID int, dbClient AsyncClient, keyGenerator *KeyGenerator, rateLimiter *lib.Limiter) {
			defer clientWaiter.Done()

			var clientErr error = nil
			isClientFinished := false
			timesUpFlag := make(chan bool)
			receiverCh := dbClient.GetResponseChannel()

			if b.T != 0 {
				go func() {
					time.Sleep(time.Duration(b.T) * time.Second)
					timesUpFlag <- true
				}()
			}

			// send command to server until finished
			clientStartTime := time.Now()
			reqCounter := 0
			for !isClientFinished {
				key := kg.next()
				value := make([]byte, b.Size)
				rand.Read(value)

				// issuing write request
				if rand.Float64() < b.W {
					// SendCommand is a non-blocking method, it returns immediately
					// without waiting for the response
					now := time.Now()
					log.Debugf("sending write command at %v", now.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandPut{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
						Value:     value,
					})
				} else { // issuing read request
					now := time.Now()
					log.Debugf("sending read command at %v", now.UnixNano())
					clientErr = dbClient.SendCommand(&DBCommandGet{
						CommandID: uint32(reqCounter),
						SentAt:    now.UnixNano(),
						Key:       Key(key),
					})
				}

				if clientErr == nil {
					reqCounter++
				} else {
					log.Errorf("failed to send command %v", clientErr)
				}

				requestTimeoutFlag := make(chan bool)
				go func() {
					time.Sleep(1 * time.Second)
					requestTimeoutFlag <- true
				}()

				// stop if the timer is up or N requests is reached
				if b.T != 0 {
					select {
					case _ = <-timesUpFlag:
						isClientFinished = true
						continue
					case resp := <-receiverCh:
						if resp.Code == CommandReplyOK {
							latencies <- time.Now().Sub(time.Unix(0, resp.SentAt))
						} else {
							log.Errorf("receive non-ok response")
						}
						// stop if this client already send N requests
						if b.N > 0 && reqCounter == b.N {
							isClientFinished = true
							break
						}
					case _ = <- requestTimeoutFlag:
						// forget this request
					}
				} else {
					log.Fatalf("throughput_collector require time T")
				}

				// wait before issuing next request, if limiter is active
				if limiter != nil {
					limiter.Wait()
				}
			}

			clientEndTime := time.Now()
			log.Infof("Client-%d runtime = %v", clientID, clientEndTime.Sub(clientStartTime))
			log.Infof("Client-%d request-rate = %f", clientID, float64(reqCounter)/clientEndTime.Sub(clientStartTime).Seconds())

		}(cid, db, kg, limiter)
	}

	clientWaiter.Wait()    // wait until all the clients finish accepting responses
	close(latencies)       // closing the latencies channel
	latWriterWaiter.Wait() // wait until all latencies are recorded in b

	t := time.Now().Sub(b.startTime)

	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	_ = stat.WriteFile("latency")
}
