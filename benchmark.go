package paxi

import (
	"encoding/binary"
	"github.com/ailidani/paxi/lib"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// DB is general interface implemented by client to call client library
// used for benchmark purposes.
type DB interface {
	Init() error

	// Read Write Write2 and Write3 are synchronous operation (blocking)
	Read(key int) (int, error)
	Write(key, value int) error
	Write2(key, value int) (interface{}, error)
	Write3(key int, value []byte) (interface{}, error)

	Stop() error
}

type DBClientFactory interface {
	Create() (DBClient, error)
}

type DBClient interface {
	Init() error
	Stop() error

	// AsyncRead and AsyncWrite are asynchronous operation (non-blocking).
	// they are used to saturate the server with as many request as possible with limited connection.
	AsyncRead(key []byte, callback func(*CommandReply))
	AsyncWrite(key, value []byte, callback func(*CommandReply))

	// Read and Write are the typical blocking operation
	Read(key []byte) (interface{}, interface{}, error) // return value, metadata, and error
	Write(key, value []byte) (interface{}, error)      // return metadata and error
}

// Bconfig holds all benchmark configuration
type Bconfig struct {
	T                    int     // total number of running time in seconds, using N if 0
	N                    int     // total number of requests
	K                    int     // accessed key space [0,K)
	W                    float64 // write ratio
	Throttle             int     // requests per second throttle, unused if 0
	Concurrency          int     // number of concurrent clients
	Distribution         string  // key-access distribution: order, uniform, conflict, normal, zipfian, exponential.
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	Rounds               int     // (unimplemented) repeat in many rounds sequentially
	RequestRate          float64 // the rate of simulated request in request/second for each client (λ in poisson distribution)

	// conflict distribution
	Conflicts int // percentage of conflicting keys [1,100]
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianS float64 // zipfian s parameter
	ZipfianV float64 // zipfian v parameter

	// exponential distribution
	Lambda float64 // rate parameter
}

// DefaultBConfig returns a default benchmark config
func DefaultBConfig() Bconfig {
	return Bconfig{
		T:                    60,
		N:                    0,
		K:                    1000,
		W:                    0.5,
		Throttle:             0,
		Concurrency:          1,
		Distribution:         "uniform",
		LinearizabilityCheck: true,
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		ZipfianS:             2,
		ZipfianV:             1,
		Lambda:               0.01,
	}
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	Bconfig

	dbClientFactory DBClientFactory
	History         *History

	db DB // (will be deprecated soon)

	rate      *lib.Limiter
	latency   []time.Duration // latency per operation
	startTime time.Time
	//counter   int

	wait sync.WaitGroup // waiting for all generated keys to complete
}

// NewBenchmarkWithDBFactory returns new Benchmark object given implementation of DB interface
func NewBenchmarkWithDBFactory(dbClientFactory DBClientFactory) *Benchmark {
	b := new(Benchmark)
	b.dbClientFactory = dbClientFactory
	b.Bconfig = config.Benchmark
	b.History = NewHistory()
	if b.T == 0 && b.N == 0 {
		log.Fatal("please set benchmark time T or number of operation N")
	}
	return b
}

func NewBenchmark(db DB) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.Bconfig = config.Benchmark
	b.History = NewHistory()
	return b
}

// Load will create all K keys to DB
func (b *Benchmark) Load() {
	latencies := make(chan time.Duration, b.Bconfig.K)
	dbClient, err := b.dbClientFactory.Create()
	if err != nil {
		log.Fatal("failed to initialize db client")
	}

	b.startTime = time.Now()

	// gather the latencies
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := b.Min; j < b.Bconfig.Min+b.Bconfig.K; j++ {
			b.latency = append(b.latency, <-latencies)
		}
	}()

	// issue the write request
	for j := b.Min; j < b.Bconfig.Min+b.Bconfig.K; j++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(j))
		val := make([]byte, 100)
		rand.Read(val)

		reqStartTime := time.Now()
		dbClient.AsyncWrite(key, val, func(reply *CommandReply) {
			if reply.OK {
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

func (b *Benchmark) RunAsyncClient() {
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

	for i := 0; i < b.Bconfig.Concurrency; i++ {
		var limiter *lib.Limiter
		if b.Throttle > 0 {
			limiter = lib.NewLimiter(b.Throttle)
		}

		dbClient, err := b.dbClientFactory.Create()
		if err != nil {
			log.Fatalf("failed to initialize db client: %s", err.Error())
		}

		keyGen := NewKeyGenerator(b)

		// run each client in a separate goroutine
		clientWaiter.Add(1)
		go func(dbClient DBClient, kg *KeyGenerator, rl *lib.Limiter) {
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
				keyValBuff := make([]byte, 100)
				binary.BigEndian.PutUint32(keyValBuff[:4], uint32(key))
				rand.Read(keyValBuff[4:])
				keyBuff := keyValBuff[:4]
				value := keyValBuff[4:]

				op := new(operation)
				requestWaiter.Add(1)

				// issuing write request
				if rand.Float64() < b.W {
					reqStartTime := time.Now()

					dbClient.AsyncWrite(keyBuff, value, func(reply *CommandReply) {
						reqEndTime := time.Now()

						latencies <- reqEndTime.Sub(reqStartTime)

						if !reply.OK {
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
					dbClient.AsyncRead(keyBuff, func(reply *CommandReply) {
						reqEndTime := time.Now()

						latencies <- reqEndTime.Sub(reqStartTime)

						if !reply.OK {
							log.Error("get non ok response from database server")
						}

						op = new(operation)
						op.output = reply.Value
						op.start = reqStartTime.Sub(b.startTime).Nanoseconds()
						op.end = reqEndTime.Sub(b.startTime).Nanoseconds()

						b.History.AddOperation(key, op)
						requestWaiter.Done()
					})
				}

				// wait before issuing next request, if limiter is active
				if limiter != nil {
					limiter.Wait()
				}

				reqCounter++
				// stop if this client already send N request
				if b.N > 0 && reqCounter >= b.N {
					isClientFinished = true
				}

				// stop if the timer is up, non-blocking checking
				if b.T != 0 {
					select {
					case x := <-timesUpFlag:
						isClientFinished = x
					default:
					}
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

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
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

func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	for key := range keys {
		log.Debugf("working ...")
		var s time.Time
		var e time.Time
		var v int
		var err error
		op := new(operation)
		if rand.Float64() < b.W {
			val := make([]byte, 100)
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

type KeyGenerator struct {
	bench   *Benchmark
	zipf    *rand.Zipf
	counter int
}

func NewKeyGenerator(b *Benchmark) *KeyGenerator {
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	zipf := rand.NewZipf(r, b.Bconfig.ZipfianS, b.Bconfig.ZipfianV, uint64(b.Bconfig.K))
	return &KeyGenerator{
		bench:   b,
		zipf:    zipf,
		counter: 0,
	}
}

func (k *KeyGenerator) next() int {
	var key int
	switch k.bench.Distribution {
	case "order":
		k.counter = (k.counter + 1) % k.bench.K
		key = k.counter + k.bench.Min

	case "uniform":
		key = rand.Intn(k.bench.K) + k.bench.Min

	case "conflict":
		if rand.Intn(100) < k.bench.Conflicts {
			key = 0
		} else {
			k.counter = (k.counter + 1) % k.bench.K
			key = k.counter + k.bench.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*k.bench.Sigma + k.bench.Mu)
		for key < 0 {
			key += k.bench.K
		}
		for key > k.bench.K {
			key -= k.bench.K
		}

	case "zipfan":
		key = int(k.zipf.Uint64())

	case "exponential":
		key = int(rand.ExpFloat64() / k.bench.Lambda)

	default:
		log.Fatalf("unknown distribution %s", k.bench.Distribution)
	}

	return key
}
