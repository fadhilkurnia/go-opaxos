package paxi

// BenchmarkConfig holds all benchmark configuration
type BenchmarkConfig struct {
	T                    int     // total number of running time in seconds, using N if 0
	N                    int     // total number of requests
	K                    int     // accessed key space [0,K)
	W                    float64 // write ratio
	Size                 int     // the size of value written in bytes, the key is always a 4 bytes integer
	Throttle             int     // requests per second throttle, unused if 0. the rate of simulated request in request/second for each client (λ in poisson distribution)
	Concurrency          int     // number of concurrent clients
	Distribution         string  // key-access distribution: order, uniform, conflict, normal, zipfian, exponential.
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	Rounds               int     // (unimplemented) repeat in many rounds sequentially
	BufferSize           int     // buffer size for the benchmark's client

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
func DefaultBConfig() BenchmarkConfig {
	return BenchmarkConfig{
		T:                    60,
		N:                    0,
		K:                    1000,
		W:                    0.5,
		Size:                 50,
		Throttle:             0,
		Concurrency:          1,
		Distribution:         "uniform",
		LinearizabilityCheck: true,
		BufferSize:           1024,
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
