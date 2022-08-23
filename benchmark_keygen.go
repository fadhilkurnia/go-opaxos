package paxi

import (
	"github.com/ailidani/paxi/log"
	"math/rand"
	"time"
)

type KeyGenerator struct {
	bench   *Benchmark
	zipf    *rand.Zipf
	counter int
}

func NewKeyGenerator(b *Benchmark) *KeyGenerator {
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	zipf := rand.NewZipf(r, b.BenchmarkConfig.ZipfianS, b.BenchmarkConfig.ZipfianV, uint64(b.BenchmarkConfig.K))
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
