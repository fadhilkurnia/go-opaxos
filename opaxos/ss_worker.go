package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/krawczyk"
	"github.com/fadhilkurnia/shamir/shamir"
	"math/rand"
	"time"
)

type secretSharingWorker struct {
	randomizer *rand.Rand
	algorithm  string
	numShares    int
	numThreshold int
}

func newWorker(algorithm string, numShares, numThreshold int) secretSharingWorker {
	return secretSharingWorker{
		rand.New(rand.NewSource(time.Now().UnixNano())),
		algorithm,
		numShares,
		numThreshold,
	}
}

func (w *secretSharingWorker) startProcessingInput(inputChannel chan *paxi.BytesRequest, outputChannel chan *SSBytesRequest) {
	for {
		req := <-inputChannel
		ss, ssTime, err := w.secretShareCommand(req.Command)
		if err != nil {
			log.Errorf("failed to do secret sharing: %v", err)
		}
		outputChannel <- &SSBytesRequest{req, ssTime, ss}
	}
}

func (w *secretSharingWorker) secretShareCommand(cmdBytes []byte) ([][]byte, int64, error) {
	var err error
	var secretShares [][]byte

	s := time.Now()

	if w.algorithm == AlgShamir {
		secretShares, err = shamir.Split(cmdBytes, w.numShares, w.numThreshold)
	} else if w.algorithm == AlgSSMS {
		secretShares, err = krawczyk.Split(cmdBytes, w.numShares, w.numThreshold)
	} else {
		secretShares = make([][]byte, w.numShares)
		for i := 0; i < w.numShares; i++ {
			secretShares[i] = make([]byte, len(cmdBytes))
			copy(secretShares[i], cmdBytes)
		}
	}

	ssTime := time.Since(s)

	if err != nil {
		log.Errorf("failed to split secret %v\n", err)
		return nil, ssTime.Nanoseconds(), err
	}

	return secretShares, ssTime.Nanoseconds(), nil
}
