package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/fadhilkurnia/shamir/csprng"
	"github.com/fadhilkurnia/shamir/krawczyk"
	"github.com/fadhilkurnia/shamir/shamir"
	"time"
)

type SecretSharingWorker struct {
	randomizer   *csprng.CSPRNG
	algorithm    string
	numShares    int
	numThreshold int
}

func (s *SecretSharingWorker) GetT() int {
	return s.numThreshold
}

func NewWorker(algorithm string, numShares, numThreshold int) SecretSharingWorker {
	return SecretSharingWorker{
		csprng.NewCSPRNG(),
		algorithm,
		numShares,
		numThreshold,
	}
}

func (w *SecretSharingWorker) StartProcessingInput(inputChannel chan *paxi.ClientBytesCommand, outputChannel chan *SecretSharedCommand) {
	for req := range inputChannel {
		log.Debugf("processing rawCmd %v", req)
		ss, ssTime, err := w.SecretShareCommand(req.Data)
		if err != nil {
			log.Errorf("failed to do secret sharing: %v", err)
		}
		outputChannel <- &SecretSharedCommand{req, ssTime, ss}
	}
}

func (w *SecretSharingWorker) SecretShareCommand(cmdBytes []byte) ([][]byte, time.Duration, error) {
	var err error
	var secretShares [][]byte

	s := time.Now()

	if w.algorithm == AlgShamir {
		secretShares, err = shamir.SplitWithRandomizer(cmdBytes, w.numShares, w.numThreshold, w.randomizer)
	} else if w.algorithm == AlgSSMS {
		secretShares, err = krawczyk.SplitWithRandomizer(cmdBytes, w.numShares, w.numThreshold, w.randomizer)
	} else {
		secretShares = make([][]byte, w.numShares)
		for i := 0; i < w.numShares; i++ {
			secretShares[i] = make([]byte, len(cmdBytes))
			copy(secretShares[i], cmdBytes)
		}
	}

	ssTime := time.Since(s)

	if err != nil {
		log.Errorf("failed to split secret: %v\n", err)
		return nil, ssTime, err
	}

	return secretShares, ssTime, nil
}
