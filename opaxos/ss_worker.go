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

func NewWorker(algorithm string, numShares, numThreshold int) SecretSharingWorker {
	return SecretSharingWorker{
		csprng.NewCSPRNG(),
		algorithm,
		numShares,
		numThreshold,
	}
}

func (w *SecretSharingWorker) StartProcessingInput(inputChannel chan *paxi.ClientCommand, outputChannel chan *SecretSharedCommand) {
	for cmd := range inputChannel {
		log.Debugf("processing rawCmd %s", cmd)
		ss, ssTime, err := w.SecretShareCommand(cmd.RawCommand)
		if err != nil {
			log.Errorf("failed to do secret sharing: %v", err)
		}
		outputChannel <- &SecretSharedCommand{
			ClientCommand: cmd,
			SSTime:        ssTime,
			Shares:        ss,
		}
	}
}

func (w *SecretSharingWorker) SecretShareCommand(cmdBytes []byte) ([]SecretShare, time.Duration, error) {
	var err error
	var secretShareBytes [][]byte
	var secretShares []SecretShare
	var ssTime time.Duration

	switch w.algorithm {
	case SSAlgorithmShamir:
		s := time.Now()
		secretShareBytes, err = shamir.SplitWithRandomizer(cmdBytes, w.numShares, w.numThreshold, w.randomizer)
		ssTime = time.Since(s)

	case SSAlgorithmSSMS:
		s := time.Now()
		secretShareBytes, err = krawczyk.SplitWithRandomizer(cmdBytes, w.numShares, w.numThreshold, w.randomizer)
		ssTime = time.Since(s)

	default:
		// the default is to copy the original cmdBytes into N pieces
		s := time.Now()
		secretShareBytes = make([][]byte, w.numShares)
		for i := 0; i < w.numShares; i++ {
			secretShareBytes[i] = make([]byte, len(cmdBytes))
			copy(secretShareBytes[i], cmdBytes)
		}
		ssTime = time.Since(s)

	}

	secretShares = make([]SecretShare, w.numShares)
	if err != nil {
		log.Errorf("failed to split secret: %v\n", err)
		return nil, ssTime, err
	}

	// cast [][]byte to []SecretShare
	for i := 0; i < w.numShares; i++ {
		secretShares[i] = secretShareBytes[i]
	}

	return secretShares, ssTime, nil
}
