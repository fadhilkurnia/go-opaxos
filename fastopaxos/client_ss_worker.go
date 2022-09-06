package fastopaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/opaxos"
)

// RawDirectCommandBallot combines client's command with the ballot number
// which is used as the command identifier. This struct is used as the input
// for the secret-sharing worker
type RawDirectCommandBallot struct {
	Slot           int
	OriginalBallot Ballot
	Command        paxi.SerializableCommand
}

type BroadcastDirectCommand struct {
	DirectCommands []*DirectCommand
}

type ClientSSWorker struct {
	opaxos.SecretSharingWorker

	coordinatorID paxi.ID
	nodeIDs       []paxi.ID
}

func NewWorker(algorithm string, numThreshold int, coordID paxi.ID, nodeIDs []paxi.ID) ClientSSWorker {
	numShares := len(nodeIDs)
	return ClientSSWorker{
		SecretSharingWorker: opaxos.NewWorker(algorithm, numShares, numThreshold),
		coordinatorID:       coordID,
		nodeIDs:             nodeIDs,
	}
}

func (w *ClientSSWorker) StartProcessingInput(inputChannel chan *RawDirectCommandBallot, outputChannel chan *BroadcastDirectCommand) {
	for cmd := range inputChannel {
		cmdBuff := cmd.Command.Serialize()
		ss, _, err := w.SecretShareCommand(cmdBuff)
		if err != nil {
			log.Errorf("failed to do secret sharing: %v", err)
		}

		// prepare DirectCommand for all the nodes
		directCmds := make([]*DirectCommand, len(w.nodeIDs))
		for i, id := range w.nodeIDs {
			directCmds[i] = &DirectCommand{
				Slot:      cmd.Slot,
				OriBallot: cmd.OriginalBallot,
				Share:     SecretShare(ss[i]),
				Command:   nil,
			}
			if w.coordinatorID == id {
				(*directCmds[i]).Command = cmdBuff
			}
		}

		outputChannel <- &BroadcastDirectCommand{
			DirectCommands: directCmds,
		}
	}
}
