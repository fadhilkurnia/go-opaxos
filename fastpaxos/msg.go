package fastpaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
)

func init() {
	//encoder.Register(P1a{})
	//encoder.Register(P1b{})
	encoder.Register(P2a{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
}

// P2a contains proposed value (client's command)
type P2a struct {
	Ballot  paxi.Ballot
	Slot    int
	Command []byte
	Fast    bool // Fast indicate whether this is fast proposal or ordinary phase2 proposal
}

// P2b message to accept or reject a proposal
type P2b struct {
	Ballot paxi.Ballot // the highest *accepted* ballot number
	ID     paxi.ID     // from node id
	Slot   int

	// additional data to respond fast proposal
	Fast      bool        // Fast indicate whether this is response for fast proposal or not
	Acc       bool        // for Fast proposal, Acc is true if the proposal is accepted
	Command   []byte      // Command contains the previously accepted value, if any
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command []byte
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%x fast=%t}", m.Ballot, m.Slot, m.Command, m.Fast)
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d fast=%t acc=%t}", m.Ballot, m.ID, m.Slot, m.Fast, m.Acc)
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}
