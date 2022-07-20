package fastopaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
)

func init() {
	encoder.Register(P2a{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
}

// P2a is proposal message broadcast by proposer
type P2a struct {
	Fast      bool        // Fast indicate whether this is fast-proposal or not
	Ballot    paxi.Ballot // the proposer's ballot number
	Slot      int         // the slot to be filled
	Command   []byte      // the proposed secret-share
	OriBallot paxi.Ballot // the original-ballot of the proposed value
	ValID     string      // hash of the secret value
}

// P2b is the response for proposal
type P2b struct {
	Fast      bool        // Fast indicate whether this is a response for fast proposal or not
	ID        paxi.ID     // the (acceptor) sender's ID
	Ballot    paxi.Ballot // the accepted ballot number
	Slot      int         // the proposed slot
	Command   []byte      // the previously accepted secret-share, if any
	OriBallot paxi.Ballot // the original ballot of the accepted secret, if any
	ValID     string      // hash of the secret value
	History   []string
}

// P3 is a commit message (learn)
type P3 struct {
	Ballot    paxi.Ballot
	Slot      int
	Command   []byte
	OriBallot paxi.Ballot
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%x fast=%t}", m.Ballot, m.Slot, m.Command, m.Fast)
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d fast=%t ssval=%d ob=%s}", m.Ballot, m.ID, m.Slot, m.Fast, len(m.Command), m.OriBallot)
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}
