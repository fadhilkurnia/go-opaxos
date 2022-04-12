package opaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
	"time"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})

	encoder.Register(P1a{})
	encoder.Register(P1b{})
	encoder.Register(P2a{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
}

// P1a prepare message from proposer to acceptor
type P1a struct {
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// P1b response of prepare (promise message),
// sent from acceptor to proposer
type P1b struct {
	Ballot paxi.Ballot          // sender leader's node-id
	ID     paxi.ID              // sender node-id
	Log    map[int]CommandShare // uncommitted logs
}

// CommandShare combines each secret-shared command with its ballot number
type CommandShare struct {
	Sn  int         // Sn : slot-number
	Ballot paxi.Ballot // Obn: original ballot-number
	Command []byte      // Cmd: the secret-shared command being proposed
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a propose message from proposer to acceptor in Phase 2
// accept message.
type P2a struct {
	Ballot paxi.Ballot    // the proposer's ballot-number
	SSCmds []CommandShare // a batch of CommandShare

	Slot    int
	Command []byte
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%x}", m.Ballot, m.Slot, m.Command)
}

// P2b response of propose message, sent from acceptor to proposer
type P2b struct {
	Ballot paxi.Ballot // the highest ballot-number stored in the acceptor
	Slots  []int       // a batch of slot number being accepted/rejected
	Acks   []bool      // the associated acknowledgements for each slot. true: accepted, false: rejected.

	Slot int
	ID   paxi.ID // the acceptor's id
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 message issued by proposer/leader to persist a previously accepted value
type P3 struct {
	Ballot paxi.Ballot // the proposer's ballot-number
	Slots  []int       // a batch of slot number being committed

	Slot   int
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}

type SSBytesRequest struct {
	*paxi.BytesRequest
	ssTime     int64
	ssCommands [][]byte
}

type SecretSharedCommand struct {
	*paxi.ClientBytesCommand               // pointer to the client's command
	ssTime                   time.Duration // time taken to secret-share the command
	ssCommands               [][]byte      // the secret-shared commands
}
