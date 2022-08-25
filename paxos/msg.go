package paxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
)

func init() {
	encoder.Register(P1a{})
	encoder.Register(P1b{})
	encoder.Register(P2a{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot `msgpack:"b"`
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// CommandsBallot combines a batch of commands with its ballot number
type CommandsBallot struct {
	Commands []paxi.BytesCommand
	Ballot   paxi.Ballot
}

func (cb CommandsBallot) String() string {
	return fmt.Sprintf("b=%v cmd=%v", cb.Ballot, cb.Commands)
}

// P1b promise message
type P1b struct {
	Ballot paxi.Ballot            `msgpack:"b"`
	ID     paxi.ID                `msgpack:"i"`           // from node id
	Log    map[int]CommandsBallot `msgpack:"v,omitempty"` // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	Ballot   paxi.Ballot         `msgpack:"b"`
	Slot     int                 `msgpack:"s"`
	Commands []paxi.BytesCommand `msgpack:"v,omitempty"`
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Commands)
}

// P2b accepted message
type P2b struct {
	Ballot paxi.Ballot `msgpack:"b"`
	ID     paxi.ID     `msgpack:"i"` // from-node id
	Slot   int         `msgpack:"s"`
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 commit message
type P3 struct {
	Ballot   paxi.Ballot         `msgpack:"b"`
	Slot     int                 `msgpack:"s"`
	Commands []paxi.BytesCommand `msgpack:"v,omitempty"`
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}
