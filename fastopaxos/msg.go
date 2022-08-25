package fastopaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
)

func init() {
	encoder.Register(P1a{})
	encoder.Register(P2a{})
	encoder.Register(P2ac{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
}

type SecretShare []byte

type PrivateCommand struct {
	Key      []byte
	ValShare []byte
}

// P1a prepare message from proposer to acceptor
type P1a struct {
	Ballot paxi.Ballot `msgpack:"b"`
}

// P1b response of prepare (promise message),
// sent from acceptor to proposer.
type P1b struct {
	Ballot paxi.Ballot          `msgpack:"b"`           // sender leader's node-id
	ID     paxi.ID              `msgpack:"i"`           // sender node-id
	Log    map[int]CommandShare `msgpack:"v,omitempty"` // uncommitted logs
}

// P2a is proposal message broadcast by proposer
type P2a struct {
	Ballot    paxi.Ballot `msgpack:"b"`           // the proposer's ballot number
	Slot      int         `msgpack:"s"`           // the slot to be filled
	AnyVal    bool        `msgpack:"a"`           // true if the proposer proposes the special ANY value
	OriBallot paxi.Ballot `msgpack:"o"`           // the original-ballot of the proposed value
	Share     []byte      `msgpack:"v,omitempty"` // the proposed secret-share

	// TODO: remove the fields below
	Fast  bool   // Fast indicate whether this is fast-proposal or not
	ValID string // hash of the secret value
}

// P2b is the response for proposal (P2a and P2ac)
type P2b struct {
	Ballot paxi.Ballot // the accepted ballot number
	ID     paxi.ID     // the (acceptor) sender's ID
	Slot   int         // the proposed slot

	// TODO: remove the fields below
	Fast      bool        // Fast indicate whether this is a response for fast proposal or not
	Command   []byte      // the previously accepted secret-share, if any
	OriBallot paxi.Ballot // the original ballot of the accepted secret, if any
	ValID     string      // hash of the secret value
	History   []string
}

// P2ac is the client's value proposal which is sent directly by client, not proposer
type P2ac struct {
	// TODO: should the client opportunistically provide the slot?
	// TODO: or the acceptors opportunistically pick the slot?
	OriBallot paxi.Ballot `msgpack:"o"` // the original-ballot / secret value identifier
	Share     SecretShare `msgpack:"v"` // a single share of the proposed secret value
}

type DirectCommand struct {
	CommandID    paxi.Ballot
	CommandShare SecretShare

	SentAt int64
}

type DirectCommandReply struct {
	Code byte
	Slot int
	Data []byte

	SentAt   int64
	ProcTime int64
}

type P2bc struct {
	Slot      int         `msgpack:"o"` // Slot is the picked slot by the acceptor
	OriBallot paxi.Ballot `msgpack:"o"` // the original-ballot / secret value identifier
	Share     SecretShare `msgpack:"v"` // a single share of the proposed secret value
}

// P3 is a commit message
type P3 struct {
	Ballot    paxi.Ballot
	Slot      int
	Command   []byte
	OriBallot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d val=%x}", m.Ballot, m.Slot, m.Share)
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d fast=%t ssval=%d ob=%s}", m.Ballot, m.ID, m.Slot, m.Fast, len(m.Command), m.OriBallot)
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}

// CommandShare combines each secret-shared command with its ballot number
type CommandShare struct {
	Ballot    paxi.Ballot // the accepted ballot number
	OriBallot paxi.Ballot // the original ballot-number
	Share     []byte      // the secret-share of value being proposed
	ID        paxi.ID

	// TODO: remove this, now they are used for debugging
	History []string
	ValID   string
}
