package untrustedopaxos

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

// P1a prepare message from proposer to acceptor
type P1a struct {
	Ballot paxi.Ballot `msgpack:"b"`
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// P1b response of prepare (promise message),
// sent from acceptor to proposer.
type P1b struct {
	Ballot paxi.Ballot          `msgpack:"b"`           // sender leader's node-id
	ID     paxi.ID              `msgpack:"i"`           // sender node-id
	Log    map[int]CommandShare `msgpack:"v,omitempty"` // uncommitted logs
}

// CommandShare combines each secret-shared command with its ballot number
type CommandShare struct {
	Ballot      paxi.Ballot            // the accepted ballot number
	ID          paxi.ID                // the sender's node ID
	OriBallots  []ClientOriginalBallot // a batch of the commands' identifier (client original ballot)
	RawCommands [][]byte               // RawCommands for the command (only used for get req, put req is omitted)
}

func (cs CommandShare) String() string {
	return fmt.Sprintf("Share{bacc=%s bori=%v val=%x}", cs.Ballot, cs.OriBallots, cs.RawCommands)
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b{b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a propose message from proposer to acceptor in Phase 2 (accept message)
type P2a struct {
	Ballot       paxi.Ballot            `msgpack:"b"` // the proposer's ballot-number
	Slot         int                    `msgpack:"s"` // the slot to be filled
	CommandBatch []ClientOriginalBallot `msgpack:"v"` // a batch of client's commands identifier (original ballot)
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%x}", m.Ballot, m.Slot, m.CommandBatch)
}

// P2b response of propose message, sent from acceptor to proposer
type P2b struct {
	Ballot paxi.Ballot `msgpack:"b"` // the highest ballot-number stored in the acceptor
	ID     paxi.ID     `msgpack:"i"` // the acceptor's id
	Slot   int         `msgpack:"s"`
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 message issued by proposer to commit
type P3 struct {
	Ballot       paxi.Ballot            `msgpack:"b"`           // the proposer's ballot-number
	Slot         int                    `msgpack:"s"`           // the slot to be committed
	CommandBatch []ClientOriginalBallot `msgpack:"v,omitempty"` // a batch of client's commands identifier (original ballot)
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}
