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
// sent from acceptor to proposer. If the P1b message comes from
// a trusted node, the Log contains clear command, not secret-shared command.
type P1b struct {
	Ballot paxi.Ballot          // sender leader's node-id
	ID     paxi.ID              // sender node-id
	Log    map[int]CommandShare // uncommitted logs
}

// CommandShare combines each secret-shared command with its ballot number
type CommandShare struct {
	Ballot      paxi.Ballot   // the accepted ballot number
	OriBallot   paxi.Ballot   // the original ballot-number
	SharesBatch []SecretShare // the secret-share of value being proposed, in a batch
	ID          paxi.ID       // the sender's node ID
}

func (cs CommandShare) String() string {
	return fmt.Sprintf("Share{bacc=%s bori=%s val=%x}", cs.Ballot, cs.OriBallot, cs.SharesBatch)
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b{b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a propose message from proposer to acceptor in Phase 2 (accept message)
type P2a struct {
	Ballot      paxi.Ballot   // the proposer's ballot-number
	Slot        int           // the slot to be filled
	SharesBatch []SecretShare // a batch of secret-shares of command
	OriBallot   paxi.Ballot   // the value's original ballot
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%x}", m.Ballot, m.Slot, m.SharesBatch)
}

// P2b response of propose message, sent from acceptor to proposer
type P2b struct {
	Ballot paxi.Ballot // the highest ballot-number stored in the acceptor
	ID     paxi.ID     // the acceptor's id
	Slot   int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 message issued by proposer to commit
type P3 struct {
	Ballot       paxi.Ballot         // the proposer's ballot-number
	Slot         int                 // the slot to be committed
	SharesBatch  []SecretShare       // a batch of secret-shares of command
	OriBallot    paxi.Ballot         // the value's original ballot
	CommandBatch []paxi.BytesCommand // a batch of clear command for fellow trusted proposer
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}

type SecretShare []byte

type SecretSharedCommand struct {
	*paxi.ClientCommand               // pointer to the client's command
	SSTime              time.Duration // time taken to secret-share the command
	Shares              []SecretShare // the N secret-shares of the command
}
