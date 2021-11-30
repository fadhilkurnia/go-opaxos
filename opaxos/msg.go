package opaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
	"time"
)

func init() {
	gob.Register(PrepareRequest{})
	gob.Register(PrepareResponse{})
	gob.Register(ProposeRequest{})
	gob.Register(ProposeResponse{})
	gob.Register(CommitRequest{})
}

// PrepareRequest prepare message from proposer to acceptor
type PrepareRequest struct {
	Ballot paxi.Ballot
}

func (m PrepareRequest) String() string {
	return fmt.Sprintf("PrepareRequest {b=%v}", m.Ballot)
}

// PrepareResponse response of prepare (promise message),
// sent from acceptor to proposer
type PrepareResponse struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

// CommandBallot combines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (m PrepareResponse) String() string {
	return fmt.Sprintf("PrepareResponse {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// ProposeRequest propose message from proposer to acceptor in Phase 2
// accept message.
type ProposeRequest struct {
	Ballot       paxi.Ballot
	Slot         int
	Command      paxi.Command
	CommandShare []byte
	SendTime     time.Time
}

func (m ProposeRequest) String() string {
	return fmt.Sprintf("ProposeRequest {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// ProposeResponse response of propose message, sent from acceptor to proposer
type ProposeResponse struct {
	Ballot paxi.Ballot
	ID     paxi.ID
	Slot   int
	SendTime time.Time
}

func (m ProposeResponse) String() string {
	return fmt.Sprintf("ProposeResponse {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// CommitRequest message issued by proposer/leader to persist a previously accepted value
type CommitRequest struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m CommitRequest) String() string {
	return fmt.Sprintf("CommitRequest {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}
