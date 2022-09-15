package fastopaxos

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/encoder"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	encoder.Register(P1a{})
	encoder.Register(P1b{})
	encoder.Register(P2a{})
	encoder.Register(P2b{})
	encoder.Register(P3{})
	encoder.Register(P3c{})
	encoder.Register(GetMetadataRequest{})
	encoder.Register(GetMetadataResponse{})
}

type SecretShare []byte

// P1a prepare message from proposer to acceptor
type P1a struct {
	Ballot Ballot `msgpack:"b"`
}

// P1b response of prepare (promise message),
// sent from acceptor to proposer.
type P1b struct {
	Ballot Ballot               `msgpack:"b"`           // sender leader's node-id
	ID     paxi.ID              `msgpack:"i"`           // sender node-id
	Log    map[int]CommandShare `msgpack:"v,omitempty"` // uncommitted logs
}

// P2a is proposal message broadcast by proposer
type P2a struct {
	Ballot    Ballot `msgpack:"b"`           // the proposer's ballot number
	Slot      int    `msgpack:"s"`           // the slot to be filled
	AnyVal    bool   `msgpack:"a"`           // true if the proposer proposes the special ANY value
	OriBallot Ballot `msgpack:"o"`           // the original-ballot of the proposed value
	Share     []byte `msgpack:"v,omitempty"` // the proposed secret-share
}

// P2b is the response for proposal (P2a and P2ac)
type P2b struct {
	Ballot    Ballot  `msgpack:"b"` // the accepted ballot number
	ID        paxi.ID `msgpack:"a"` // the (acceptor) sender's ID
	Slot      int     `msgpack:"s"` // the proposed slot
	OriBallot Ballot  `msgpack:"o"` // the original ballot of the accepted secret, if any
	Share     []byte  `msgpack:"v"` // the previously accepted secret-share, if any
}

// DirectCommand is the client's value proposal which is sent directly by client, not proposer.
// The DirectCommand for coordinator has both the secret-shared command and the clear command, but
// the DirectCommand for non-coordinator only has the secret-shared command.
type DirectCommand struct {
	Slot      int         `msgpack:"s"`
	OriBallot Ballot      `msgpack:"o"`           // the original-ballot / secret value identifier
	Share     SecretShare `msgpack:"v"`           // a single share of the proposed secret value
	Command   []byte      `msgpack:"c,omitempty"` // the command in a clear form, intended for the coordinator
}

func (c DirectCommand) GetCommandType() byte {
	return paxi.TypeOtherCommand
}

func (c DirectCommand) String() string {
	return fmt.Sprintf("DirectCmd{o:%s, v:%x c:%x}", c.OriBallot, c.Share, c.Command)
}

func (c *DirectCommand) Serialize() []byte {
	lenShare := len(c.Share)
	lenCmd := len(c.Command)

	// 4 bytes for the slot (uint32)
	// 8 bytes for the command identifier / original-ballot
	// 2 bytes for share's length (uint16)
	// lenShare bytes for the share
	// lenCmd bytes for the clear command
	buff := make([]byte, 4+8+2+lenShare+lenCmd)
	binary.BigEndian.PutUint32(buff[0:4], uint32(c.Slot))
	binary.BigEndian.PutUint64(buff[4:12], uint64(c.OriBallot))
	binary.BigEndian.PutUint16(buff[12:14], uint16(lenShare))
	if lenShare > 0 {
		copy(buff[14:14+lenShare], c.Share)
	}
	if lenCmd > 0 {
		copy(buff[14+lenShare:14+lenShare+lenCmd], c.Command)
	}

	return buff
}

func DeserializeDirectCommand(buff []byte) (*DirectCommand, error) {
	cmd := &DirectCommand{}
	lenBuff := len(buff)
	if lenBuff < 14 {
		return nil, errors.New("buffer's length is at least 14")
	}

	cmd.Slot = int(binary.BigEndian.Uint32(buff[0:4]))
	cmd.OriBallot = Ballot(binary.BigEndian.Uint64(buff[4:12]))
	lenShare := binary.BigEndian.Uint16(buff[12:14])
	if lenShare > 0 && lenBuff < (int(lenShare)+10) {
		return nil, errors.New("mismatch share's length")
	}
	cmd.Share = buff[14 : 14+lenShare]
	if lenBuff > (int(lenShare) + 14) {
		cmd.Command = buff[14+lenShare:]
	}

	return cmd, nil
}

// P3 is a commit message
type P3 struct {
	Ballot    Ballot      `msgpack:"b"`
	Slot      int         `msgpack:"s"`
	OriBallot Ballot      `msgpack:"o"`
	Share     SecretShare `msgpack:"v"`
	Command   []byte      `msgpack:"c,omitempty"`
}

// P3c contains clear committed command for other trusted node
type P3c struct {
	Slot      int    `msgpack:"s"`
	OriBallot Ballot `msgpack:"o"`
	Command   []byte `msgpack:"c"`
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d val=%x}", m.Ballot, m.Slot, m.Share)
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d ssval=%d ob=%s}", m.Ballot, m.ID, m.Slot, len(m.Share), m.OriBallot)
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d}", m.Ballot, m.Slot)
}

// CommandShare combines each secret-shared command with its ballot number
type CommandShare struct {
	ID        paxi.ID `msgpack:"i"`
	Ballot    Ballot  `msgpack:"b"`           // the accepted ballot number
	OriBallot Ballot  `msgpack:"o"`           // the original ballot-number
	Share     []byte  `msgpack:"c,omitempty"` // the secret-share of the accepted value
}

type GetMetadataRequest struct{}

func (g GetMetadataRequest) Serialize() []byte {
	buff, _ := msgpack.Marshal(g)
	return buff
}

func (g GetMetadataRequest) GetCommandType() byte {
	return paxi.TypeGetMetadataCommand
}

func DeserializeGetMetadataRequest(buff []byte) GetMetadataRequest {
	return GetMetadataRequest{}
}

type GetMetadataResponse struct {
	NextSlot  int   `msgpack:"s"`

	Execute   int   `msgpack:"x"`
	LastEntry entry `msgpack:"e"`
}
