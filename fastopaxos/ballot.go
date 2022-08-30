package fastopaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
)

// Ballot is ballot number type combines
// 32 bits of natural number, 1 bit to indicate ballot type: fast or classic,
// and 31 bits of node id into uint64
type Ballot uint64
const BallotTypeFast int = 0
const BallotTypeClassic int = 1

// NewBallot generates ballot number in format <n, ballot_type, zone, node>
func NewBallot(n int, isClassic bool, id paxi.ID) Ballot {
	bit := BallotTypeFast
	if isClassic {
		bit = BallotTypeClassic
	}
	// 32 bits for n | 1 bit for ballot type | 15 bits for zone | 16 bit for node id
	return Ballot(n<<32 | bit<<31 | id.Zone()<<16 | id.Node())
}

// N returns first 32 bit of ballot
func (b Ballot) N() int {
	return int(uint64(b) >> 32)
}

func (b Ballot) Type() int {
	return int(uint64(b) << 32 >> 32 >> 31)
}

// ID return node id as the last 31 bits of ballot
func (b Ballot) ID() paxi.ID {
	zone := int(uint32(b) << 1 >> 1 >> 16)
	node := int(uint16(b))
	return paxi.NewID(zone, node)
}

// Next generates the next ballot number given node id
func (b *Ballot) Next(id paxi.ID) {
	isClassic := true
	if b.Type() == BallotTypeFast {
		isClassic = false
	}
	*b = NewBallot(b.N()+1, isClassic, id)
}

func (b *Ballot) ToClassic() {
	if b.Type() == BallotTypeClassic {
		return
	}
	*b = NewBallot(b.N(), true, b.ID())
}

func (b Ballot) String() string {
	return fmt.Sprintf("%d.%d.%s", b.N(), b.Type(), b.ID())
}