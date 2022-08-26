package fastopaxos

import "github.com/ailidani/paxi"

// Ballot is ballot number type combines
// 32 bits of natural number, 1 bit to indicate ballot type: fast or classic,
// and 31 bits of node id into uint64
type Ballot uint64

// NewBallot generates ballot number in format <n, ballot_type, zone, node>
func NewBallot(n int, isClassic bool, id paxi.ID) Ballot {
	bit := 0
	if isClassic {
		bit = 1
	}
	return Ballot(n<<32 | bit<<16 | id.Zone()<<15 | id.Node())
}
