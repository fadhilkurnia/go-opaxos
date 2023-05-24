package untrustedopaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
)

type ClientOriginalBallot uint32

// NewBallot generates ballot number in format <n, zone, node>
func NewBallot(n int, id paxi.ID) ClientOriginalBallot {
	return ClientOriginalBallot(n<<16 | int(int8(id.Zone()))<<8 | int(int8(id.Node())))
}

// N returns first 16 bit of ballot
func (b ClientOriginalBallot) N() int {
	return int(uint32(b) >> 16)
}

// ID return node id as last 16 bits of ballot
func (b ClientOriginalBallot) ID() paxi.ID {
	zone := int(int16(b) >> 8)
	node := int(int8(b))
	return paxi.NewID(zone, node)
}

func (b ClientOriginalBallot) String() string {
	return fmt.Sprintf("%d.%s", b.N(), b.ID())
}

func (b *ClientOriginalBallot) Next() {
	*b = NewBallot(b.N()+1, b.ID())
}