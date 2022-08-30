package fastopaxos

import (
	"github.com/ailidani/paxi"
	"testing"
)

func TestBallot(t *testing.T) {
	n := 0
	id := paxi.NewID(2, 1)
	b := NewBallot(n, false, id)

	if b.String() != "0.0.2.1" {
		t.Errorf("wrong ballot %s, expecting 0.0.2.1", b.String())
	}

	b.ToClassic()
	if b.String() != "0.1.2.1" {
		t.Errorf("wrong ballot %s, expecting 0.1.2.1", b.String())
	}

	b.Next(id)
	b.Next(id)

	if b.N() != n+2 {
		t.Errorf("Ballot.N() %v != %v", b.N(), n+1)
	}

	if b.ID() != id {
		t.Errorf("Ballot.ID() %v != %v", b.ID(), id)
	}
}

