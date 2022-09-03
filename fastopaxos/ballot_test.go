package fastopaxos

import (
	"bytes"
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

func TestDirectCommandSerialize(t *testing.T) {
	cmd := DirectCommand{
		OriBallot: NewBallot(123, false, paxi.NewID(123, 123)),
		Share:     []byte("asdasdasdasdasda"),
		Command:   []byte("1234134jhasdkjha"),
	}

	buff := cmd.Serialize()
	ncmd, err := DeserializeDirectCommand(buff)
	if err != nil {
		t.Error(err)
	}
	if ncmd.OriBallot != cmd.OriBallot {
		t.Errorf("not matching original-ballot: %s vs %s", ncmd.OriBallot, cmd.OriBallot)
	}
	if bytes.Compare(ncmd.Share, cmd.Share) != 0 {
		t.Errorf("unmatch share")
	}
	if bytes.Compare(ncmd.Command, cmd.Command) != 0 {
		t.Errorf("unmatch command")
	}
}
