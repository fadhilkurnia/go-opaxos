package paxi

import "sync"

const ringSize = 4096
const mask = ringSize-1

type ring struct {
	_     [8]uint64
	write uint64
	_     [7]uint64
	read1 uint64
	_     [7]uint64
	read2 uint64
	_     [7]uint64
	mask  uint64
	_     [7]uint64
	slots [ringSize]slot
}

type slot struct {
	cond   *sync.Cond
	mark   uint32
	req    RPCMessage
	respCh chan RPCMessage
}

func newRing() *ring {
	r := &ring{}
	for i := range r.slots {
		r.slots[i] = slot{respCh: make(chan RPCMessage, 0)}
		r.slots[i].cond = &sync.Cond{}
		r.slots[i].cond.L = &sync.Mutex{}
	}
	return r
}