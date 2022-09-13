package paxi

import (
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put message to outbound queue
	Send(to ID, m interface{})

	// MulticastZone send msg to all nodes in the same site
	MulticastZone(zone int, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	MulticastQuorum(quorum int, m interface{})

	// MulticastUniqueMessage sends to all peers with the provided message
	MulticastUniqueMessage(ms []interface{})

	// MulticastQuorumUniqueMessage sends to a random quorum of peers with the provided message
	MulticastQuorumUniqueMessage(quorum int, ms[]interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(id ID, t int)             // drops every message send to ID last for t seconds
	Slow(id ID, d int, t int)      // delays every message send to ID for d ms and last for t seconds
	Flaky(id ID, p float64, t int) // drop message by chance p for t seconds
	Crash(t int)                   // node crash for t seconds
}

type socket struct {
	id        ID
	addresses map[ID]string
	nodes     map[ID]Transport

	crash bool
	drop  map[ID]bool
	slow  map[ID]uint64
	flaky map[ID]float64

	lock sync.RWMutex // locking map nodes
}

// NewSocket return Socket interface instance given self ID, node list, transport and codec name
func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:        id,
		addresses: addrs,
		nodes:     make(map[ID]Transport),
		crash:     false,
		drop:      make(map[ID]bool),
		slow:      make(map[ID]uint64),
		flaky:     make(map[ID]float64),
	}

	socket.nodes[id] = NewTransport(addrs[id])
	socket.nodes[id].Listen()

	// initialize delays
	delayMap, ok := GetConfig().Delays[string(id)]
	if ok {
		for destID, _ := range addrs {
			delay, ok2 := delayMap[string(destID)]
			if ok2 {
				// convert ms to ns
				socket.slow[destID] = uint64(delay * float64(1_000_000))
			}
		}
	}

	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	log.Debugf("node %s send message %+v to %v", s.id, m, to)

	if s.crash {
		return
	}

	if s.drop[to] {
		return
	}

	if p, ok := s.flaky[to]; ok && p > 0 {
		if rand.Float64() < p {
			return
		}
	}

	s.lock.RLock()
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	if !exists {
		s.lock.RLock()
		address, ok := s.addresses[to]
		s.lock.RUnlock()
		if !ok {
			log.Errorf("socket does not have address of node %s", to)
			return
		}
		t = NewTransport(address)
		err := Retry(t.Dial, 3, time.Duration(50)*time.Millisecond)
		if err != nil {
			log.Errorf("failed to make connection to %s: %v", to, err)
			return
		}
		s.lock.Lock()
		s.nodes[to] = t
		s.lock.Unlock()
	}

	if delay, ok := s.slow[to]; ok && delay > 0 {
		timer := time.NewTimer(time.Duration(delay))
		go func() {
			<-timer.C
			t.Send(m)
		}()
		return
	}

	t.Send(m)
}

func (s *socket) Recv() interface{} {
	s.lock.RLock()
	t := s.nodes[s.id]
	s.lock.RUnlock()
	for {
		m := t.Recv()
		if !s.crash {
			return m
		}
	}
}

func (s *socket) MulticastZone(zone int, m interface{}) {
	log.Debugf("node %s broadcasting message %+v in zone %d", s.id, m, zone)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		if id.Zone() == zone {
			s.Send(id, m)
		}
	}
}

func (s *socket) MulticastQuorum(quorum int, m interface{}) {
	log.Debugf("node %s multicasting message %+v for %d nodes", s.id, m, quorum)
	i := 0
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
		i++
		if i == quorum {
			break
		}
	}
}

func (s *socket) MulticastUniqueMessage(ms []interface{}) {
	log.Debugf("node %s unique-broadcasting messages", s.id)
	if len(ms) < len(s.addresses)-1 {
		log.Fatalf("need more message to be sent to the peers, expecting %d messages but only get %d",
			len(s.addresses)-1, len(ms))
		return
	}
	i := 0
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, ms[i])
		i++
	}
}

func (s *socket) MulticastQuorumUniqueMessage(quorum int, ms[]interface{}) {
	if len(ms) < quorum {
		log.Fatalf("need more message to be sent to the peers, expecting %d messages but only get %d",
			quorum, len(ms))
		return
	}
	i := 0
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, ms[i])
		i++
		if i == quorum {
			break
		}
	}
}

func (s *socket) Broadcast(m interface{}) {
	log.Debugf("node %s broadcasting message %+v", s.id, m)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
}

func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

func (s *socket) Drop(id ID, t int) {
	s.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.drop[id] = false
	}()
}

// Slow assign delay in ms
func (s *socket) Slow(id ID, delay int, t int) {
	s.slow[id] = uint64(delay * 1_000_000) // convert ms to ns
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = 0
	}()
}

func (s *socket) Flaky(id ID, p float64, t int) {
	s.flaky[id] = p
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = 0
	}()
}

func (s *socket) Crash(t int) {
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
		}()
	}
}
