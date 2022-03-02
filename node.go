package paxi

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"reflect"
	"sync"

	"github.com/ailidani/paxi/log"
)

var isPprof = flag.Bool("pprof", false, "activate pprof server")

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	Socket
	Database
	ID() ID
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
	GetConfig() *Config
}

// node implements Node interface
type node struct {
	id ID

	Socket
	Database
	MessageChan chan interface{}
	handles     map[string]reflect.Value
	server      *http.Server

	sync.RWMutex
	forwards map[string]*Request
}

// NewNode creates a new Node object from configuration
func NewNode(id ID) Node {
	return &node{
		id:          id,
		Socket:      NewSocket(id, config.Addrs),
		Database:    NewDatabase(),
		MessageChan: make(chan interface{}, config.ChanBufferSize),
		handles:     make(map[string]reflect.Value),
		forwards:    make(map[string]*Request),
	}
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) GetConfig() *Config {
	return &config
}

func (n *node) Retry(r Request) {
	log.Debugf("node %v retry reqeust %v", n.id, r)
	n.MessageChan <- r
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running", n.id)

	if *isPprof {
		go func() {
			log.Fatal(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
	}
	if *ClientType == "default" || *ClientType == "" {
		n.http()
	} else {
		n.rpc()
	}
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		m := n.Recv()
		switch m := m.(type) {
		case Request:
			m.c = make(chan Reply, 1)
			go func(r Request) {
				n.Send(r.NodeID, <-r.c)
			}(m)
			n.MessageChan <- m
			continue

		case Reply:
			n.RLock()
			r := n.forwards[m.Command.String()]
			log.Debugf("node %v received reply %v", n.id, m)
			n.RUnlock()
			r.Reply(m)
			continue
		}
		n.MessageChan <- m
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Infof("no registered handle function for message type %v", name)
			continue
		}
		f.Call([]reflect.Value{v})
	}
}

func (n *node) Forward(id ID, m Request) {
	log.Debugf("Node %v forwarding %v to %s", n.ID(), m, id)
	m.NodeID = n.id
	n.Lock()
	n.forwards[m.Command.String()] = &m
	n.Unlock()
	n.Send(id, m)
}
