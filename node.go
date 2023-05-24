package paxi

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	"runtime"
	"sync"

	"github.com/ailidani/paxi/log"
)

var isPprof = flag.Bool("pprof", false, "activate pprof server")

// Node is the primary access point for every replica
// it includes networking, state machine and server
type Node interface {
	Socket
	Database
	ID() ID
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
	GetConfig() *Config
	GetStorage() *bufio.Writer
}

// node implements Node interface
type node struct {
	id ID

	Socket
	Database
	MessageChan     chan interface{}
	ProtocolMsgChan chan interface{}
	handles         map[string]reflect.Value
	server          *http.Server
	storage         *bufio.Writer
	buffer          []byte

	sync.RWMutex
	forwards map[string]*Request
}

// NewNode creates a new Node object from configuration
func NewNode(id ID) Node {
	n := &node{
		id:              id,
		Socket:          NewSocket(id, config.Addrs),
		Database:        NewDatabase(),
		MessageChan:     make(chan interface{}, config.ChanBufferSize),
		ProtocolMsgChan: make(chan interface{}, config.ChanBufferSize),
		handles:         make(map[string]reflect.Value),
		forwards:        make(map[string]*Request),
		storage:         nil,
		buffer:          make([]byte, 16),
	}

	// preparing storage for persistent storage
	storageFile := config.StoragePath
	if storageFile != "" {
		var err error
		storageFile = fmt.Sprintf("%s_%s", storageFile, id)
		if err = os.RemoveAll(storageFile); err != nil {
			log.Fatalf("failed to cleanup place for storage")
		}
		f, err := os.Create(storageFile)
		if err != nil {
			log.Fatalf("failed to create file for storage: %v", err)
		}
		n.storage = bufio.NewWriter(f)
	}

	// set the delay
	delayMap, ok := GetConfig().Delays[string(id)]
	if ok {
		delay, ok2 := delayMap["clients"]
		if ok2 {
			// convert ms to ns
			ServerToClientDelay = uint64(delay * float64(1_000_000))
		}
	}

	return n
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) GetConfig() *Config {
	return &config
}

func (n *node) GetStorage() *bufio.Writer {
	return n.storage
}

func (n *node) Retry(r Request) {
	log.Debugf("node %v retry request %v", n.id, r)
	n.MessageChan <- r
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || (fn.Type().In(0).Kind() != reflect.Interface && fn.Type().In(0) != t) {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running", n.id)

	if *isPprof {
		go func() {
			runtime.SetMutexProfileFraction(5)
			runtime.SetBlockProfileRate(1)
			log.Fatal(http.ListenAndServe("127.0.0.1:6060", nil))
		}()
	}

	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
	}
	if *ClientType == "http" {
		n.http()
	} else if *ClientType == "unix" {
		n.runUnixServer()
	} else if *ClientType == "tcp" {
		n.runTCPServer()
	} else if *ClientType == "udp" {
		n.runUDPServer()
	} else {
		log.Fatalf("unknown client-node connection type: %s", *ClientType)
	}
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		m := n.Recv()
		log.Debugf("node receiving messages: %v", m)
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

		if len(n.ProtocolMsgChan) == cap(n.ProtocolMsgChan) {
			log.Warningf("Channel for protocol messages is full (len=%d)", len(n.ProtocolMsgChan))
		}

		// other messages are handled as protocol message
		n.ProtocolMsgChan <- m
	}
}

// handle receives messages from message channel and protocol message channel
// and calls handle function using refection
func (n *node) handle() {
	for {
		var msg interface{}

		// protocol messages are prioritized more than client's message
		select {
		case msg = <-n.ProtocolMsgChan:
			break
		case msg = <-n.ProtocolMsgChan:
			break
		case msg = <-n.ProtocolMsgChan:
			break
		case msg = <-n.ProtocolMsgChan:
			break
		case msg = <-n.ProtocolMsgChan:
			break
		case msg = <-n.MessageChan:
			break
		}

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
