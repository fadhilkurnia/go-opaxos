package paxi

import (
	"encoding/binary"
	"encoding/json"
	"sync"
)

// Key type of the key-value database
type Key int32

func (k Key) ToBytes() []byte {
	buff := make([]byte, 4)
	binary.BigEndian.PutUint32(buff, uint32(k))
	return buff
}

// Value type of key-value database
type Value []byte

// Database defines a database interface
type Database interface {
	Execute(Command) Value
	History(Key) []Value
	Get(Key) Value
	Put(Key, Value)
}

// Database implements a multi-version key-value datastore as the StateMachine
type database struct {
	sync.RWMutex
	data         map[Key]Value
	version      int
	multiversion bool
	history      map[Key][]Value
}

// NewDatabase returns database that implements Database interface
func NewDatabase() Database {
	return &database{
		data:         make(map[Key]Value),
		version:      0,
		multiversion: config.MultiVersion,
		history:      make(map[Key][]Value),
	}
}

// Execute executes a command against database
func (d *database) Execute(c Command) Value {
	d.Lock()
	defer d.Unlock()

	// get previous value
	v := d.data[c.Key]

	// writes new value
	d.put(c.Key, c.Value)

	return v
}

// Get gets the current value and version of given key
func (d *database) Get(k Key) Value {
	d.RLock()
	defer d.RUnlock()
	return d.data[k]
}

func (d *database) put(k Key, v Value) {
	if v != nil {
		d.data[k] = v
		d.version++
		if d.multiversion {
			if d.history[k] == nil {
				d.history[k] = make([]Value, 0)
			}
			d.history[k] = append(d.history[k], v)
		}
	}
}

// Put puts a new value of given key
func (d *database) Put(k Key, v Value) {
	d.Lock()
	defer d.Unlock()
	d.put(k, v)
}

// Version returns current version of given key
func (d *database) Version(k Key) int {
	d.RLock()
	defer d.RUnlock()
	return d.version
}

// History returns entire value history in order
func (d *database) History(k Key) []Value {
	d.RLock()
	defer d.RUnlock()
	return d.history[k]
}

func (d *database) String() string {
	d.RLock()
	defer d.RUnlock()
	b, _ := json.Marshal(d.data)
	return string(b)
}

// Conflict checks if two commands are conflicting as reorder them will end in different states
func Conflict(gamma *Command, delta *Command) bool {
	if gamma.Key == delta.Key {
		if !gamma.IsRead() || !delta.IsRead() {
			return true
		}
	}
	return false
}
