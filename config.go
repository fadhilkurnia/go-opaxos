package paxi

import (
	"encoding/json"
	"flag"
	"net/url"
	"os"

	"github.com/ailidani/paxi/log"
)

var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")
var GatherSecretShareTime = flag.Bool("sstimeon", false, "Whether the server need to return the secret-sharing encoding time or not")

// Config contains every system configuration
type Config struct {
	Addrs     map[ID]string `json:"address"`      // address for node communication
	HTTPAddrs map[ID]string `json:"http_address"` // address for client server communication
	RPCAddrs  map[ID]string `json:"rpc_address"`  // address for client server communication
	Roles     map[ID]string `json:"roles"`        // (used in OPaxos) roles for each node, separated with comma. e.g: proposer,acceptor

	StoragePath string `json:"storage_path"`

	Protocol map[string]interface{} `json:"protocol"` // (used in OPaxos) consensus protocol used, and its configuration parameter

	Policy    string  `json:"policy"`    // leader change policy {consecutive, majority}
	Threshold float64 `json:"threshold"` // threshold for policy in WPaxos {n consecutive or time interval in ms}

	Thrifty        bool    `json:"thrifty"`          // only send messages to a quorum
	BufferSize     int     `json:"buffer_size"`      // buffer size for maps
	ChanBufferSize int     `json:"chan_buffer_size"` // buffer size for channels
	MultiVersion   bool    `json:"multiversion"`     // create multi-version database
	Benchmark      Bconfig `json:"benchmark"`        // benchmark configuration

	// for future implementation
	// Batching bool `json:"batching"`
	// Consistency string `json:"consistency"`
	// Codec string `json:"codec"` // codec for message serialization between nodes

	n   int         // total number of nodes
	z   int         // total number of zones
	npz map[int]int // nodes per zone
}

// Config is global configuration singleton generated by init() func below
var config Config

func init() {
	config = MakeDefaultConfig()
}

// GetConfig returns paxi package configuration
func GetConfig() Config {
	return config
}

// Simulation enable go channel transportation to simulate distributed environment
func Simulation() {
	*scheme = "chan"
}

// MakeDefaultConfig returns Config object with few default values
// only used by init() and master
func MakeDefaultConfig() Config {
	return Config{
		Policy:         "consecutive",
		Threshold:      3,
		BufferSize:     1024,
		ChanBufferSize: 1024,
		MultiVersion:   false,
		Benchmark:      DefaultBConfig(),
	}
}

// IDs returns all node ids
func (c Config) IDs() []ID {
	ids := make([]ID, 0)
	for id := range c.Addrs {
		ids = append(ids, id)
	}
	return ids
}

// N returns total number of nodes
func (c Config) N() int {
	return c.n
}

// Z returns total number of zones
func (c Config) Z() int {
	return c.z
}

func (c Config) GetRPCHost(id ID) string {
	fullAddress := c.RPCAddrs[id]
	address, err := url.Parse(fullAddress)
	if err != nil {
		log.Fatalf("failed to parse server address from config file: %s", err)
	}
	return address.Host
}

func (c Config) GetRPCPort(id ID) string {
	fullAddress := c.RPCAddrs[id]
	address, err := url.Parse(fullAddress)
	if err != nil {
		log.Fatalf("failed to parse server address from config file: %s", err)
	}
	return address.Port()
}

// String is implemented to print the config
func (c Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Error(err)
		return ""
	}
	return string(config)
}

// Load loads configuration from config file in JSON format
func (c *Config) Load() {
	file, err := os.Open(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		log.Fatal(err)
	}

	c.npz = make(map[int]int)
	for id := range c.Addrs {
		c.n++
		c.npz[id.Zone()]++
	}
	c.z = len(c.npz)
}

// Save saves configuration to file in JSON format
func (c Config) Save() error {
	file, err := os.Create(*configFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
