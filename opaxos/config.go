package opaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

const ProtocolName = "opaxos"
const AlgShamir = "shamir"
const AlgSSMS = "ssms"

type ProtocolConfig struct {
	Name          string `json:"name"`           // the consensus protocol name: 'opaxos'
	SecretSharing string `json:"secret_sharing"` // options: shamir, ssms. default: shamir
	Threshold     int    `json:"threshold"`      // threshold for the secret sharing
	Quorum1       int    `json:"quorum_1"`       // size of the phase-1 quorum. default: N/2+k
	Quorum2       int    `json:"quorum_2"`       // size of the phase-2 quorum. default: N/2
	QuorumFast    int    `json:"quorum_fast"`    // size of the fast quorum.
}

type Config struct {
	*paxi.Config
	Protocol ProtocolConfig
}

func InitConfig(cfg *paxi.Config) Config {
	// parse protocol config
	protocolCfg := ProtocolConfig{}
	for key, v := range cfg.Protocol {
		switch key {
		case "name":
			if v != ProtocolName {
				log.Fatalf("'name' have to be '%s'", ProtocolName)
			}
			protocolCfg.Name = ProtocolName
		case "secret_sharing":
			if v == "" {
				v = AlgShamir
			}
			if v != AlgShamir && v != AlgSSMS {
				log.Warningf("'secret_sharing' have to be '%s' or '%s', now we are using '%s'", AlgShamir, AlgSSMS, v)
			}
			protocolCfg.SecretSharing = v.(string)
		case "threshold":
			k := int(v.(float64))
			if k < 2 || k > cfg.N() {
				log.Fatalf("'threshold' must be greater than 1 and less than N+1. (k=%d)", k)
			}
			protocolCfg.Threshold = k
		case "quorum_1":
			q1 := int(v.(float64))
			if q1 < 1 || q1 > cfg.N() {
				log.Fatalf("'quorum_1' must be a positive integer less than or equal to N. (q1=%d)", q1)
			}
			protocolCfg.Quorum1 = q1
		case "quorum_2":
			q2 := int(v.(float64))
			if q2 < 1 || q2 > cfg.N() {
				log.Fatalf("'quorum_2' must be a positive integer less than or equal to N. (q2=%d)", q2)
			}
			protocolCfg.Quorum2 = q2
		case "quorum_fast":
			qf := int(v.(float64))
			if qf < 1 || qf > cfg.N() {
				log.Fatalf("'quorum_fast' must be a positive integer less than or equal to N. (qf=%d)", qf)
			}
			protocolCfg.QuorumFast = qf
		default:
			log.Fatalf("unknown config parameter %s", key)
		}
	}

	// set default quorum size
	if protocolCfg.Quorum1 == 0 && protocolCfg.Quorum2 == 0 {
		protocolCfg.Quorum1 = (cfg.N() / 2) + protocolCfg.Threshold
		if cfg.N() % 2 == 0 {
			protocolCfg.Quorum1 -= 1
		}
		protocolCfg.Quorum2 = (cfg.N() / 2) + 1
	}

	// check q1 and q2 quorum intersection
	if protocolCfg.SecretSharing != "other" && protocolCfg.Quorum1+protocolCfg.Quorum2 < cfg.N()+protocolCfg.Threshold {
		log.Fatal("'quorum_1' and 'quorum_2' must intersect with at least 'threshold' nodes")
	}

	// check q1 and fast quorum intersection
	if protocolCfg.SecretSharing != "other" && 2*protocolCfg.QuorumFast+protocolCfg.Quorum1 < 2*cfg.N()+protocolCfg.Threshold {
		log.Fatal("the intersection of 'quorum_fast' must intersect with at least 'threshold' nodes in any 'quorum_1'")
	}

	return Config{cfg, protocolCfg}
}
