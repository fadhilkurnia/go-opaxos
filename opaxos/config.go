package opaxos

import "github.com/ailidani/paxi"

// TODO: implement this

type Config struct {
	SecretSharing        string // options: shamir, krawczyk. default: shamir
	NumNode              *int32
	Threshold            *int32
	Quorum1              *int32
	Quorum2              *int32
	QuorumFast           *int32
	ProposerIDs          []paxi.ID
	AcceptorIDs          []paxi.ID
	TrustedAcceptorIDs   []paxi.ID
	UntrustedAcceptorIDs []paxi.ID
	LearnerIDs           []paxi.ID
}

func InitConfig(cfg *paxi.Config) Config {
	return Config{}
}
