package config

import (
	"io/ioutil"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"gopkg.in/yaml.v2"
)

type Config struct {
	NodeID  uint64            `yaml:"node_id"`
	Address string            `yaml:"address"`
	DataDir string            `yaml:"data_dir"`
	Peers   map[uint64]string `yaml:"peers"`

	RaftTickMs            int `yaml:"raft_tick_ms"`            // default: 100ms
	ElectionTimeoutTicks  int `yaml:"election_timeout_ticks"`  // default: 10
	HeartbeatTimeoutTicks int `yaml:"heartbeat_timeout_ticks"` // default: 1

	TCPWorkers int `yaml:"tcp_workers"` // default: 4
}

// LoadConfig loads and parses the YAML config file
func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Apply defaults
	if cfg.RaftTickMs == 0 {
		cfg.RaftTickMs = 100
	}
	if cfg.ElectionTimeoutTicks == 0 {
		cfg.ElectionTimeoutTicks = 10
	}
	if cfg.HeartbeatTimeoutTicks == 0 {
		cfg.HeartbeatTimeoutTicks = 1
	}
	if cfg.TCPWorkers == 0 {
		cfg.TCPWorkers = 4
	}

	return &cfg, nil
}

// RaftTickDuration returns time.Duration for ticking
func (c *Config) RaftTickDuration() time.Duration {
	return time.Duration(c.RaftTickMs) * time.Millisecond
}

func (cfg *Config) PeerList() []raft.Peer {
	var peers []raft.Peer

	// Include self
	peers = append(peers, raft.Peer{ID: cfg.NodeID})

	for id := range cfg.Peers {
		if id == cfg.NodeID {
			continue // already included self
		}
		peers = append(peers, raft.Peer{ID: id})
	}
	return peers
}
