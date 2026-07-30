package server

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/BurntSushi/toml"

	"github.com/Dario-Zela/quorum/raft"
)

// Config wires one node. TOML keys are snake_case; Peers maps node id →
// peer-transport address and must be identical on every node (static
// 5-node config — membership changes are v1's #1 non-goal).
type Config struct {
	ID            uint64            `toml:"id"`
	DataDir       string            `toml:"data_dir"`
	ListenPeer    string            `toml:"listen_peer"`
	ListenClient  string            `toml:"listen_client"`
	ListenMetrics string            `toml:"listen_metrics"` // "" disables
	Peers         map[string]string `toml:"peers"`

	TickMs          int `toml:"tick_ms"`           // default 10
	ElectionTickMin int `toml:"election_tick_min"` // default 10
	ElectionTickMax int `toml:"election_tick_max"` // default 20
	HeartbeatTicks  int `toml:"heartbeat_ticks"`   // default 3

	// SnapshotEntries triggers a state-machine snapshot + log compaction
	// once lastApplied outruns the last snapshot by this many entries.
	SnapshotEntries int `toml:"snapshot_entries"` // default 10000
}

// LoadConfig reads a TOML config file.
func LoadConfig(path string) (Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Validate applies defaults and sanity-checks.
func (c *Config) Validate() error {
	if c.ID == 0 {
		return fmt.Errorf("config: id must be non-zero")
	}
	if c.DataDir == "" {
		return fmt.Errorf("config: data_dir required")
	}
	if _, ok := c.Peers[strconv.FormatUint(c.ID, 10)]; !ok {
		return fmt.Errorf("config: peers must include this node's id %d", c.ID)
	}
	if c.TickMs == 0 {
		c.TickMs = 10
	}
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = 10000
	}
	return nil
}

// PeerIDs returns all cluster members, sorted.
func (c *Config) PeerIDs() ([]raft.NodeID, error) {
	var ids []raft.NodeID
	for k := range c.Peers {
		n, err := strconv.ParseUint(k, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("config: bad peer id %q", k)
		}
		ids = append(ids, raft.NodeID(n))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// PeerAddrs returns the id → address map with numeric keys.
func (c *Config) PeerAddrs() map[raft.NodeID]string {
	out := make(map[raft.NodeID]string, len(c.Peers))
	for k, v := range c.Peers {
		n, _ := strconv.ParseUint(k, 10, 64)
		out[raft.NodeID(n)] = v
	}
	return out
}
