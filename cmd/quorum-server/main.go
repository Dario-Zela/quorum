// quorum-server runs one cluster node.
//
//	quorum-server -config node1.toml
//	quorum-server -id 1 -data /data -listen-peer :7101 -listen-client :7201 \
//	              -peers "1=localhost:7101,2=localhost:7102,3=localhost:7103"
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Dario-Zela/quorum/server"
)

func main() {
	var (
		configPath    = flag.String("config", "", "TOML config file (flags override nothing when set)")
		id            = flag.Uint64("id", 0, "node id")
		dataDir       = flag.String("data", "", "data directory")
		listenPeer    = flag.String("listen-peer", ":7101", "raft transport listen address")
		listenClient  = flag.String("listen-client", ":7201", "client service listen address")
		listenMetrics = flag.String("listen-metrics", "", "prometheus /metrics listen address (empty = off)")
		peers         = flag.String("peers", "", "comma-separated id=host:port peer map (must include self)")
		snapEntries   = flag.Int("snapshot-entries", 0, "compact after this many applied entries (0 = default 10000)")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var cfg server.Config
	var err error
	if *configPath != "" {
		cfg, err = server.LoadConfig(*configPath)
		if err != nil {
			logger.Error("config", "err", err)
			os.Exit(1)
		}
	} else {
		cfg = server.Config{
			ID: *id, DataDir: *dataDir,
			ListenPeer: *listenPeer, ListenClient: *listenClient, ListenMetrics: *listenMetrics,
			SnapshotEntries: *snapEntries,
			Peers:           map[string]string{},
		}
		for _, kvp := range strings.Split(*peers, ",") {
			parts := strings.SplitN(strings.TrimSpace(kvp), "=", 2)
			if len(parts) == 2 {
				cfg.Peers[parts[0]] = parts[1]
			}
		}
		if err := cfg.Validate(); err != nil {
			logger.Error("config", "err", err)
			os.Exit(1)
		}
	}

	n, err := server.Run(cfg, logger)
	if err != nil {
		logger.Error("startup", "err", err)
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	n.Stop()
}
