// cmd/irondb/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/udbhav1990/IronDB/cmd/config"
	"github.com/udbhav1990/IronDB/internal/kv"
	"github.com/udbhav1990/IronDB/internal/replicaloop"
)

func main1() {
	// Parse config file path from CLI
	cfgPath := flag.String("config", "config.yaml", "Path to YAML config")
	flag.Parse()

	// Load config
	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Ensure data dir exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	// Step 1: Create WAL
	wal, err := kv.NewWAL(cfg.DataDir + "/wal.log")
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}

	// Step 3: Create IronDB instance
	store, err := kv.NewIronDB(cfg.DataDir, wal)
	if err != nil {
		log.Fatalf("Failed to create IronDB instance: %v", err)
	}

	// Step 3: Create Raft Node FIRST (StepFunc comes from this)
	raftNode := replicaloop.NewRaftNode(cfg.NodeID, wal, store, cfg.PeerList())

	// Step 4: Create TCP Transport AFTER (inject raftNode.Step)
	transport, err := replicaloop.NewTCPTransport(cfg.NodeID, cfg.Address, cfg.Peers, raftNode.Step)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}

	// Step 5: Run
	go raftNode.Run(transport)

	// Step 9: Simple CLI loop for testing
	fmt.Println("IronDB started. Press Ctrl+C to exit.")
	select {}
}
