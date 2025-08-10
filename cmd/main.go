// cmd/irondb/main.go
package main

import (
	"flag"
	"log"
	"os"

	"github.com/udbhav1990/IronDB/cmd/config"
	"github.com/udbhav1990/IronDB/internal/kv"
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
	// create a tcp server which takes put get delete command and call store methods
	log.Printf("IronDB instance created successfully with data dir: %s", store)
	// Start the TCP server (not implemented here, but would handle commands)
	log.Printf("Starting TCP server on %s...", cfg.Address)

}
