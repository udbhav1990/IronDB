package main

import (
	"log"
	"os"
	"strconv"

	"github.com/udbhav1990/IronDB/internal/kv"
	"github.com/udbhav1990/IronDB/internal/server"
)

func main() {
	db, err := kv.NewIronDB("data")
	if err != nil {
		log.Fatalf("Failed to start IronDB: %v", err)
	}
	defer db.Close()

	// Optional: read from env or CLI
	workerCount := 0
	if len(os.Args) > 1 {
		workerCount, _ = strconv.Atoi(os.Args[1])
	}

	server.StartTCPServer(":9090", db, workerCount)
}
