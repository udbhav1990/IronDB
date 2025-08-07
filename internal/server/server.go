package server

import (
	"log"
	"net"

	"github.com/udbhav1990/IronDB/internal/kv"
)

func StartTCPServer(addr string, store *kv.IronDB, workerCount int) {
	const defaultWorkers = 20
	if workerCount <= 0 {
		workerCount = defaultWorkers
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Error starting TCP server: %v", err)
	}
	log.Printf("IronDB listening on %s with %d worker(s)\n", addr, workerCount)

	connCh := make(chan net.Conn, 100) // buffered queue

	// Start N workers
	for i := 0; i < workerCount; i++ {
		go worker(connCh, store, i)
	}

	// Accept connections and send to queue
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		connCh <- conn
	}
}

func worker(connCh <-chan net.Conn, store *kv.IronDB, id int) {
	for conn := range connCh {
		log.Printf("[Worker %d] Handling new connection\n", id)
		handleConnection(conn, store)
	}
}
