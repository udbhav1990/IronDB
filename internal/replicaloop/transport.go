package replicaloop

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type TCPTransport struct {
	id       uint64
	address  string
	peers    map[uint64]string    // peerID -> address
	stepFunc func(raftpb.Message) // Raft's node.Step function
	listener net.Listener
	mu       sync.Mutex
}

// NewTCPTransport starts the TCP server and returns the transport
func NewTCPTransport(id uint64, address string, peers map[uint64]string, stepFunc func(raftpb.Message)) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	t := &TCPTransport{
		id:       id,
		address:  address,
		peers:    peers,
		stepFunc: stepFunc,
		listener: ln,
	}

	go t.acceptLoop()

	return t, nil
}

// acceptLoop listens for incoming Raft messages
func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Printf("transport: failed to accept connection: %v", err)
			continue
		}
		go t.handleConnection(conn)
	}
}

// handleConnection reads Raft messages from a TCP connection
func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read 4-byte length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("transport: read length error: %v", err)
			}
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// Read protobuf payload
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			log.Printf("transport: read data error: %v", err)
			return
		}

		var msg raftpb.Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			log.Printf("transport: failed to unmarshal raftpb.Message: %v", err)
			return
		}

		// Deliver to raft
		t.stepFunc(msg)
	}
}

// Send sends raftpb.Messages to their respective peers
func (t *TCPTransport) Send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		addr, ok := t.peers[msg.To]
		if !ok {
			log.Printf("transport: peer %d not found", msg.To)
			continue
		}
		go t.sendOne(addr, msg)
	}
}

// sendOne sends a single raftpb.Message to a peer
func (t *TCPTransport) sendOne(addr string, msg raftpb.Message) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("transport: failed to dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	data, err := proto.Marshal(&msg)
	if err != nil {
		log.Printf("transport: marshal error: %v", err)
		return
	}

	// Write length-prefixed message
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		log.Printf("transport: write length error: %v", err)
		return
	}
	if _, err := conn.Write(data); err != nil {
		log.Printf("transport: write data error: %v", err)
		return
	}
}
