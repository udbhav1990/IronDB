// --- Updated transport.go ---

package replicaloop

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type TCPTransport struct {
	id         uint64
	address    string
	peers      map[uint64]string    // peerID -> address
	stepFunc   func(raftpb.Message) // Raft's node.Step function
	listener   net.Listener
	alivePeers map[uint64]bool
	mu         sync.Mutex
}

func NewTCPTransport(id uint64, address string, peers map[uint64]string, stepFunc func(raftpb.Message)) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	t := &TCPTransport{
		id:         id,
		address:    address,
		peers:      peers,
		stepFunc:   stepFunc,
		listener:   ln,
		alivePeers: make(map[uint64]bool),
	}

	go t.acceptLoop()
	go t.monitorPeers()

	return t, nil
}

func (t *TCPTransport) monitorPeers() {
	for {
		t.mu.Lock()
		for id, addr := range t.peers {
			if id == t.id {
				continue
			}
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				t.alivePeers[id] = false
			} else {
				t.alivePeers[id] = true
				conn.Close()
			}
		}
		t.mu.Unlock()
		time.Sleep(2 * time.Second)
	}
}

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

func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("transport: read length error: %v", err)
			}
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			log.Printf("transport: read data error: %v", err)
			return
		}
		var msg raftpb.Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			log.Printf("transport: unmarshal error: %v", err)
			return
		}
		t.stepFunc(msg)
	}
}

func (t *TCPTransport) Send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		t.mu.Lock()
		alive := t.alivePeers[msg.To]
		addr := t.peers[msg.To]
		t.mu.Unlock()

		if !alive {
			log.Printf("transport: peer %d not alive, skipping", msg.To)
			continue
		}
		go t.sendOne(addr, msg)
	}
}

func (t *TCPTransport) sendOne(addr string, msg raftpb.Message) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("transport: dial error to %s: %v", addr, err)
		return
	}
	defer conn.Close()

	data, err := proto.Marshal(&msg)
	if err != nil {
		log.Printf("transport: marshal error: %v", err)
		return
	}

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
