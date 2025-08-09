package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	proto "github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type TCPTransport struct {
	id         uint64
	address    string
	peers      map[uint64]string    // peerID -> address (includes self)
	stepFunc   func(raftpb.Message) // Raft's node.Step
	listener   net.Listener
	alivePeers map[uint64]bool
	mu         sync.Mutex
}

func NewTCPTransport(id uint64, address string, peers map[uint64]string, stepFunc func(raftpb.Message)) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", address, err)
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
			c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				t.alivePeers[id] = false
			} else {
				t.alivePeers[id] = true
				_ = c.Close()
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
			log.Printf("transport accept: %v", err)
			continue
		}
		go t.handle(conn)
	}
}

func (t *TCPTransport) handle(conn net.Conn) {
	defer conn.Close()
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("transport read len: %v", err)
			}
			return
		}
		n := binary.BigEndian.Uint32(lenBuf)
		if n == 0 {
			continue
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(conn, data); err != nil {
			log.Printf("transport read data: %v", err)
			return
		}
		var m raftpb.Message
		if err := proto.Unmarshal(data, &m); err != nil {
			log.Printf("transport unmarshal: %v", err)
			return
		}
		t.stepFunc(m)
	}
}

func (t *TCPTransport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		t.mu.Lock()
		addr := t.peers[m.To]
		alive := t.alivePeers[m.To]
		t.mu.Unlock()
		if addr == "" {
			log.Printf("transport: unknown peer id=%d", m.To)
			continue
		}
		if !alive {
			log.Printf("transport: peer %d down, skipping", m.To)
			continue
		}
		go t.sendOne(addr, m)
	}
}

func (t *TCPTransport) sendOne(addr string, m raftpb.Message) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("transport dial %s: %v", addr, err)
		return
	}
	defer c.Close()

	b, err := proto.Marshal(&m)
	if err != nil {
		log.Printf("transport marshal: %v", err)
		return
	}
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(b)))
	if _, err := c.Write(lenBuf); err != nil {
		log.Printf("transport write len: %v", err)
		return
	}
	if _, err := c.Write(b); err != nil {
		log.Printf("transport write data: %v", err)
		return
	}
}
