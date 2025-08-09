package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/udbhav1990/IronDB/internal/kv"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// ----- Replicated command (value embedded; no value WAL) -----

type Command struct {
	Op    string `json:"op"`              // "put" | "delete"
	Key   string `json:"key"`             // key
	Value []byte `json:"value,omitempty"` // for put
}

// ----- Minimal Raft WAL (stdlib only) -----
// Stores HardState + Entries for durability across restarts.

type RaftWAL struct {
	f *os.File
}

func openRaftWAL(path string) (*RaftWAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	// append mode
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &RaftWAL{f: f}, nil
}

func (w *RaftWAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	_ = w.f.Sync()
	return w.f.Close()
}

// Very simple framing: [1 byte type 'H'|'E'][4-byte big-endian len][payload]
func writeRecord(f *os.File, typ byte, payload []byte) error {
	hdr := []byte{typ, 0, 0, 0, 0}
	n := uint32(len(payload))
	hdr[1] = byte(n >> 24)
	hdr[2] = byte(n >> 16)
	hdr[3] = byte(n >> 8)
	hdr[4] = byte(n)
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	_, err := f.Write(payload)
	return err
}

func (w *RaftWAL) Save(hs raftpb.HardState, ents []raftpb.Entry) error {
	if !raft.IsEmptyHardState(hs) {
		b, err := hs.Marshal()
		if err != nil {
			return err
		}
		if err := writeRecord(w.f, 'H', b); err != nil {
			return err
		}
	}
	for i := range ents {
		b, err := ents[i].Marshal()
		if err != nil {
			return err
		}
		if err := writeRecord(w.f, 'E', b); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

func (w *RaftWAL) ReadAll() (raftpb.HardState, []raftpb.Entry, error) {
	var hs raftpb.HardState
	var out []raftpb.Entry
	if _, err := w.f.Seek(0, os.SEEK_SET); err != nil {
		return hs, nil, err
	}
	hdr := make([]byte, 5)
	for {
		if _, err := w.f.Read(hdr); err != nil {
			// EOF = done (ignore partial)
			break
		}
		typ := hdr[0]
		n := int(hdr[1])<<24 | int(hdr[2])<<16 | int(hdr[3])<<8 | int(hdr[4])
		if n <= 0 {
			continue
		}
		buf := make([]byte, n)
		if _, err := w.f.Read(buf); err != nil {
			break
		}
		switch typ {
		case 'H':
			var t raftpb.HardState
			if err := t.Unmarshal(buf); err != nil {
				return hs, out, err
			}
			hs = t
		case 'E':
			var e raftpb.Entry
			if err := e.Unmarshal(buf); err != nil {
				return hs, out, err
			}
			out = append(out, e)
		default:
			// unknown => stop
			return hs, out, nil
		}
	}
	_, _ = w.f.Seek(0, os.SEEK_END)
	return hs, out, nil
}

// ----- Raft node with TCP transport -----

type RaftNode struct {
	id      uint64
	address string
	peers   map[uint64]string // includes self
	dataDir string

	node    raft.Node
	storage *raft.MemoryStorage
	wal     *RaftWAL

	db *kv.IronDBR // rename to your type (e.g., *kv.IronDB) if different

	applied uint64

	transport *TCPTransport
	ticker    *time.Ticker

	stopc  chan struct{}
	errorC chan error
}

func NewRaftNode(
	id uint64,
	address string,
	peers map[uint64]string,
	dataDir string,
	db *kv.IronDBR, // rename if your type name differs
	join bool,
) (*RaftNode, <-chan error, error) {

	if address == "" {
		return nil, nil, errors.New("address required")
	}
	if _, ok := peers[id]; !ok {
		return nil, nil, fmt.Errorf("peers must include self id=%d", id)
	}

	r := &RaftNode{
		id:      id,
		address: address,
		peers:   peers,
		dataDir: dataDir,
		db:      db,
		storage: raft.NewMemoryStorage(),
		ticker:  time.NewTicker(100 * time.Millisecond),
		stopc:   make(chan struct{}),
		errorC:  make(chan error, 1),
	}

	// WAL open + replay
	var err error
	walPath := filepath.Join(dataDir, fmt.Sprintf("raft-%d.wal", id))
	r.wal, err = openRaftWAL(walPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open raft wal: %w", err)
	}
	hs, ents, err := r.wal.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("replay raft wal: %w", err)
	}
	if !raft.IsEmptyHardState(hs) {
		r.storage.SetHardState(hs)
	}
	if len(ents) > 0 {
		if err := r.storage.Append(ents); err != nil {
			return nil, nil, fmt.Errorf("storage append: %w", err)
		}
		r.applied = ents[len(ents)-1].Index
	}

	cfg := &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   r.storage,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           128,
		MaxUncommittedEntriesSize: 1 << 30,
		Applied:                   r.applied,
	}

	// seed peers if new cluster and not joining
	var rpeers []raft.Peer
	if len(ents) == 0 && !join {
		rpeers = make([]raft.Peer, 0, len(peers))
		for pid := range peers {
			rpeers = append(rpeers, raft.Peer{ID: pid})
		}
	}

	if len(ents) > 0 || join {
		r.node = raft.RestartNode(cfg)
	} else {
		r.node = raft.StartNode(cfg, rpeers)
	}

	// custom TCP transport
	tr, err := NewTCPTransport(r.id, r.address, r.peers, r.Step)
	if err != nil {
		return nil, nil, fmt.Errorf("tcp transport: %w", err)
	}
	r.transport = tr

	go r.loop()
	return r, r.errorC, nil
}

func (r *RaftNode) Stop() { close(r.stopc) }

func (r *RaftNode) Step(m raftpb.Message) { _ = r.node.Step(context.TODO(), m) }

// ProposePut / ProposeDelete forward to Raft (you said IronDB has helpers now;
func (r *RaftNode) ProposePut(key string, value []byte) error {
	cmd := Command{Op: "put", Key: key, Value: value}
	b, _ := json.Marshal(cmd)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return r.node.Propose(ctx, b)
}
func (r *RaftNode) ProposeDelete(key string) error {
	cmd := Command{Op: "delete", Key: key}
	b, _ := json.Marshal(cmd)
	return r.node.Propose(context.TODO(), b)
}

func (r *RaftNode) loop() {
	defer r.wal.Close()
	defer r.ticker.Stop()

	for {
		select {
		case <-r.ticker.C:
			r.node.Tick()

		case rd := <-r.node.Ready():
			// 1) persist hardstate + entries
			if err := r.wal.Save(rd.HardState, rd.Entries); err != nil {
				r.fail(fmt.Errorf("wal save: %w", err))
				return
			}
			// 2) reflect in storage
			if len(rd.Entries) > 0 {
				if err := r.storage.Append(rd.Entries); err != nil {
					r.fail(fmt.Errorf("storage append: %w", err))
					return
				}
			}
			// 3) send raft messages
			r.transport.Send(rd.Messages)
			// 4) apply committed
			if err := r.applyCommitted(rd.CommittedEntries); err != nil {
				r.fail(fmt.Errorf("apply: %w", err))
				return
			}
			// 5) advance
			r.node.Advance()

		case <-r.stopc:
			if r.node != nil {
				r.node.Stop()
			}
			return
		}
	}
}

func (r *RaftNode) applyCommitted(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].Index
	if first > r.applied+1 {
		return fmt.Errorf("first committed %d > applied+1 %d", first, r.applied+1)
	}
	var toApply []raftpb.Entry
	if r.applied-first+1 < uint64(len(ents)) {
		toApply = ents[r.applied-first+1:]
	}

	for _, e := range toApply {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				continue
			}
			var cmd Command
			if err := json.NewDecoder(bytes.NewReader(e.Data)).Decode(&cmd); err != nil {
				log.Printf("apply decode: %v", err)
				continue
			}
			switch cmd.Op {
			case "put":
				if err := r.db.ApplyPut(cmd.Key, string(cmd.Value)); err != nil {
					log.Printf("ApplyPut: %v", err)
				}
			case "delete":
				if err := r.db.ApplyDelete(cmd.Key); err != nil {
					log.Printf("ApplyDelete: %v", err)
				}
			default:
				log.Printf("unknown op %q", cmd.Op)
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				log.Printf("confchange decode: %v", err)
				continue
			}
			r.node.ApplyConfChange(cc)
			// NOTE: if you carry peer address in cc.Context, update r.transport peers here.
		}
	}

	r.applied = ents[len(ents)-1].Index
	return nil
}

func (r *RaftNode) fail(err error) {
	log.Printf("raft fatal: %v", err)
	select {
	case r.errorC <- err:
	default:
	}
	r.Stop()
}
