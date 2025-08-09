package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Command is the compact record you replicate via Raft.
// Values live in the WAL; Raft only replicates metadata.
type Command struct {
	Op    string `json:"op"` // "put" | "delete"
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"` // present for "put"
}

// Proposer is how IronDB emits a Raft proposal (string payload).
// You pass a proposer func (or a chan-wrapper) from the raft layer.
type Proposer func(string) error

type IronDBR struct {
	mu       sync.RWMutex
	memTable *MemTable
	//wal       *WAL
	sstables  []*SSTable // newest to oldest
	dataDir   string
	nextSSTID int

	flushLimit int  // memtable flush threshold
	raftMode   bool // if true, skip WAL replay at startup

	proposer Proposer // optional; set via SetProposer
}

// -------- Construction --------

type Options struct {
	FlushLimit int
	RaftMode   bool // if true, skip value-WAL replay into memtable on startup
}

func defaultOptions() Options {
	return Options{
		FlushLimit: 1000,
		RaftMode:   false,
	}
}

func NewIronDBR(dataDir string, opts ...Options) (*IronDBR, error) {
	_ = os.MkdirAll(dataDir, 0755)

	o := defaultOptions()
	if len(opts) > 0 {
		o = opts[0]
	}

	mem := NewMemTable()

	var ssts []*SSTable
	files, _ := filepath.Glob(filepath.Join(dataDir, "sst_*.db"))
	for _, file := range files {
		if sst, err := LoadSSTable(file); err == nil {
			ssts = append(ssts, sst)
		}
	}

	return &IronDBR{
		memTable: mem,
		//wal:        wal,
		sstables:   ssts,
		dataDir:    dataDir,
		nextSSTID:  len(ssts),
		flushLimit: o.FlushLimit,
		raftMode:   o.RaftMode,
	}, nil
}

// Optional: supply a proposer later (e.g., wraps sending to proposeC).
func (db *IronDBR) SetProposer(p Proposer) { db.proposer = p }

// -------- Propose path (leader-side helpers) --------
// These DO write to WAL to get the value offset, then call proposer
// to replicate a compact command. They DO NOT touch the memtable.

func (db *IronDBR) ProposePut(key, value string) error {
	if db.proposer == nil {
		return errors.New("proposer not set")
	}
	cmd := Command{Op: "put", Key: key, Value: []byte(value)}
	b, _ := json.Marshal(cmd)

	return db.proposer(string(b))
}

func (db *IronDBR) ProposeDelete(key string) error {
	if db.proposer == nil {
		return errors.New("proposer not set")
	}
	cmd := Command{Op: "delete", Key: key}
	b, _ := json.Marshal(cmd)
	return db.proposer(string(b))
}

// -------- Commit/apply path (all nodes, incl leader) --------
// These are called when a raft Command is committed. NO WAL writes here.

func (db *IronDBR) ApplyPut(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.memTable.Put(key, value)
	if db.memTable.Size() >= db.flushLimit {
		return db.flushMemTableLocked()
	}
	return nil
}

func (db *IronDBR) ApplyDelete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.memTable.Delete(key)
	if db.memTable.Size() >= db.flushLimit {
		return db.flushMemTableLocked()
	}
	return nil
}

// -------- Reads --------

func (db *IronDBR) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, ok := db.memTable.Get(key); ok {
		return val, true
	}
	for _, sst := range db.sstables { // newest to oldest
		// Optional bloom filter check
		if sst.filter != nil && !sst.filter.Test([]byte(key)) {
			continue
		}
		if v, ok, _ := sst.Get(key); ok {
			return v, true
		}
	}
	return "", false
}

func (db *IronDBR) ReadKeyRange(start, end string) map[string]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	out := db.memTable.Range(start, end)
	for _, sst := range db.sstables {
		chunk := sst.Range(start, end)
		for k, v := range chunk {
			if _, exists := out[k]; !exists {
				out[k] = v
			}
		}
	}
	return out
}

// -------- Flush / Close --------

func (db *IronDBR) flushMemTableLocked() error {
	sstFile := filepath.Join(db.dataDir, fmt.Sprintf("sst_%06d.db", db.nextSSTID))
	sst, err := CreateSSTableFromMemTable(db.memTable, sstFile)
	if err != nil {
		return err
	}
	db.sstables = append([]*SSTable{sst}, db.sstables...)
	db.memTable = NewMemTable()
	db.nextSSTID++
	// Value WAL rotation/GC policy can be handled elsewhere.
	return nil
}

func (db *IronDBR) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushMemTableLocked()
}

func (db *IronDBR) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_ = db.flushMemTableLocked()
	for _, sst := range db.sstables {
		_ = sst.Close()
	}

	return nil
}

// -------- Snapshot Export/Import (for raft snapshots) --------

// GetSnapshotBytes returns a JSON snapshot of the *current visible state*.
// Naive but correct: merge newest→oldest SSTables, then overlay memtable.
func (db *IronDBR) GetSnapshotBytes() ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Build from SSTables (newest wins), then overlay MemTable (latest)
	state := make(map[string]string, db.memTable.Size())

	// Merge SSTables newest→oldest, first write wins
	for _, sst := range db.sstables {
		for _, k := range sst.Keys() { // helper; see below
			if _, exists := state[k]; exists {
				continue
			}
			if v, ok, _ := sst.Get(k); ok {
				state[k] = v
			}
		}
	}

	// Overlay MemTable
	for k, v := range db.memTable.data {
		if v.Tombstone {
			delete(state, k)
			continue
		}
		state[k] = v.Data
	}

	return json.Marshal(state)
}

// LoadSnapshotBytes replaces current in-memory view with the snapshot.
// (Persisted SSTables on disk are left as-is; for full compaction you could
// write a new SSTable from the snapshot, but this keeps it simple.)
func (db *IronDBR) LoadSnapshotBytes(snap []byte) error {
	var m map[string]string
	if len(snap) == 0 {
		// empty snapshot means clear memtable
		db.mu.Lock()
		db.memTable = NewMemTable()
		db.mu.Unlock()
		return nil
	}
	if err := json.Unmarshal(snap, &m); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	mt := NewMemTable()
	for k, v := range m {
		mt.Put(k, v)
	}
	db.memTable = mt
	return nil
}

// -------- Helpers you may add to SSTable for snapshotting --------

// Keys returns all keys known to the SSTable index.
// If you don't have this, implement by iterating s.index (as you showed).
func (s *SSTable) Keys() []string {
	keys := make([]string, 0, len(s.index))
	for k := range s.index {
		keys = append(keys, k)
	}
	return keys
}
