package kv

import (
	"os"
	"path/filepath"
	"sync"
)

type IronDB struct {
	memTable  *MemTable
	wal       *WAL
	sstables  []*SSTable // newest to oldest
	dataDir   string
	nextSSTID int
	compactCh chan struct{}
	//consistency ConsistencyLevel

	mu         sync.RWMutex
	flushLimit int // max entries before flush
}

func NewIronDB(dataDir string) (*IronDB, error) {
	os.MkdirAll(dataDir, 0755)

	wal, err := NewWAL(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, err
	}

	mem := NewMemTable()

	// Replay WAL into MemTable
	err = wal.Replay(func(op byte, key, value []byte) {
		if op == OpPut {
			mem.Put(string(key), string(value))
		} else if op == OpDelete {
			mem.Delete(string(key))
		}
	})
	if err != nil {
		return nil, err
	}

	// Load existing SSTables (optional for now)
	var ssts []*SSTable
	files, _ := filepath.Glob(filepath.Join(dataDir, "sst_*.db"))
	for _, file := range files {
		sst, _ := LoadSSTable(file)
		ssts = append(ssts, sst)
	}

	return &IronDB{
		memTable:  mem,
		wal:       wal,
		sstables:  ssts,
		dataDir:   dataDir,
		nextSSTID: len(ssts),
		//consistency: consistency,
		flushLimit: 1000,
	}, nil
}

// Put adds or updates a key-value pair in the IronDB
func (db *IronDB) Put(key, value string) error {
	// First Write to wal and then to memtable
	if err := db.wal.WritePut([]byte(key), []byte(value)); err != nil {
		return err
	} else {
		db.mu.Lock()
		defer db.mu.Unlock()
		// Write to memtable
		db.memTable.Put(key, value)
	}

	if len(db.memTable.data) > 1000 { // Example threshold
		select {
		case db.compactCh <- struct{}{}:
		default:
			// Already compacting
		}
	}
	return nil
}
