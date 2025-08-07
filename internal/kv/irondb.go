package kv

import (
	"fmt"
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
		switch op {
		case OpPut:
			mem.Put(string(key), string(value))
		case OpDelete:
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
	if _, err := db.wal.WritePut([]byte(key), []byte(value)); err != nil {
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

func (db *IronDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.wal.WriteDelete([]byte(key)); err != nil {
		return err
	}

	db.memTable.Delete(key)

	if db.memTable.Size() >= db.flushLimit {
		return db.flushMemTable()
	}
	return nil
}

func (db *IronDB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check MemTable
	if val, ok := db.memTable.Get(key); ok {
		return val, true
	}

	// Check SSTables (newest to oldest)
	for _, sst := range db.sstables {
		if !sst.filter.Test([]byte(key)) {
			continue
		}
		val, ok, _ := sst.Get(key)
		if ok {
			return val, true
		}
	}
	return "", false
}

func (db *IronDB) ReadKeyRange(start, end string) map[string]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := db.memTable.Range(start, end)
	for _, sst := range db.sstables {
		sstData := sst.Range(start, end)
		for k, v := range sstData {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result
}

func (db *IronDB) BatchPut(keys, values []string) error {
	if len(keys) != len(values) {
		return fmt.Errorf("key and value count mismatch")
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	for i := range keys {
		db.wal.WritePut([]byte(keys[i]), []byte(values[i]))
		db.memTable.Put(keys[i], values[i])
	}

	if db.memTable.Size() >= db.flushLimit {
		return db.flushMemTable()
	}
	return nil
}

func (db *IronDB) flushMemTable() error {
	sstFile := filepath.Join(db.dataDir, fmt.Sprintf("sst_%06d.db", db.nextSSTID))
	sst, err := CreateSSTableFromMemTable(db.memTable, sstFile)
	if err != nil {
		return err
	}
	db.sstables = append([]*SSTable{sst}, db.sstables...)
	db.memTable = NewMemTable()
	db.nextSSTID++
	_ = db.wal.Reset() // optional: rotate WAL here
	return nil
}

func (db *IronDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_ = db.flushMemTable()
	for _, sst := range db.sstables {
		sst.Close()
	}
	return db.wal.Close()
}
