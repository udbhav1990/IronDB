package kv

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type IronDB struct {
	memTable   *MemTable     // In-memory sorted map
	wal        *WAL          // Write-ahead log for durability
	sstables   []*SSTable    // Immutable on-disk files
	mu         sync.RWMutex  // Protect concurrent access
	dataDir    string        // Path to store WAL and SSTables
	compactCh  chan struct{} // Trigger compaction
	shutdownCh chan struct{} // Graceful shutdown
}

func NewIronDB(dataDir string) *IronDB {

	// Load existing SSTables
	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatalf("Failed to read data directory: %v", err)
	}

	sstables := make([]*SSTable, 0)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sst") {
			sstable, err := NewSSTable(filepath.Join(dataDir, file.Name()))
			if err != nil {
				log.Fatalf("Failed to load SSTable: %v", err)
			}
			sstables = append(sstables, sstable)
		}
	}

	return &IronDB{
		//memTable:  NewMemTable(),
		//wal:       NewWAL(dataDir),
		sstables:  sstables,
		mu:        sync.RWMutex{},
		dataDir:   dataDir,
		compactCh: make(chan struct{}),
	}

}
