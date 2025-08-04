package kv

import (
	"sort"
	"sync"
)

// create a memtable test file to test the memtable implementation

// MemTable is an in-memory key-value store that supports basic operations
type Value struct {
	Data      string
	Tombstone bool
}

type MemTable struct {
	mu   *sync.RWMutex
	data map[string]Value
}

// NewMemTable creates a new MemTable instance
func NewMemTable() *MemTable {
	return &MemTable{
		mu:   &sync.RWMutex{},
		data: make(map[string]Value),
	}
}

// Put adds or updates a key-value pair in the MemTable
func (mt *MemTable) Put(key string, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.data[key] = Value{Data: value, Tombstone: false}
}

// Get retrieves a value by key from the MemTable
func (mt *MemTable) Get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if val, exists := mt.data[key]; exists && !val.Tombstone {
		return val.Data, true
	}
	return "", false
}

// Delete marks a key as deleted in the MemTable
func (mt *MemTable) Delete(key string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if val, exists := mt.data[key]; exists {
		val.Tombstone = true
		mt.data[key] = val
	}
}

// Exists checks if a key exists in the MemTable
func (mt *MemTable) Exists(key string) bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if val, exists := mt.data[key]; exists && !val.Tombstone {
		return true
	}
	return false
}

func (m *MemTable) Range(start, end string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect and sort keys
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		if k >= start && k <= end {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	result := make(map[string]string)
	for _, k := range keys {
		val := m.data[k]
		if !val.Tombstone {
			result[k] = val.Data
		}
	}
	return result
}

// Size returns the number of key-value pairs in the MemTable
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// Clear removes all entries from the MemTable
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]Value)
}

// Keys returns a sorted list of keys in the MemTable
func (m *MemTable) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		if !m.data[k].Tombstone {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Implement a Flush method to SStable
func (m *MemTable) Flush() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a new map to hold the flushed data
	flushedData := make(map[string]string)

	for k, v := range m.data {
		if !v.Tombstone {
			flushedData[k] = v.Data
		}
	}

	// Clear the MemTable after flushing
	m.Clear()

	return flushedData
}
