package kv

import "sync"

type MemTable struct {
	mu sync.RWMutex
}
