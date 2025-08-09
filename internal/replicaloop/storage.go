package replicaloop

import (
	"errors"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

var ErrUnavailable = errors.New("requested entry unavailable")

type RaftStorage struct {
	mu       sync.RWMutex
	entries  []raftpb.Entry // Lightweight entries (no values)
	snapshot raftpb.Snapshot
}

// NewRaftStorage initializes raft storage with dummy entry
func NewRaftStorage() *RaftStorage {
	return &RaftStorage{
		entries: []raftpb.Entry{{Term: 0, Index: 0}}, // dummy for offset logic
	}
}

// AppendEntries adds raft entries (Command with offset, no values)
func (s *RaftStorage) AppendEntries(newEntries []raftpb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ent := range newEntries {
		lastIndex, _ := s.LastIndex()
		if ent.Index <= lastIndex {
			continue // skip duplicates
		}
		s.entries = append(s.entries, ent)
	}
}

func (s *RaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return raftpb.HardState{
			Term:   1,
			Commit: 0, // very important: must be ≤ LastIndex()
		}, raftpb.ConfState{
			Voters: []uint64{1, 2}, // must match your cluster
		}, nil
}

// Entries returns raft entries in [lo, hi)
func (s *RaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return nil, err
	}

	if lo < firstIndex || hi > lastIndex+1 {
		return nil, ErrUnavailable
	}

	var result []raftpb.Entry
	var size uint64

	for i := lo; i < hi; i++ {
		ent := s.entries[i]
		size += uint64(ent.Size())
		if maxSize > 0 && size > maxSize {
			break
		}
		result = append(result, ent)
	}

	return result, nil
}

// Term returns the term of a given log index.
func (s *RaftStorage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return 0, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return 0, err
	}

	if i < firstIndex || i > lastIndex {
		return 0, ErrUnavailable
	}

	// offset-based indexing
	return s.entries[i-firstIndex].Term, nil
}

// FirstIndex returns the index of the first log entry.
func (s *RaftStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return 0, ErrUnavailable
	}
	return s.entries[0].Index, nil
}

// LastIndex returns the index of the last log entry.
func (s *RaftStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return 0, ErrUnavailable
	}
	return s.entries[len(s.entries)-1].Index, nil
}

// Snapshot returns latest snapshot
func (s *RaftStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot.Metadata.Index == 0 {
		return raftpb.Snapshot{}, ErrUnavailable
	}
	return s.snapshot, nil
}
