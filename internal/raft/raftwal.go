package raft

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

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
