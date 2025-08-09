package kv

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	OpPut    byte = 0
	OpDelete byte = 1
)

type WAL struct {
	file      *os.File
	mu        *sync.Mutex
	lastState raftpb.HardState
}

// ReadAt reads the value at a given byte offset in the WAL
func (w *WAL) ReadAt(offset int64) (string, error) {
	_, err := w.file.Seek(offset, 0)
	if err != nil {
		return "", fmt.Errorf("wal seek error: %w", err)
	}

	var keyLen, valLen uint32

	if err := binary.Read(w.file, binary.BigEndian, &keyLen); err != nil {
		return "", fmt.Errorf("read keyLen: %w", err)
	}
	if err := binary.Read(w.file, binary.BigEndian, &valLen); err != nil {
		return "", fmt.Errorf("read valLen: %w", err)
	}

	key := make([]byte, keyLen)
	value := make([]byte, valLen)

	if _, err := w.file.Read(key); err != nil {
		return "", fmt.Errorf("read key: %w", err)
	}
	if _, err := w.file.Read(value); err != nil {
		return "", fmt.Errorf("read value: %w", err)
	}

	return string(value), nil
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
		mu:   &sync.Mutex{},
	}, nil
}

func (w *WAL) WritePut(key, value []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Capture current offset
	offset, err := w.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}

	record := []byte{OpPut}
	keyLen := make([]byte, 4)
	valLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))
	binary.BigEndian.PutUint32(valLen, uint32(len(value)))

	record = append(record, keyLen...)
	record = append(record, valLen...)
	record = append(record, key...)
	record = append(record, value...)

	_, err = w.file.Write(record)
	return offset, err
}

func (w *WAL) WriteDelete(key []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, err := w.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}

	record := []byte{OpDelete}
	keyLen := make([]byte, 4)
	valLen := make([]byte, 4) // 0 for delete
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))
	binary.BigEndian.PutUint32(valLen, 0)

	record = append(record, keyLen...)
	record = append(record, valLen...)
	record = append(record, key...)

	_, err = w.file.Write(record)
	return offset, err
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Replay(callback func(op byte, key, value []byte)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}

	buf := make([]byte, 1+4+4) // header
	for {
		_, err := w.file.Read(buf)
		if err != nil {
			break
		}

		op := buf[0]
		keyLen := binary.BigEndian.Uint32(buf[1:5])
		valLen := binary.BigEndian.Uint32(buf[5:9])

		key := make([]byte, keyLen)
		_, err = w.file.Read(key)
		if err != nil {
			break
		}

		var value []byte
		if op == OpPut {
			value = make([]byte, valLen)
			_, err = w.file.Read(value)
			if err != nil {
				break
			}
		}

		callback(op, key, value)
	}
	return nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get current file name
	path := w.file.Name()

	// Close the file
	if err := w.file.Close(); err != nil {
		return err
	}

	// Optional: archive it (e.g., rename with timestamp or suffix)
	backupPath := path + ".bak"
	os.Rename(path, backupPath)

	// Reopen a new WAL file with the same name (truncate)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.file = f
	return nil
}

// WriteRawEntry writes raw raft entry data (you can extend this with op codes if needed)
func (w *WAL) WriteRawEntry(data []byte) error {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))

	if _, err := w.file.Write(length); err != nil {
		return err
	}
	_, err := w.file.Write(data)
	return err
}

// WriteReady persists entries and hard state
func (w *WAL) WriteReady(rd raft.Ready) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Persist HardState if non-empty
	if !raft.IsEmptyHardState(rd.HardState) {
		// Example: you might choose to serialize this to a separate file for durability
		w.lastState = rd.HardState
	}

	// Write entries
	for _, entry := range rd.Entries {
		if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			if err := w.WriteRawEntry(entry.Data); err != nil {
				return err
			}
		}
	}

	// Optional: Handle snapshot persistence here
	return nil
}
