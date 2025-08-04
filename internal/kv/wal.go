package kv

import (
	"encoding/binary"
	"os"
	"sync"
)

const (
	OpPut    byte = 0
	OpDelete byte = 1
)

type WAL struct {
	file *os.File
	mu   *sync.Mutex
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

func (w *WAL) WritePut(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := []byte{OpPut}
	keyLen := make([]byte, 4)
	valLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))
	binary.BigEndian.PutUint32(valLen, uint32(len(value)))

	record = append(record, keyLen...)
	record = append(record, valLen...)
	record = append(record, key...)
	record = append(record, value...)

	_, err := w.file.Write(record)
	return err
}

func (w *WAL) WriteDelete(key []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := []byte{OpDelete}
	keyLen := make([]byte, 4)
	valLen := make([]byte, 4) // 0 for delete
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))

	record = append(record, keyLen...)
	record = append(record, valLen...)
	record = append(record, key...)

	_, err := w.file.Write(record)
	return err
}

func (w *WAL) Close() error {
	return w.file.Close()
}
