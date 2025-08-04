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
