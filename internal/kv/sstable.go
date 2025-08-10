package kv

import (
	"encoding/binary"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
)

type SSTable struct {
	FilePath string
	index    map[string]int64 // key → byte offset in file
	file     *os.File
	filter   *bloom.BloomFilter
}

func CreateSSTableFromMemTable(mem *MemTable, filePath string) (*SSTable, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	index := make(map[string]int64)
	filter := bloom.NewWithEstimates(100000, 0.01) // 100k items, 1% false positive rate

	// Extract and sort keys
	keys := mem.SortedKeys() // implement this helper
	for _, key := range keys {
		val, ok := mem.GetRaw(key)
		if !ok || val.Tombstone {
			continue
		}

		// Mark current offset
		offset, _ := f.Seek(0, os.SEEK_CUR)
		index[key] = offset
		filter.Add([]byte(key))

		// Write record: key length + value length + key + value
		writeKV(f, key, val.Data)
	}

	return &SSTable{
		FilePath: filePath,
		index:    index,
		file:     f,
		filter:   filter,
	}, nil
}

func (s *SSTable) Get(key string) (string, bool, error) {
	if s.filter != nil && !s.filter.Test([]byte(key)) {
		return "", false, nil
	}

	offset, ok := s.index[key]
	if !ok {
		return "", false, nil
	}

	_, err := s.file.Seek(offset, 0)
	if err != nil {
		return "", false, err
	}

	return readKV(s.file)
}

func (s *SSTable) Range(start, end string) map[string]string {
	result := make(map[string]string)
	for k, offset := range s.index {
		if k >= start && k <= end {
			s.file.Seek(offset, 0)
			v, _, _ := readKV(s.file)
			result[k] = v
		}
	}
	return result
}

func writeKV(f *os.File, key, value string) error {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	binary.Write(f, binary.BigEndian, keyLen)
	binary.Write(f, binary.BigEndian, valLen)
	f.Write([]byte(key))
	f.Write([]byte(value))
	return nil
}

func readKV(f *os.File) (string, bool, error) {
	var keyLen, valLen uint32
	if err := binary.Read(f, binary.BigEndian, &keyLen); err != nil {
		return "", false, err
	}
	if err := binary.Read(f, binary.BigEndian, &valLen); err != nil {
		return "", false, err
	}

	key := make([]byte, keyLen)
	val := make([]byte, valLen)
	f.Read(key)
	f.Read(val)

	return string(val), true, nil
}

func LoadSSTable(filePath string) (*SSTable, error) {

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	index := make(map[string]int64)
	filter := bloom.NewWithEstimates(100000, 0.01)
	offset := int64(0)

	for {
		// Read record header
		var keyLen, valLen uint32
		err := binary.Read(f, binary.BigEndian, &keyLen)
		if err != nil {
			break
		}
		binary.Read(f, binary.BigEndian, &valLen)

		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		f.Read(key)
		f.Read(val)

		index[string(key)] = offset
		filter.Add(key)
		offsetNow, _ := f.Seek(0, os.SEEK_CUR)
		offset = offsetNow
	}

	return &SSTable{
		FilePath: filePath,
		index:    index,
		file:     f,
		filter:   filter,
	}, nil
}

func (s *SSTable) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
