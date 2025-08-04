package kv

type SSTable struct {
	filePath string
}

func NewSSTable(filePath string) (*SSTable, error) {
	return &SSTable{filePath: filePath}, nil
}
