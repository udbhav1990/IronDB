package replicaloop

import (
	"encoding/json"
	"fmt"
)

type Command struct {
	Op     string `json:"op"`  // "put" or "delete"
	Key    string `json:"key"` // Key to mutate
	Offset int64  // offset into WAL or value file
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	if err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	return cmd, nil
}
