package replicaloop

import (
	"fmt"

	"github.com/udbhav1990/IronDB/internal/kv"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type FSM struct {
	db  *kv.IronDB
	wal *kv.WAL
}

func NewFSM(db *kv.IronDB, wal *kv.WAL) *FSM {
	return &FSM{db: db, wal: wal}
}

// Apply is called when a log entry is committed
func (f *FSM) Apply(entry raftpb.Entry) interface{} {
	cmd, err := DecodeCommand(entry.Data)
	if err != nil {
		fmt.Printf("FSM apply: decode error: %v\n", err)
		return nil
	}

	switch cmd.Op {
	case "put":
		val, err := f.wal.ReadAt(cmd.Offset)
		if err != nil {
			fmt.Printf("FSM apply: wal read error: %v\n", err)
			return nil
		}
		err = f.db.Put(cmd.Key, val)
		if err != nil {
			fmt.Printf("FSM apply: db put error: %v\n", err)
		}
	case "delete":
		err := f.db.Delete(cmd.Key)
		if err != nil {
			fmt.Printf("FSM apply: db delete error: %v\n", err)
		}
	default:
		fmt.Printf("FSM apply: unknown command op %s\n", cmd.Op)
	}

	return nil
}
