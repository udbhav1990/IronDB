package replicaloop

import (
	"context"
	"fmt"
	"time"

	"github.com/udbhav1990/IronDB/internal/kv"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RaftNode struct {
	id       uint64
	node     raft.Node
	storage  *RaftStorage
	fsm      *FSM
	wal      *kv.WAL
	ticker   *time.Ticker
	ctx      context.Context
	cancel   context.CancelFunc
	peers    []raft.Peer
	proposeC chan []byte // proposals come here
	commitC  chan []byte // applied entries go here
}

func NewRaftNode(id uint64, wal *kv.WAL, db *kv.IronDB, peers []raft.Peer) *RaftNode {
	storage := NewRaftStorage()
	fsm := NewFSM(db, wal)

	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	ctx, cancel := context.WithCancel(context.Background())
	node := raft.StartNode(c, peers)

	return &RaftNode{
		id:       id,
		node:     node,
		storage:  storage,
		fsm:      fsm,
		wal:      wal,
		ticker:   time.NewTicker(100 * time.Millisecond),
		ctx:      ctx,
		cancel:   cancel,
		peers:    peers,
		proposeC: make(chan []byte),
		commitC:  make(chan []byte),
	}
}

// Run starts the Raft loop (tick + ready processing)
func (rn *RaftNode) Run() {
	go rn.serveProposals()

	for {
		select {
		case <-rn.ticker.C:
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			// 1. Persist log entries
			rn.storage.AppendEntries(rd.Entries)

			// 2. Apply committed entries to FSM
			for _, ent := range rd.CommittedEntries {
				if ent.Type == raftpb.EntryNormal && len(ent.Data) > 0 {
					rn.fsm.Apply(ent)
				}
			}

			rn.node.Advance()
		case <-rn.ctx.Done():
			return
		}
	}
}

// serveProposals listens for local writes to replicate
func (rn *RaftNode) serveProposals() {
	for data := range rn.proposeC {
		rn.node.Propose(rn.ctx, data)
	}
}

// Propose sends a new command into Raft
func (rn *RaftNode) Propose(cmd []byte) error {
	select {
	case rn.proposeC <- cmd:
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("proposal timeout")
	}
}

// Stop shuts down the Raft node
func (rn *RaftNode) Stop() {
	rn.cancel()
	rn.node.Stop()
}
