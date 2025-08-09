package replicaloop

import (
	"context"
	"fmt"
	"log"
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

// Run is the main event loop of the Raft node
func (r *RaftNode) Run(transport *TCPTransport) {
	defer r.ticker.Stop()

	for {
		select {
		case <-r.ticker.C:
			// Tick Raft (advances timeouts)
			r.node.Tick()

		case rd := <-r.node.Ready():
			// Step 1: Persist to WAL
			if err := r.wal.WriteReady(rd); err != nil {
				log.Printf("error writing to WAL: %v", err)
				continue
			}

			// Step 3: Append new entries to storage
			r.storage.AppendEntries(rd.Entries)

			// Step 4: Send messages to peers
			transport.Send(rd.Messages)

			// Step 5: Apply committed entries to FSM (IronDB)
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					if len(entry.Data) > 0 {
						if err := r.fsm.Apply(entry); err != nil {
							log.Printf("fsm apply failed: %v", err)
						}
					}
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						log.Printf("conf change unmarshal failed: %v", err)
						continue
					}
					r.node.ApplyConfChange(cc)
				}
			}

			// Step 6: Advance the Raft node
			r.node.Advance()

		case <-r.ctx.Done():
			log.Printf("RaftNode %d shutting down", r.id)
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

func (r *RaftNode) Step(msg raftpb.Message) {
	_ = r.node.Step(r.ctx, msg)
}
