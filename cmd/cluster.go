package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/udbhav1990/IronDB/internal/kv"
	"github.com/udbhav1990/IronDB/internal/raft"
)

// peers flag format: "1=127.0.0.1:9001,2=127.0.0.1:9002"
func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	parts := strings.Split(s, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		kvp := strings.SplitN(p, "=", 2)
		if len(kvp) != 2 {
			return nil, fmt.Errorf("bad peer item %q, want id=addr", p)
		}
		var id uint64
		_, err := fmt.Sscanf(kvp[0], "%d", &id)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad peer id in %q", p)
		}
		out[id] = kvp[1]
	}
	return out, nil
}

func main() {
	var (
		id      = flag.Uint64("id", 1, "node id (unique, >0)")
		addr    = flag.String("addr", "127.0.0.1:9001", "raft tcp listen address")
		peersS  = flag.String("peers", "1=127.0.0.1:9001,2=127.0.0.1:9002", "comma-separated peers: id=host:port,... (must include self)")
		dataDir = flag.String("data", "./data/node1", "data dir for this node")
		join    = flag.Bool("join", false, "join existing cluster (start with empty peer list in raft storage)")
	)
	flag.Parse()

	peers, err := parsePeers(*peersS)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}
	if peers[*id] != *addr {
		log.Printf("warning: peers list has addr %q for id %d; overriding with %q", peers[*id], *id, *addr)
		peers[*id] = *addr
	}

	// 1) Value WAL (stores actual bytes)
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}

	// 2) IronDB in Raft mode (do NOT replay value WAL)
	db, err := kv.NewIronDBR(*dataDir, kv.Options{RaftMode: true})
	if err != nil {
		log.Fatalf("init IronDB: %v", err)
	}
	defer db.Close()

	// 3) Start Raft node with TCP transport
	rn, errC, err := raft.NewRaftNode(
		*id,
		*addr,
		peers,
		*dataDir,
		db,
		*join,
	)
	if err != nil {
		log.Fatalf("start raft: %v", err)
	}

	// 4) Tiny REPL for manual testing (no HTTP). Use on ONE node (the one you type into).
	fmt.Println("Ready. Commands: put <k> <v> | get <k> | del <k> | quit")
	go func() {
		in := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("> ")
			if !in.Scan() {
				return
			}
			line := strings.TrimSpace(in.Text())
			if line == "" {
				continue
			}
			args := strings.Fields(line)
			switch strings.ToLower(args[0]) {
			case "quit", "exit":
				rn.Stop()
				return
			case "put":
				if len(args) < 3 {
					fmt.Println("usage: put <key> <value>")
					continue
				}
				key := args[1]
				value := strings.Join(args[2:], " ")
				log.Printf("put %q=%q", key, value)
				if err := rn.ProposePut(key, []byte(value)); err != nil {
					fmt.Println("error:", err)
				} else {
					// small wait to let commit apply (for demo only)
					time.Sleep(150 * time.Millisecond)
					if v, ok := db.Get(key); ok {
						fmt.Printf("OK (read after put) %q=%q\n", key, v)
					} else {
						fmt.Println("OK proposed; value not visible yet")
					}
				}
			case "get":
				if len(args) != 2 {
					fmt.Println("usage: get <key>")
					continue
				}
				if v, ok := db.Get(args[1]); ok {
					fmt.Printf("%q\n", v)
				} else {
					fmt.Println("(not found)")
				}
			case "del", "delete":
				if len(args) != 2 {
					fmt.Println("usage: del <key>")
					continue
				}
				if err := rn.ProposeDelete(args[1]); err != nil {
					fmt.Println("error:", err)
				} else {
					time.Sleep(150 * time.Millisecond)
					if _, ok := db.Get(args[1]); !ok {
						fmt.Println("OK (deleted)")
					} else {
						fmt.Println("OK proposed; still visible (eventual)")
					}
				}
			default:
				fmt.Println("unknown command")
			}
		}
	}()

	// 5) fatal errors from raft loop
	if err := <-errC; err != nil {
		log.Fatalf("raft error: %v", err)
	}
}
