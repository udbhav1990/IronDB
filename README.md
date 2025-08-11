# IronDB Raft Cluster — Quick Start & REPL Guide

This document shows how to **build**, **run**, and **use the REPL** for IronDB’s Raft-based cluster. Everything is in one place so you can paste this into your README or keep it as a standalone doc.

---

## Prerequisites

- Go 1.22+
- macOS or Linux terminal
- This repository cloned locally

---

## Build

From the repository root:

```bash
make build
```

This compiles `cmd/cluster.go` and creates the binary at `./bin/irondb-cluster`.

---

## Run Modes (Foreground)

All run targets keep the node **in the foreground** so you can use its REPL directly. Open **one terminal per node** for multi-node clusters.

### Single node (dev)

```bash
make single
```

- Node ID: `1`
- Address: `127.0.0.1:9101`
- Data dir: `./data/node1`

### 3-node cluster (run each in its own terminal)

**Terminal 1**
```bash
make node1
```

**Terminal 2**
```bash
make node2
```

**Terminal 3**
```bash
make node3
```

- Node 1 → `id=1`, `addr=127.0.0.1:9101`, `data=./data/node1`
- Node 2 → `id=2`, `addr=127.0.0.1:9102`, `data=./data/node2`
- Node 3 → `id=3`, `addr=127.0.0.1:9103`, `data=./data/node3`

> Tip: Start `node1` first, then `node2`, then `node3`.

---

## Cleaning

- Clean binaries and data:
```bash
make clean
```

- Clean only data (keep binary):
```bash
make clean-data
```

- Clean only binary (keep data):
```bash
make clean-bin
```

---

## REPL — Supported Commands

When a node starts, it prints a prompt:

```
>
```

Use these commands (type into the node’s terminal):

### Write a single key
```text
put <key> <value>
```
Example:
```text
put user1 Alice
```

### Read a single key
```text
get <key>
```
Example:
```text
get user1
```

### Delete a key
```text
del <key>
```
or
```text
delete <key>
```
Example:
```text
del user1
```

### Read an inclusive key range
```text
readkeyrange <startKey> <endKey>
```
Alias:
```text
range <startKey> <endKey>
```
Example:
```text
readkeyrange a z
```

### Quit the node
```text
quit
```
or
```text
exit
```

> Notes:
> - Quoted arguments are supported if keys/values contain spaces, e.g. `put "user 1" "Alice A"`.
> - Write operations (`put`, `del`) must be sent to the **leader** node. In **single-node** mode the node becomes leader automatically.

---

## Example Sessions

### Single node

```bash
make clean-data
make single
```

REPL:
```
> put name Alice
> get name
"Alice"
> del name
> get name
(not found)
```

### 3-node cluster

```bash
# Terminal 1
make node1
# Terminal 2
make node2
# Terminal 3
make node3
```

From the **leader** node’s REPL:
```
> readkeyrange a z
a=1
b=2
c key=value 3
```

---

## Troubleshooting Quick Notes

- If you restart a node and see logs like `voters=()`, the node started with a WAL that has no persisted voter set (ConfState). For dev, you can do:
  ```bash
  make clean-data
  ```
  and start again. In production, add bootstrap logic to seed ConfState on restart.
- Ensure the **leader** handles writes. If you propose to a follower, proposals may time out.
- Use separate terminals for each node so the REPL input isn’t mixed.

---
