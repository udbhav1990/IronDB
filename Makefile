# -------- Settings --------
APP    := irondb-cluster
MAIN   := cmd/cluster.go
BIN    := ./bin/$(APP)

ADDR1  := 127.0.0.1:9101
ADDR2  := 127.0.0.1:9102
ADDR3  := 127.0.0.1:9103

DATA1  := ./bin/data/node1
DATA2  := ./bin/data/node2
DATA3  := ./bin/data/node3

# Peers
PEERS1 := 1=$(ADDR1)
PEERS3 := 1=$(ADDR1),2=$(ADDR2),3=$(ADDR3)

# -------- Targets --------
.PHONY: build rebuild clean clean-bin clean-data \
        node1 node2 node3 single

build:
	@mkdir -p bin
	@echo ">> Building $(BIN)"
	@go build -o $(BIN) $(MAIN)

rebuild: clean-bin build

clean:
	@echo ">> Cleaning bin and data"
	@rm -rf ./bin $(DATA1) $(DATA2) $(DATA3)

clean-bin:
	@rm -rf ./bin

clean-data:
	@rm -rf $(DATA1) $(DATA2) $(DATA3)

# ---------- Run (foreground) ----------
# Open THREE terminals and run make node1 / node2 / node3 separately.

node1: build
	@mkdir -p $(DATA1)
	@echo ">> Starting node1 (id=1) on $(ADDR1)"
	@$(BIN) \
		-id=1 \
		-addr=$(ADDR1) \
		-data=$(DATA1) \
		-peers=$(PEERS3) 2> ./bin/node1.log

node2: build
	@mkdir -p $(DATA2)
	@echo ">> Starting node2 (id=2) on $(ADDR2)"
	@$(BIN) \
		-id=2 \
		-addr=$(ADDR2) \
		-data=$(DATA2) \
		-peers=$(PEERS3) \
		-join 2> ./bin/node2.log

node3: build
	@mkdir -p $(DATA3)
	@echo ">> Starting node3 (id=3) on $(ADDR3)"
	@$(BIN) \
		-id=3 \
		-addr=$(ADDR3) \
		-data=$(DATA3) \
		-peers=$(PEERS3) \
		-join 2> ./bin/node3.log

# Single-node dev (fastest way to test REPL/writes)
single: build
	@mkdir -p $(DATA1)
	@echo ">> Starting SINGLE node (id=1) on $(ADDR1)"
	@$(BIN) \
		-id=1 \
		-addr=$(ADDR1) \
		-data=$(DATA1) \
		-peers=$(PEERS1) 2> ./bin/single.log