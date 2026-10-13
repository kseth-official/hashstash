# hashstash

A fault-tolerant, replicated key/value store that uses Raft to keep a cluster of servers in agreement.

## Folder structure

```
hashstash/
├── README.md
└── src/
    ├── go.mod
    ├── constants/          # shared constants
    ├── kvraft/             # replicated KV service (client, server, tests)
    ├── labgob/             # RPC-safe encoding
    ├── labrpc/             # simulated RPC / network
    ├── logger/             # logging helpers
    ├── models/             # linearizability models used by tests
    ├── porcupine/          # linearizability checker
    └── raft/               # Raft consensus library
```

## Setup

Requires [Go 1.20+](https://go.dev/dl/). Clone the repo and work from `src/`:

```powershell
git clone https://github.com/kseth-official/hashstash.git
cd hashstash/src
```

## Run tests

KV service tests (partitions, restarts, unreliable networks, linearizability):

```powershell
cd kvraft
go test
```

Raft consensus tests:

```powershell
cd raft
go test
```

Run a subset by name, check for races, or repeat a run:

```powershell
go test -run 5A
go test -race
go test -count=1
```
