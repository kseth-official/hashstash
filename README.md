# hashstash

A fault-tolerant, replicated key/value store that uses Raft to keep a cluster of servers in agreement.

## Setup

Requires [Go 1.20+](https://go.dev/dl/). Clone the repo and work from `src/`:

```powershell
git clone https://github.com/kseth-official/hashstash.git
cd hashstash/src
```

## Run the server

Start a 3-server cluster with an interactive client (no tests):

```powershell
go run ./cmd/hashstash
```

Then type commands:

```
put foo bar
get foo
append foo baz
quit
```

This does **not** listen on the network. All three servers and the client share one process and talk over a local fake network made of Go channels. They cannot run on separate machines.

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
go test -run TestBasic
go test -race
go test -count=1
```

## Folder structure

```
hashstash/
├── README.md
└── src/
    ├── go.mod
    ├── cmd/hashstash/      # interactive cluster runner
    ├── constants/          # shared constants
    ├── kvraft/             # replicated KV service (client, server, tests)
    ├── logger/             # logging helpers
    ├── models/             # linearizability models used by tests
    ├── porcupine/          # linearizability checker
    └── raft/               # Raft consensus library
```

## Future work

Modify the transport so servers can be hosted across multiple devices (real RPC over the network, one process per machine, instead of a shared in-process channel network).
