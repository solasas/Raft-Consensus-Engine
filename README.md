# Raft-Consensus-Engine

A [Raft](https://raft.github.io/raft.pdf) consensus engine written from scratch in Go, with a replicated key-value store on top, a simulated network you can break on purpose, and a live browser dashboard for watching elections and running chaos experiments.

No third-party dependencies: everything, including the WebSocket server, uses only the Go standard library.

```
 Browser  ──WebSocket + JSON API──►  dashboard  ──►  kv (key-value store)  ──►  raft (consensus)
                                                                                    │
                                                              transport (simulated network + chaos)
```

## What it does

- **Leader election** with randomized timeouts, terms, and the up-to-date-log voting rule.
- **Log replication** with the log-matching check, fast backtracking on conflicts, and commit by quorum (only current-term entries commit by counting, as Raft requires).
- **Snapshots and log compaction.** When a node's log passes a threshold it folds the old entries into a snapshot and drops them. Followers that fall too far behind are caught up with an `InstallSnapshot` RPC.
- **A replicated key-value store** (`put` / `delete`) that applies committed entries in order and tells a writer honestly whether its write committed or was overwritten.
- **A simulated network** with per-link latency, random packet loss, hard partitions, and one-way (asymmetric) cuts, controlled at runtime.
- **A live dashboard**: leader elections as they happen, replication progress bars, a log viewer, and buttons such as **Sever Network Link** to run chaos experiments.

## Requirements

- Go 1.22 or newer (the HTTP routing uses the method patterns added in 1.22).
- A modern browser for the dashboard.

```bash
brew install go        # macOS; see https://go.dev/dl for other systems
```

## Quick start

Run the dashboard:

```bash
go run ./cmd/raftviz
```

Open <http://127.0.0.1:8080>. A 5-node cluster starts and elects a leader within a few seconds.

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:8080` | Address to listen on. Keep it on loopback: the page can break the cluster. |
| `-nodes` | `5` | Cluster size, 3 to 9. |
| `-slow` | `3` | Multiplies every Raft timeout so elections are slow enough to watch. `1` is real speed. |
| `-snapshot-threshold` | `30` | Compact a node's log into a snapshot once it exceeds this many entries. |
| `-seed` | `0` | Seed for network randomness (`0` = random). |

Run the tests:

```bash
go vet ./...
go test -race ./...
```

The cluster and chaos tests use real timers, so the full run takes a minute or two.

## Using the dashboard

The page has a KPI strip (leader, quorum, throughput, commit index, failed writes), a network graph, a control rail, replication bars, an event feed, and a log viewer.

**Things to try**

1. Click **Isolate leader**. The others stop hearing heartbeats and elect a new leader in a higher term. The old leader keeps believing it leads, so the page shows a **split-brain** banner and marks it `STALE LEADER`.
2. Pick the stale leader in **Send to** and press **Write**. The entry lands in its log but never commits; it appears dashed (uncommitted) and then fails.
3. Press **Heal partitions**. The old leader sees the higher term, steps down, drops its uncommitted entry, and catches up.
4. Click a link between two nodes and press **Sever Network Link**. Use **Cut A → B** for a one-way fault: requests arrive and run, but the replies are lost.
5. Raise **Packet loss** or set **Latency** to `100–500ms` and watch the replication bars lag.
6. Turn on **Background load**, then compare throughput before and after a partition.

**Reading the graph**

| You see | It means |
|---|---|
| Gold node with a crown | Leader |
| Blue pulsing node | Candidate (an election is running) |
| Dashed red halo, `STALE LEADER` | A leader from an older term that does not know it was replaced |
| Neutral line | Healthy link (animated teal when the leader is sending) |
| Orange dotted / dashed line | Packet loss or latency / a one-way cut |
| Red dotted line | Severed link |
| Ring around a node | Fraction of its log that is committed |

One color means one thing throughout: teal = healthy or committed, gold = leader, blue = candidate, orange = degraded, red = fault.

The page works with the keyboard (`Tab` to a link or node, `Enter` to select), respects `prefers-reduced-motion`, and follows the system light/dark theme with a manual toggle.

## Project layout

| Path | What it contains |
|---|---|
| `raft/` | The consensus core: node state machine, election, replication, commit, snapshots. |
| `kv/` | The key-value state machine, its applier loop, and `Service` (a node plus a store). |
| `transport/` | The network layer: a `Transport` interface, a simulated `Switch`, and the `Chaos` controller. |
| `dashboard/` | The cluster runner, WebSocket hub, JSON API, and the embedded single-page UI (`web/index.html`). |
| `dashboard/ws/` | A small RFC 6455 WebSocket server (and `wstest/`, a test client). |
| `cmd/raftviz/` | The dashboard's `main`. |

## How it is built

### Concurrency model

Each `raft.Node` runs **one goroutine that owns all of its state** (term, vote, log, indexes, timers). Nothing else reads or writes that state, so there are no locks around it and state changes cannot race. Everything talks to it over channels: inbound RPCs, client proposals, status queries, and the results of outbound RPCs. Outbound calls run in short-lived goroutines that never touch node state; each result carries the term it was sent in, so stale replies are dropped.

Committed entries go through an unbounded queue to a separate applier goroutine, so a slow state machine can never stall heartbeats or elections.

### The log is a window

After compaction, `log[0]` stands for the last discarded entry, and entry `i` lives at `log[i - snapshotIndex]`. All code goes through `entry()`, `termAt()` and `lastIndex()` instead of indexing the slice directly.

### Safety details worth knowing

- Term, vote and log are persisted **before** replying to an RPC that changed them. A failed write panics rather than risk breaking Raft's guarantees.
- A snapshot is saved before the trimmed log, so a crash in between is recoverable on restart.
- A new leader appends a no-op entry in its own term so it can commit entries from earlier terms.
- Followers truncate only on a real conflict, so a stale or duplicate `AppendEntries` can never delete entries they already hold.
- An RPC is a round trip: the network judges the request and the reply separately, on separate directions. This is what makes one-way faults realistic.
- A message delayed past its caller's timeout is still delivered later, as on a real network, so latency faults produce stale, reordered RPCs and not only losses.

### The transport layers

```
raft.Node ──► raft.Transport            typed: RequestVote / AppendEntries / InstallSnapshot
                 │  transport.AsRaftTransport
                 ▼
            transport.Transport         SendRPC(ctx, peerID, msg)
                 │
        SimulatedTransport ──► Switch (directed link matrix) ◄── Chaos
```

A real network transport (TCP or gRPC) would implement `transport.Transport` and run under the same chaos rules via `Switch.NewTransport(self, inner)`.

## Dashboard API

All state-changing requests are `POST` with a JSON body and must send the header `X-Requested-With: raftviz`. If the browser sends an `Origin`, it must match the `Host`. The WebSocket also refuses foreign origins. This stops a web page on another site from driving a dashboard bound to localhost.

| Endpoint | Body |
|---|---|
| `GET /ws` | WebSocket; first a `hello` (state + recent events), then `state` messages (10 per second) and `event` messages as they happen |
| `GET /api/state` | The same cluster view as one JSON document |
| `POST /api/links/sever` | `{"from":"n1","to":"n2","both":true}` |
| `POST /api/links/restore` | `{"from":"n1","to":"n2","both":true}` |
| `POST /api/nodes/isolate` | `{"id":"n1"}` |
| `POST /api/nodes/reconnect` | `{"id":"n1"}` |
| `POST /api/chaos/partition` | `{"groups":[["n1","n2","n3"],["n4","n5"]]}` |
| `POST /api/chaos/heal` | none (lifts partitions, keeps loss and latency) |
| `POST /api/chaos/reset` | none (perfect network) |
| `POST /api/chaos/drop` | `{"rate":0.1}` (0 to 1) |
| `POST /api/chaos/latency` | `{"minMs":100,"maxMs":500}` |
| `POST /api/kv/put` | `{"target":"","key":"color","value":"red"}` (`target` empty = current leader) |
| `POST /api/kv/burst` | `{"count":50}` |
| `POST /api/load` | `{"rate":20}` writes per second, `0` to stop |

Example:

```bash
curl -X POST localhost:8080/api/links/sever \
  -H 'X-Requested-With: raftviz' -H 'Content-Type: application/json' \
  -d '{"from":"n1","to":"n2","both":true}'
```

## Using the packages directly

```go
transport := /* a raft.Transport that also implements raft.SnapshotTransport */
svc, err := kv.NewService(raft.Config{
    ID:        "n1",
    Peers:     []raft.Peer{{ID: "n2", Addr: "..."}, {ID: "n3", Addr: "..."}},
    Transport: transport,
})
svc.Start()
defer svc.Stop()

err = svc.Put(ctx, "color", "red")  // returns once committed and applied on this node
v, ok := svc.Get("color")           // local read: may be stale on a follower
```

`Put` on a non-leader returns `*raft.NotLeaderError` with a hint about who the leader is. `ErrNotCommitted` means a newer leader overwrote the entry, so the write definitely did not happen. `ErrOutcomeUnknown` means the entry was folded into a snapshot before the caller could check, so the caller cannot tell.

## Testing

| Layer | What is tested |
|---|---|
| `raft/` | Vote rules (table), `AppendEntries` rules (table), leader backoff against the divergent logs from Figure 7 of the paper, quorum commit including the Figure 8 case, election timers, compaction, `InstallSnapshot`, restart recovery |
| `kv/` | Store ordering, replay safety, determinism, snapshot/restore, `Await` outcomes; whole clusters for replication, failover, overwritten writes, lagging followers and restarts |
| `transport/` | Latency, loss, partitions, one-way cuts and context handling; five chaos scenarios (split brain, 3\|2 partition, asymmetric cut, high latency, lossy network) with a monitor that fails the test if any term ever has two leaders; snapshot catch-up over a lossy partition |
| `dashboard/` | Cluster actions and events, HTTP and WebSocket behavior, cross-site request protection, and the WebSocket server's framing and protocol checks |

The chaos scenarios use real timers and are sensitive to a heavily loaded machine. Run them repeatedly to check for flakiness:

```bash
go test -race -count=5 ./transport/
```

## Status and limitations

- The consensus core and the dashboard run. The timing-based chaos tests in `transport/` had a failure on the last full run that has not been diagnosed yet.
- **Persistence is in memory.** `MemoryStorage` loses everything on restart, and persisting rewrites the whole retained log each time. A real implementation needs an append-only write-ahead log with fsync.
- **Snapshots travel in one message.** Large snapshots need chunked transfer.
- **No membership changes**, no PreVote (so a node that rejoins after a partition with an inflated term forces an election), and no linearizable reads. `Get` reads the local copy and can be stale.
- **No real network transport yet.** All nodes run in one process on the simulated network.
- **No crash/restart control in the dashboard.** The chaos controls act on the network, not on processes.
- The dashboard is a development tool. Do not expose it beyond loopback.

## License

No license has been chosen yet.
