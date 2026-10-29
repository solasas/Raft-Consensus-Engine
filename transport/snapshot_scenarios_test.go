package transport

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/sashanks/raft/kv"
	"github.com/sashanks/raft/raft"
)

// A follower is partitioned away while the rest of the cluster writes enough to
// compact its log past the follower's position. The network then heals, but
// stays lossy: the InstallSnapshot RPC (and the entries after it) must survive
// dropped packets by being retried.
func TestSnapshotCatchUpOverALossyPartitionedNetwork(t *testing.T) {
	sw := NewSwitch(Options{Seed: 11})
	chaos := NewChaos(sw)
	t.Cleanup(sw.Close) // registered first, so it runs after the services stop

	ids := []string{"n1", "n2", "n3"}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcs := map[string]*kv.Service{}
	for _, id := range ids {
		var peers []raft.Peer
		for _, o := range ids {
			if o != id {
				peers = append(peers, raft.Peer{ID: o, Addr: "sim://" + o})
			}
		}
		svc, err := kv.NewService(raft.Config{
			ID: id, Peers: peers, Logger: quiet, SnapshotThreshold: 25,
			Transport: AsRaftTransport(sw.NewTransport(id, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		svcs[id] = svc
		sw.Register(id, ServeNode(svc.Node()))
	}
	for _, s := range svcs {
		s.Start()
	}
	t.Cleanup(func() {
		for _, s := range svcs {
			s.Stop()
		}
	})

	status := func(id string) raft.Status {
		s, _ := svcs[id].Node().Status()
		return s
	}
	waitLeader := func() *kv.Service {
		var out *kv.Service
		eventually(t, "a single leader", func() bool {
			var found []string
			for _, id := range ids {
				if status(id).State == raft.Leader {
					found = append(found, id)
				}
			}
			if len(found) == 1 {
				out = svcs[found[0]]
				return true
			}
			return false
		})
		return out
	}
	// Puts are idempotent, so on any failure (leadership moved, packet loss) it is
	// safe to look up the leader again and repeat.
	put := func(key, val string) {
		for attempt := 0; attempt < 20; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := waitLeader().Put(ctx, key, val)
			cancel()
			if err == nil {
				return
			}
		}
		t.Fatalf("put %s never succeeded", key)
	}

	// 1. Partition one follower away, then make the rest of the network lossy and slow.
	var lagID string
	current := waitLeader()
	for _, id := range ids {
		if svcs[id] != current {
			lagID = id
			break
		}
	}
	chaos.Isolate(lagID)
	frozen := status(lagID)
	must(t, chaos.SetGlobalLatency(1*time.Millisecond, 3*time.Millisecond))
	must(t, chaos.SetGlobalDropRate(0.10))
	t.Logf("injected: %v", chaos.Faults())

	// 2. The majority writes and compacts well past where the follower stopped.
	const keys = 120
	for i := 0; i < keys; i++ {
		put(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i))
	}
	eventually(t, "the connected nodes to compact past the isolated node's log", func() bool {
		for _, id := range ids {
			if id != lagID && status(id).SnapshotIndex <= frozen.LastLogIndex {
				return false
			}
		}
		return true
	})
	if n := len(svcs[lagID].Store().Dump()); n != 0 {
		t.Fatalf("isolated node holds %d keys", n)
	}

	// 3. Heal the partition but keep the loss. The follower must come back via a snapshot.
	chaos.Reconnect(lagID)
	eventually(t, "the isolated node to catch up through packet loss", func() bool {
		return len(svcs[lagID].Store().Dump()) == keys
	})
	if got := status(lagID).SnapshotIndex; got <= frozen.LastLogIndex {
		t.Errorf("snapshot index %d never passed the node's frozen log end %d: it was not caught up by a snapshot", got, frozen.LastLogIndex)
	}

	// 4. Everyone ends up with exactly the same data.
	chaos.Reset()
	eventually(t, "identical state on every node", func() bool {
		ref := svcs[ids[0]].Store().Dump()
		for _, id := range ids[1:] {
			if !reflect.DeepEqual(ref, svcs[id].Store().Dump()) {
				return false
			}
		}
		return len(ref) == keys
	})
}
