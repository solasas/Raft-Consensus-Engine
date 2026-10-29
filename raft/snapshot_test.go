package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// fakeSM is a Snapshotter whose answer the test controls.
type fakeSM struct {
	data  []byte
	index uint64
	err   error
	calls atomic.Int32
}

func (f *fakeSM) Snapshot() ([]byte, uint64, error) {
	f.calls.Add(1)
	return f.data, f.index, f.err
}

// snapRecorder is a Transport + SnapshotTransport that hands every outbound RPC
// to the test, which plays the network by feeding replies back by hand.
type snapRecorder struct {
	appends chan AppendEntriesArgs
	snaps   chan InstallSnapshotArgs
}

func newSnapRecorder() *snapRecorder {
	return &snapRecorder{appends: make(chan AppendEntriesArgs, 16), snaps: make(chan InstallSnapshotArgs, 16)}
}

var errTestDrives = errors.New("test drives replies by hand")

func (r *snapRecorder) RequestVote(context.Context, Peer, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errTestDrives
}

func (r *snapRecorder) AppendEntries(_ context.Context, _ Peer, a AppendEntriesArgs) (AppendEntriesReply, error) {
	r.appends <- a
	return AppendEntriesReply{}, errTestDrives
}

func (r *snapRecorder) InstallSnapshot(_ context.Context, _ Peer, a InstallSnapshotArgs) (InstallSnapshotReply, error) {
	r.snaps <- a
	return InstallSnapshotReply{}, errTestDrives
}

func (r *snapRecorder) nextSnap(t *testing.T) InstallSnapshotArgs {
	t.Helper()
	select {
	case a := <-r.snaps:
		return a
	case <-time.After(time.Second):
		t.Fatal("no InstallSnapshot was sent")
		return InstallSnapshotArgs{}
	}
}

func (r *snapRecorder) nextAppend(t *testing.T) AppendEntriesArgs {
	t.Helper()
	select {
	case a := <-r.appends:
		return a
	case <-time.After(time.Second):
		t.Fatal("no AppendEntries was sent")
		return AppendEntriesArgs{}
	}
}

// expectNoRPC asserts that nothing is sent for a short while.
func (r *snapRecorder) expectNoRPC(t *testing.T) {
	t.Helper()
	select {
	case a := <-r.snaps:
		t.Fatalf("unexpected InstallSnapshot: %+v", a)
	case a := <-r.appends:
		t.Fatalf("unexpected AppendEntries: %+v", a)
	case <-time.After(60 * time.Millisecond):
	}
}

func snapConfig(id string, rec *snapRecorder, st Storage, sm Snapshotter, threshold int, ids ...string) Config {
	var peers []Peer
	for _, o := range ids {
		if o != id {
			peers = append(peers, Peer{ID: o, Addr: o + ":7000"})
		}
	}
	return Config{
		ID: id, Peers: peers, Transport: rec, Storage: st,
		ApplyCh:           make(chan ApplyMsg, 8),
		Snapshotter:       sm,
		SnapshotThreshold: threshold,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// newSnapNode builds an un-started node with snapshotting enabled.
func newSnapNode(t *testing.T, id string, sm Snapshotter, threshold int, ids ...string) (*Node, *MemoryStorage, *snapRecorder) {
	t.Helper()
	rec, st := newSnapRecorder(), NewMemoryStorage()
	n, err := NewNode(snapConfig(id, rec, st, sm, threshold, ids...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n, st, rec
}

// committed gives the node a log of `ts` terms that is fully committed and applied.
func committed(n *Node, ts ...uint64) {
	setLog(n, ts...)
	n.commitIndex, n.lastApplied = uint64(len(ts)), uint64(len(ts))
	n.persist()
}

// ---------------------------------------------------------------------------
// compaction
// ---------------------------------------------------------------------------

func TestCompactToTrimsLogAndKeepsIndexing(t *testing.T) {
	n, st, _ := newSnapNode(t, "n", &fakeSM{}, 100, "n", "a", "b")
	n.currentTerm = 4
	setLog(n, 1, 1, 2, 2, 3, 3, 3, 4) // indexes 1..8
	n.commitIndex, n.lastApplied = 6, 6
	n.persist()

	n.compactTo(5, []byte("img5"))

	if n.snapIndex != 5 || n.snapTerm != 3 {
		t.Fatalf("snapshot boundary = (%d, term %d), want (5, term 3)", n.snapIndex, n.snapTerm)
	}
	if n.lastIndex() != 8 {
		t.Errorf("lastIndex = %d, want 8: compaction must not change the log's extent", n.lastIndex())
	}
	if got := len(n.log) - 1; got != 3 {
		t.Errorf("retained %d entries, want 3 (indexes 6-8)", got)
	}
	for idx, want := range map[uint64]uint64{5: 3, 6: 3, 7: 3, 8: 4} {
		if got := n.termAt(idx); got != want {
			t.Errorf("termAt(%d) = %d, want %d", idx, got, want)
		}
	}
	if e := n.entry(7); e.Index != 7 {
		t.Errorf("entry(7).Index = %d", e.Index)
	}
	if idx, term := n.lastLog(); idx != 8 || term != 4 {
		t.Errorf("lastLog = (%d, %d), want (8, 4)", idx, term)
	}

	snap, _ := st.LoadSnapshot()
	if snap.Index != 5 || snap.Term != 3 || string(snap.Data) != "img5" {
		t.Errorf("persisted snapshot = %+v", snap)
	}
	if _, saved, _ := st.Load(); len(saved) != 3 || saved[0].Index != 6 {
		t.Errorf("persisted log = %+v, want indexes 6-8", saved)
	}

	// It only ever moves forward, and never past what has been applied.
	n.compactTo(4, []byte("old"))
	n.compactTo(7, []byte("too new")) // lastApplied is 6
	if n.snapIndex != 5 || string(n.snapshot) != "img5" {
		t.Errorf("stale or premature compaction was applied: index %d image %q", n.snapIndex, n.snapshot)
	}
	n.compactTo(6, []byte("img6"))
	if n.snapIndex != 6 || len(n.log)-1 != 2 {
		t.Errorf("second compaction: snapIndex %d retained %d, want 6 and 2", n.snapIndex, len(n.log)-1)
	}
}

func TestCompactToReleasesMemory(t *testing.T) {
	n, _, _ := newSnapNode(t, "n", &fakeSM{}, 100, "n", "a", "b")
	ts := make([]uint64, 1000)
	for i := range ts {
		ts[i] = 1
	}
	committed(n, ts...)

	n.compactTo(990, []byte("img"))
	if c := cap(n.log); c > 64 {
		t.Errorf("cap(log) = %d after compacting 990 of 1000 entries: the old backing array is still held", c)
	}
}

func TestStatusReportsCompaction(t *testing.T) {
	n, _, _ := newSnapNode(t, "n", &fakeSM{}, 100, "n", "a", "b")
	committed(n, 1, 1, 1, 1, 1)
	n.compactTo(3, []byte("x"))
	s := n.status()
	if s.SnapshotIndex != 3 || s.LogEntries != 2 || s.LastLogIndex != 5 {
		t.Errorf("status = %+v, want SnapshotIndex 3, LogEntries 2, LastLogIndex 5", s)
	}
}

// ---------------------------------------------------------------------------
// the log keeps working above a snapshot
// ---------------------------------------------------------------------------

// compactedFollower: committed log terms [1,1,2,2,3,3], compacted through index 4
// (snapshot term 2), leaving entries 5 and 6 (both term 3).
func compactedFollower(t *testing.T) *Node {
	t.Helper()
	n, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
	n.currentTerm = 3
	setLog(n, 1, 1, 2, 2, 3, 3)
	n.commitIndex, n.lastApplied = 4, 4
	n.persist()
	n.compactTo(4, []byte("img4"))
	return n
}

func TestAppendEntriesOnACompactedLog(t *testing.T) {
	tests := []struct {
		name        string
		args        AppendEntriesArgs
		wantSuccess bool
		wantLast    uint64
		wantCI      uint64
		wantCT      uint64
	}{
		{
			name:        "prev at the snapshot edge with the right term appends",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 2, Entries: entries(5, 3, 3, 3)},
			wantSuccess: true, wantLast: 7,
		},
		{
			name:        "prev at the snapshot edge with the wrong term is rejected",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 9},
			wantSuccess: false, wantLast: 6, wantCI: 4, wantCT: 2,
		},
		{
			name:        "prev inside the snapshot: covered entries skipped, the rest appended",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(3, 2, 2, 3, 3, 3)},
			wantSuccess: true, wantLast: 7,
		},
		{
			name:        "RPC entirely inside the snapshot is a harmless success",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 1, 2)},
			wantSuccess: true, wantLast: 6,
		},
		{
			name:        "prev beyond the log is rejected with the log length",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 9, PrevLogTerm: 3},
			wantSuccess: false, wantLast: 6, wantCI: 7,
		},
		{
			name:        "conflict above the snapshot truncates and replaces",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 2, Entries: entries(5, 9)},
			wantSuccess: true, wantLast: 5,
		},
		{
			name:        "backtrack hint never points into the compacted prefix",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 6, PrevLogTerm: 7},
			wantSuccess: false, wantLast: 6, wantCI: 5, wantCT: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := compactedFollower(t)
			reply := f.handleAppendEntries(tc.args)
			if reply.Success != tc.wantSuccess {
				t.Fatalf("Success = %v, want %v (reply %+v)", reply.Success, tc.wantSuccess, reply)
			}
			if f.lastIndex() != tc.wantLast {
				t.Errorf("lastIndex = %d, want %d", f.lastIndex(), tc.wantLast)
			}
			if !tc.wantSuccess && (reply.ConflictIndex != tc.wantCI || reply.ConflictTerm != tc.wantCT) {
				t.Errorf("hint = (index %d, term %d), want (%d, %d)", reply.ConflictIndex, reply.ConflictTerm, tc.wantCI, tc.wantCT)
			}
			if f.snapIndex != 4 || f.termAt(4) != 2 {
				t.Errorf("snapshot boundary disturbed: (%d, term %d)", f.snapIndex, f.termAt(4))
			}
		})
	}
}

func TestVotingUsesTheSnapshotBoundaryWhenTheLogIsEmpty(t *testing.T) {
	n, _, _ := newSnapNode(t, "v", &fakeSM{}, 100, "v", "c", "x")
	n.currentTerm = 3
	committed(n, 1, 2, 3)
	n.compactTo(3, []byte("img")) // nothing left in the log: last entry is (index 3, term 3)

	if idx, term := n.lastLog(); idx != 3 || term != 3 {
		t.Fatalf("lastLog = (%d, %d), want (3, 3)", idx, term)
	}
	tests := []struct {
		name       string
		idx, term  uint64
		wantGrant  bool
		candidate  string
		candTerm   uint64
	}{
		{"identical log", 3, 3, true, "c", 4},
		{"longer log, same term", 5, 3, true, "c", 5},
		{"shorter log", 2, 3, false, "c", 6},
		{"older last term", 9, 2, false, "c", 7},
	}
	for _, tc := range tests {
		reply := n.handleRequestVote(RequestVoteArgs{Term: tc.candTerm, CandidateID: tc.candidate, LastLogIndex: tc.idx, LastLogTerm: tc.term})
		if reply.VoteGranted != tc.wantGrant {
			t.Errorf("%s: granted = %v, want %v", tc.name, reply.VoteGranted, tc.wantGrant)
		}
	}
}

// ---------------------------------------------------------------------------
// leader: choosing between entries and a snapshot
// ---------------------------------------------------------------------------

// compactedLeader: term 3, committed log [1,1,2,2,3,3], compacted through 4.
func compactedLeader(t *testing.T) (*Node, *snapRecorder) {
	t.Helper()
	n, _, rec := newSnapNode(t, "l", &fakeSM{}, 100, "l", "f", "x")
	n.state, n.currentTerm, n.leaderID = Leader, 3, "l"
	committed(n, 1, 1, 2, 2, 3, 3)
	n.compactTo(4, []byte("img4"))
	n.nextIndex = map[string]uint64{"f": 3, "x": 5}
	n.matchIndex = map[string]uint64{"f": 0, "x": 0}
	return n, rec
}

func TestReplicateToSendsASnapshotOnlyWhenEntriesAreGone(t *testing.T) {
	n, rec := compactedLeader(t)

	// f needs index 3, which is inside the snapshot.
	n.replicateTo(Peer{ID: "f"})
	s := rec.nextSnap(t)
	if s.LastIncludedIndex != 4 || s.LastIncludedTerm != 2 || string(s.Data) != "img4" || s.Term != 3 || s.LeaderID != "l" {
		t.Errorf("InstallSnapshot = %+v", s)
	}

	// Heartbeats must not pile up copies while that transfer is outstanding.
	n.replicateTo(Peer{ID: "f"})
	n.replicateTo(Peer{ID: "f"})
	rec.expectNoRPC(t)

	// x needs index 5: the entry right after the snapshot, so plain entries suffice.
	n.replicateTo(Peer{ID: "x"})
	a := rec.nextAppend(t)
	if a.PrevLogIndex != 4 || a.PrevLogTerm != 2 || len(a.Entries) != 2 || a.Entries[0].Index != 5 {
		t.Errorf("AppendEntries = prev (%d, term %d) entries %d, want prev (4, term 2) and indexes 5-6",
			a.PrevLogIndex, a.PrevLogTerm, len(a.Entries))
	}
}

func TestSnapshotResultMovesTheFollowerForward(t *testing.T) {
	n, rec := compactedLeader(t)
	args := InstallSnapshotArgs{Term: 3, LeaderID: "l", LastIncludedIndex: 4, LastIncludedTerm: 2, Data: []byte("img4")}

	n.snapInFlight["f"] = true
	n.handleSnapshotResult(snapResult{from: "f", term: 3, args: args, reply: InstallSnapshotReply{Term: 3}})
	if n.snapInFlight["f"] {
		t.Error("in-flight mark not cleared")
	}
	if n.matchIndex["f"] != 4 || n.nextIndex["f"] != 5 {
		t.Errorf("match/next = %d/%d, want 4/5", n.matchIndex["f"], n.nextIndex["f"])
	}
	// The entries after the snapshot follow immediately.
	if a := rec.nextAppend(t); a.PrevLogIndex != 4 || len(a.Entries) != 2 {
		t.Errorf("follow-up AppendEntries = prev %d, %d entries; want prev 4, 2 entries", a.PrevLogIndex, len(a.Entries))
	}

	t.Run("failed transfer clears the mark and changes nothing", func(t *testing.T) {
		n, _ := compactedLeader(t)
		n.snapInFlight["f"] = true
		n.handleSnapshotResult(snapResult{from: "f", term: 3, args: args, err: errTestDrives})
		if n.snapInFlight["f"] || n.matchIndex["f"] != 0 || n.nextIndex["f"] != 3 {
			t.Errorf("inflight %v match %d next %d", n.snapInFlight["f"], n.matchIndex["f"], n.nextIndex["f"])
		}
	})

	t.Run("reply from an earlier term of leadership is ignored", func(t *testing.T) {
		n, _ := compactedLeader(t)
		n.handleSnapshotResult(snapResult{from: "f", term: 2, args: args, reply: InstallSnapshotReply{Term: 2}})
		if n.matchIndex["f"] != 0 || n.nextIndex["f"] != 3 {
			t.Errorf("match %d next %d: stale reply had an effect", n.matchIndex["f"], n.nextIndex["f"])
		}
	})

	t.Run("a higher term in the reply demotes the leader", func(t *testing.T) {
		n, _ := compactedLeader(t)
		n.heartbeat = time.NewTicker(time.Hour) // stopped by becomeFollower, as for a real leader
		n.handleSnapshotResult(snapResult{from: "f", term: 3, args: args, reply: InstallSnapshotReply{Term: 9}})
		if n.state != Follower || n.currentTerm != 9 {
			t.Errorf("state %v term %d, want Follower in term 9", n.state, n.currentTerm)
		}
	})
}

// ---------------------------------------------------------------------------
// follower: InstallSnapshot
// ---------------------------------------------------------------------------

func TestHandleInstallSnapshot(t *testing.T) {
	snapArgs := func(term, idx, lterm uint64) InstallSnapshotArgs {
		return InstallSnapshotArgs{Term: term, LeaderID: "l", LastIncludedIndex: idx, LastIncludedTerm: lterm, Data: []byte("img")}
	}

	t.Run("empty follower adopts the snapshot", func(t *testing.T) {
		f, st, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		reply := f.handleInstallSnapshot(snapArgs(3, 10, 3))

		if reply.Term != 3 || f.currentTerm != 3 || f.leaderID != "l" {
			t.Errorf("reply %+v term %d leader %q", reply, f.currentTerm, f.leaderID)
		}
		if f.snapIndex != 10 || f.snapTerm != 3 || f.lastIndex() != 10 || len(f.log) != 1 {
			t.Errorf("snap (%d, %d) last %d log len %d, want snapshot at 10 and an empty log", f.snapIndex, f.snapTerm, f.lastIndex(), len(f.log))
		}
		if f.commitIndex != 10 || f.lastApplied != 10 {
			t.Errorf("commit %d applied %d, want 10 and 10", f.commitIndex, f.lastApplied)
		}
		if len(f.applyQ.items) != 1 || !f.applyQ.items[0].IsSnapshot() || string(f.applyQ.items[0].Snapshot) != "img" || f.applyQ.items[0].SnapshotIndex != 10 {
			t.Errorf("state machine was handed %+v", f.applyQ.items)
		}
		if snap, _ := st.LoadSnapshot(); snap.Index != 10 || string(snap.Data) != "img" {
			t.Errorf("persisted snapshot = %+v", snap)
		}
	})

	t.Run("a log that contradicts the snapshot is discarded", func(t *testing.T) {
		f, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		setLog(f, 1, 1, 2) // uncommitted junk from a dead leader
		f.handleInstallSnapshot(snapArgs(5, 2, 5))
		if f.lastIndex() != 2 || f.snapIndex != 2 || len(f.log) != 1 {
			t.Errorf("last %d snap %d log len %d, want everything replaced by the snapshot", f.lastIndex(), f.snapIndex, len(f.log))
		}
	})

	t.Run("a matching entry keeps the suffix after it", func(t *testing.T) {
		f, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		setLog(f, 1, 1, 2, 2, 2)
		f.commitIndex, f.lastApplied = 1, 1
		f.handleInstallSnapshot(snapArgs(2, 3, 2)) // entry 3 is (term 2): same as ours

		if f.snapIndex != 3 || f.lastIndex() != 5 || len(f.log)-1 != 2 || f.termAt(5) != 2 {
			t.Errorf("snap %d last %d retained %d, want 3, 5 and 2", f.snapIndex, f.lastIndex(), len(f.log)-1)
		}
		if f.commitIndex != 3 || f.lastApplied != 3 {
			t.Errorf("commit %d applied %d, want 3 and 3", f.commitIndex, f.lastApplied)
		}
	})

	t.Run("a snapshot older than our commit index is ignored", func(t *testing.T) {
		f, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		committed(f, 1, 1, 1, 1, 1)
		reply := f.handleInstallSnapshot(snapArgs(1, 4, 1))
		if f.snapIndex != 0 || f.lastIndex() != 5 || len(f.applyQ.items) != 0 {
			t.Errorf("snap %d last %d queued %d: installing it would roll the state machine back", f.snapIndex, f.lastIndex(), len(f.applyQ.items))
		}
		if reply.Term != 1 {
			t.Errorf("reply term = %d", reply.Term)
		}
	})

	t.Run("a stale leader is rejected", func(t *testing.T) {
		f, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		f.currentTerm = 5
		reply := f.handleInstallSnapshot(snapArgs(4, 10, 4))
		if reply.Term != 5 || f.snapIndex != 0 || f.leaderID != "" {
			t.Errorf("reply %+v snap %d leader %q: stale snapshot must change nothing", reply, f.snapIndex, f.leaderID)
		}
	})

	t.Run("a newer term is adopted and clears the vote", func(t *testing.T) {
		f, _, _ := newSnapNode(t, "f", &fakeSM{}, 100, "f", "l", "x")
		f.currentTerm, f.votedFor = 2, "x"
		f.handleInstallSnapshot(snapArgs(6, 10, 6))
		if f.currentTerm != 6 || f.votedFor != "" {
			t.Errorf("term %d votedFor %q, want 6 and none", f.currentTerm, f.votedFor)
		}
	})
}

func TestInstallSnapshotNeedsSnapshotSupport(t *testing.T) {
	n, _ := newBareNode(t, "n", "n", "a", "b") // no Snapshotter configured
	if _, err := n.InstallSnapshot(InstallSnapshotArgs{Term: 1}); !errors.Is(err, ErrSnapshotsDisabled) {
		t.Errorf("err = %v, want ErrSnapshotsDisabled", err)
	}
}

// A follower with nothing, behind a leader that compacted: the snapshot goes
// first, then the entries after it, and the follower ends identical.
func TestLeaderBringsAnEmptyFollowerUpToDate(t *testing.T) {
	leader, rec := func() (*Node, *snapRecorder) {
		n, _, rec := newSnapNode(t, "l", &fakeSM{}, 1000, "l", "f", "x")
		n.state, n.currentTerm, n.leaderID = Leader, 3, "l"
		ts := make([]uint64, 60)
		for i := range ts {
			ts[i] = 3
		}
		committed(n, ts...)
		n.compactTo(50, []byte("img50")) // leaves entries 51-60
		n.nextIndex = map[string]uint64{"f": 1, "x": 61}
		n.matchIndex = map[string]uint64{"f": 0, "x": 0}
		return n, rec
	}()
	follower, _, _ := newSnapNode(t, "f", &fakeSM{}, 1000, "f", "l", "x")

	leader.replicateTo(Peer{ID: "f"})
	snapshots, appends := 0, 0
	for leader.matchIndex["f"] != 60 {
		if snapshots+appends > 10 {
			t.Fatalf("no convergence after %d RPCs; match %d next %d", snapshots+appends, leader.matchIndex["f"], leader.nextIndex["f"])
		}
		select {
		case s := <-rec.snaps:
			snapshots++
			reply := follower.handleInstallSnapshot(s)
			leader.handleSnapshotResult(snapResult{from: "f", term: 3, args: s, reply: reply})
		case a := <-rec.appends:
			appends++
			reply := follower.handleAppendEntries(a)
			leader.handleAppendResult(appendResult{from: "f", term: 3, args: a, reply: reply})
		case <-time.After(time.Second):
			t.Fatal("leader went quiet before the follower caught up")
		}
	}

	if snapshots != 1 || appends != 1 {
		t.Errorf("used %d snapshots and %d AppendEntries, want exactly 1 and 1", snapshots, appends)
	}
	if follower.snapIndex != 50 || follower.lastIndex() != 60 || follower.termAt(60) != 3 {
		t.Errorf("follower snap %d last %d term(60) %d, want 50, 60, 3", follower.snapIndex, follower.lastIndex(), follower.termAt(60))
	}
	if follower.currentTerm != 3 || follower.leaderID != "l" {
		t.Errorf("follower term %d leader %q", follower.currentTerm, follower.leaderID)
	}
}

// ---------------------------------------------------------------------------
// when to snapshot
// ---------------------------------------------------------------------------

func recvCompact(t *testing.T, n *Node) compactResult {
	t.Helper()
	select {
	case r := <-n.compactDone:
		return r
	case <-time.After(time.Second):
		t.Fatal("the Snapshotter was never asked")
		return compactResult{}
	}
}

func TestSnapshotTriggersOnlyAboveTheThreshold(t *testing.T) {
	sm := &fakeSM{data: []byte("img"), index: 7}
	n, _, _ := newSnapNode(t, "n", sm, 5, "n", "a", "b")

	committed(n, 1, 1, 1, 1, 1) // 5 entries: not above a threshold of 5
	n.maybeSnapshot()
	if n.snapshotting || sm.calls.Load() != 0 {
		t.Fatal("snapshot started at exactly the threshold")
	}

	committed(n, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1) // 10 entries
	n.maybeSnapshot()
	n.maybeSnapshot() // second call while one is in flight must do nothing
	res := recvCompact(t, n)
	if got := sm.calls.Load(); got != 1 {
		t.Errorf("Snapshotter called %d times, want 1", got)
	}

	n.handleCompactResult(res)
	if n.snapshotting || n.snapIndex != 7 || len(n.log)-1 != 3 {
		t.Errorf("snapshotting %v snapIndex %d retained %d, want false, 7, 3", n.snapshotting, n.snapIndex, len(n.log)-1)
	}
	n.maybeSnapshot() // 3 retained entries: well below the threshold
	if sm.calls.Load() != 1 {
		t.Error("snapshotted again with a small log")
	}
}

// A state machine that hasn't applied anything yet must not make the node spin
// asking for snapshots that compact nothing.
func TestSnapshotBacksOffWhenTheStateMachineLags(t *testing.T) {
	sm := &fakeSM{index: 0}
	n, _, _ := newSnapNode(t, "n", sm, 8, "n", "a", "b")
	committed(n, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1) // 10 entries, threshold 8

	n.maybeSnapshot()
	n.handleCompactResult(recvCompact(t, n)) // index 0: nothing to compact
	if n.snapIndex != 0 {
		t.Fatalf("snapIndex = %d", n.snapIndex)
	}

	n.maybeSnapshot()
	if sm.calls.Load() != 1 {
		t.Error("retried immediately: that is a hot loop")
	}

	n.lastApplied = n.snapRetryAt // enough progress since the last attempt
	n.maybeSnapshot()
	recvCompact(t, n)
	if sm.calls.Load() != 2 {
		t.Errorf("calls = %d, want a retry after progress", sm.calls.Load())
	}
}

func TestSnapshotErrorLeavesTheLogAlone(t *testing.T) {
	sm := &fakeSM{err: errors.New("disk full")}
	n, _, _ := newSnapNode(t, "n", sm, 3, "n", "a", "b")
	committed(n, 1, 1, 1, 1, 1)
	n.maybeSnapshot()
	n.handleCompactResult(recvCompact(t, n))
	if n.snapIndex != 0 || len(n.log)-1 != 5 || n.snapshotting {
		t.Errorf("snapIndex %d retained %d snapshotting %v after a failed snapshot", n.snapIndex, len(n.log)-1, n.snapshotting)
	}
}

// ---------------------------------------------------------------------------
// restart
// ---------------------------------------------------------------------------

func TestNewNodeRestoresFromSnapshot(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveSnapshot(Snapshot{Index: 5, Term: 2, Data: []byte("img")})
	// A crash between saving the snapshot and saving the trimmed log leaves entries
	// 4-5 behind, already covered by the snapshot.
	_ = st.Save(HardState{CurrentTerm: 3, VotedFor: "x"}, []LogEntry{
		{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 3}, {Index: 7, Term: 3},
	})

	n, err := NewNode(snapConfig("n", newSnapRecorder(), st, &fakeSM{}, 100, "n", "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)

	if n.snapIndex != 5 || n.snapTerm != 2 || n.lastIndex() != 7 || len(n.log)-1 != 2 || n.log[1].Index != 6 {
		t.Errorf("snap (%d, %d) last %d retained %d: want the overlap trimmed to entries 6-7", n.snapIndex, n.snapTerm, n.lastIndex(), len(n.log)-1)
	}
	if n.currentTerm != 3 || n.votedFor != "x" {
		t.Errorf("hard state not restored: term %d vote %q", n.currentTerm, n.votedFor)
	}
	if n.commitIndex != 5 || n.lastApplied != 5 {
		t.Errorf("commit %d applied %d, want the snapshot treated as applied", n.commitIndex, n.lastApplied)
	}
	// The state machine must be rebuilt from the snapshot before entries replay.
	if len(n.applyQ.items) != 1 || !n.applyQ.items[0].IsSnapshot() || n.applyQ.items[0].SnapshotIndex != 5 {
		t.Errorf("queued for the state machine: %+v", n.applyQ.items)
	}
}

func TestNewNodeRejectsAHoleBetweenSnapshotAndLog(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveSnapshot(Snapshot{Index: 5, Term: 2, Data: []byte("img")})
	_ = st.Save(HardState{CurrentTerm: 3}, []LogEntry{{Index: 7, Term: 3}, {Index: 8, Term: 3}}) // 6 is missing

	if _, err := NewNode(snapConfig("n", newSnapRecorder(), st, &fakeSM{}, 100, "n", "a", "b")); err == nil {
		t.Error("NewNode accepted a log with a gap after the snapshot")
	}
}

// bareStorage implements Storage but not SnapshotStorage.
type bareStorage struct{ inner *MemoryStorage }

func (b bareStorage) Save(hs HardState, e []LogEntry) error     { return b.inner.Save(hs, e) }
func (b bareStorage) Load() (HardState, []LogEntry, error)      { return b.inner.Load() }

func TestSnapshotConfigIsValidated(t *testing.T) {
	cfg := snapConfig("n", newSnapRecorder(), bareStorage{NewMemoryStorage()}, &fakeSM{}, 100, "n", "a", "b")
	if _, err := NewNode(cfg); err == nil {
		t.Error("accepted a Snapshotter with a Storage that cannot persist snapshots")
	}

	cfg = snapConfig("n", newSnapRecorder(), NewMemoryStorage(), &fakeSM{}, 100, "n", "a", "b")
	cfg.Transport = deadTransport{} // no InstallSnapshot
	if _, err := NewNode(cfg); err == nil {
		t.Error("accepted a Snapshotter with a Transport that cannot send InstallSnapshot")
	}

	cfg = snapConfig("n", newSnapRecorder(), NewMemoryStorage(), &fakeSM{}, 0, "n", "a", "b")
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if n.cfg.SnapshotThreshold != 1024 || n.cfg.SnapshotTimeout != 5*time.Second {
		t.Errorf("defaults = threshold %d timeout %v, want 1024 and 5s", n.cfg.SnapshotThreshold, n.cfg.SnapshotTimeout)
	}
}

// Keep the compiler honest about the helper types used only through interfaces.
var (
	_ SnapshotTransport = (*snapRecorder)(nil)
	_ SnapshotStorage   = (*MemoryStorage)(nil)
	_ Snapshotter       = (*fakeSM)(nil)
)
