// Command raftviz runs a simulated Raft cluster and serves a live control room
// for it in the browser: leader elections, log replication progress, and buttons
// that sever network links to run chaos experiments.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sashanks/raft/dashboard"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "address to listen on (keep it on loopback: the page can break the cluster)")
		nodes     = flag.Int("nodes", 5, "cluster size, 3-9")
		slow      = flag.Float64("slow", 3, "multiply every Raft timeout by this, so elections are slow enough to watch (1 = real speed)")
		threshold = flag.Int("snapshot-threshold", 30, "compact a node's log into a snapshot once it exceeds this many entries")
		seed      = flag.Int64("seed", 0, "seed for network randomness (0 = random)")
	)
	flag.Parse()

	cluster, err := dashboard.NewCluster(dashboard.Options{
		Nodes: *nodes, Slow: *slow, SnapshotThreshold: *threshold, Seed: *seed,
	})
	if err != nil {
		log.Fatal(err)
	}
	cluster.Start()
	defer cluster.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := dashboard.NewServer(cluster, nil)
	go srv.Run(ctx)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()

	fmt.Printf("raftviz: %d-node cluster, open http://%s/\n", *nodes, ln.Addr())
	if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
