package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

// A startup failure must exit, not hang.
//
// run() starts the compactor and the replicator as goroutines and waits for them in deferred
// receives. Those waits are only safe if something cancels their context first, and the
// signal context's own cancel is deferred earliest, so it runs last. Before the fix, a port
// conflict — the most ordinary startup failure there is — left the process alive forever with
// nothing listening: `docker ps` showed it up, `/healthz` was answered by the OTHER process
// holding the port, and the error appeared only when someone killed it by hand.
func TestRunExitsWhenItCannotBind(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupying a port: %v", err)
	}
	defer ln.Close()

	t.Setenv("ANALYTICS_NODE_ID", "bind-conflict")
	t.Setenv("ANALYTICS_DATA_DIR", t.TempDir())
	t.Setenv("ANALYTICS_LISTEN_ADDR", ln.Addr().String())
	// Without a bucket the replicator never starts, and its wait is the second of the two
	// deferred receives. Configure object storage at an endpoint nothing answers on: the
	// replicator's loop must be stopped on the way out just like the compactor's, and an
	// unreachable bucket must not turn a failed start into a hang either.
	t.Setenv("ANALYTICS_S3_BUCKET", "not-a-real-bucket")
	t.Setenv("ANALYTICS_S3_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("ANALYTICS_S3_ACCESS_KEY", "x")
	t.Setenv("ANALYTICS_S3_SECRET_KEY", "y")

	done := make(chan error, 1)
	go func() { done <- run() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run() returned nil after failing to bind")
		}
		if !strings.Contains(err.Error(), "address already in use") {
			t.Fatalf("expected a bind error, got %v", err)
		}
	case <-time.After(10 * time.Second):
		// Deliberately not t.Fatal inside a select on a goroutine that is still running:
		// the point of the failure is that run() never returns.
		t.Fatal("run() did not return after failing to bind: it is hanging on shutdown")
	}
}
