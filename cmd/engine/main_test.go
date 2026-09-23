package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modlix-india/analytics-engine/internal/config"
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

// TestRedisTLSComesFromTheAddress guards the one thing that stops the resolver's shared cache
// working in every deployed environment.
//
// The platform's Redis on OCI is a managed instance that speaks TLS only. Built with a bare
// Addr and no TLSConfig — as this was — the client cannot connect, the resolver quietly falls
// back to a per-node cache, and a URL change then leaves some nodes serving the old site with
// nothing in the logs mentioning TLS.
func TestRedisTLSComesFromTheAddress(t *testing.T) {
	t.Run("a bare address stays plaintext", func(t *testing.T) {
		opts, err := redisOptions(config.Config{RedisAddr: "localhost:6379", RedisPassword: "p"})
		if err != nil {
			t.Fatal(err)
		}
		if opts.TLSConfig != nil {
			t.Error("a bare host:port must not start speaking TLS; every existing deployment uses one")
		}
		if opts.Addr != "localhost:6379" || opts.Password != "p" {
			t.Errorf("addr/password not carried through: %+v", opts)
		}
	})

	t.Run("rediss:// gets TLS", func(t *testing.T) {
		opts, err := redisOptions(config.Config{
			RedisAddr:     "rediss://example.redis.us-ashburn-1.oci.oraclecloud.com:6379",
			RedisPassword: "secret",
		})
		if err != nil {
			t.Fatal(err)
		}
		if opts.TLSConfig == nil {
			t.Fatal("rediss:// must speak TLS, or the managed instance refuses the connection")
		}
		// The password is configured separately, the way the Java side keeps it out of the URL.
		if opts.Password != "secret" {
			t.Errorf("password = %q, want the configured one", opts.Password)
		}
	})

	t.Run("redis:// is explicit plaintext", func(t *testing.T) {
		opts, err := redisOptions(config.Config{RedisAddr: "redis://localhost:6379"})
		if err != nil {
			t.Fatal(err)
		}
		if opts.TLSConfig != nil {
			t.Error("redis:// asked for plaintext and must get it")
		}
	})

	// The exact two forms the platform configures, from oci-config. Not invented shapes: the
	// Java side writes a URL in every environment, and these are copied from
	// application-default.yml and application-ocidev.yml.
	t.Run("the platform's local url: credentials in the url, no TLS", func(t *testing.T) {
		opts, err := redisOptions(config.Config{RedisAddr: "redis://default:p%40ss%3Aword@localhost:6379"})
		if err != nil {
			t.Fatal(err)
		}
		if opts.TLSConfig != nil {
			t.Error("local redis speaks plaintext")
		}
		// Percent-decoded by ParseURL, which is the reason to use it rather than split the
		// string by hand: the @ and the : that need escaping are also the delimiters.
		if opts.Password != "p@ss:word" {
			t.Errorf("password = %q, want the percent-decoded one", opts.Password)
		}
		if opts.Username != "default" {
			t.Errorf("username = %q, want default", opts.Username)
		}
	})

	t.Run("the platform's OCI url: TLS, and no password anywhere", func(t *testing.T) {
		// The managed instance has no auth — `redis.url` in application-ocidev.yml carries no
		// credentials — so nothing must invent one.
		opts, err := redisOptions(config.Config{
			RedisAddr: "rediss://aaay56thcyacium5sz4tfcbsz75hhvfplieulowym5egxlep26xt4gq-p.redis.us-ashburn-1.oci.oraclecloud.com:6379",
		})
		if err != nil {
			t.Fatal(err)
		}
		if opts.TLSConfig == nil {
			t.Fatal("rediss:// must speak TLS")
		}
		if opts.Password != "" {
			t.Errorf("password = %q, want none", opts.Password)
		}
	})

	t.Run("a malformed address is an error, not a silent plaintext guess", func(t *testing.T) {
		if _, err := redisOptions(config.Config{RedisAddr: "http://nonsense:1"}); err == nil {
			t.Error("an address that is not a redis URL must be reported")
		}
	})
}

// TestHealthcheckProbesTheConfiguredPort keeps the container healthcheck honest.
//
// The runtime image is distroless: no shell, no curl. `engine -healthcheck` is the only thing a
// HEALTHCHECK can run, so if it stops working the compose file reports a container healthy on
// the strength of the process having started.
func TestHealthcheckProbesTheConfiguredPort(t *testing.T) {
	var asked string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ANALYTICS_LISTEN_ADDR", ":"+port)

	if err := healthcheck(); err != nil {
		t.Fatalf("a serving engine reported unhealthy: %v", err)
	}
	if asked != "/healthz" {
		t.Errorf("probed %q, want /healthz", asked)
	}
}

// And the other direction: nothing listening must be a failure, not a pass.
func TestHealthcheckFailsWhenNothingIsListening(t *testing.T) {
	// Port 1 needs root to bind, so nothing of ours is ever on it.
	t.Setenv("ANALYTICS_LISTEN_ADDR", ":1")
	if err := healthcheck(); err == nil {
		t.Error("healthcheck passed with nothing listening; the container would report healthy")
	}
}
