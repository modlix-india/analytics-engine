// Command engine ingests analytics events, stores them as Parquet, and answers a fixed set of
// analytics questions.
//
// One binary, three concerns: an ingest path that acknowledges in microseconds by appending to
// a write-ahead log, a compactor that folds closed segments into columnar files, and a query
// path that reads rollups. The WAL is the queue, which is what makes a message broker
// something to add under load rather than a prerequisite for starting.
//
// See PLAN.md for why each of those is shaped the way it is.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"fmt"
	"github.com/modlix-india/analytics-engine/internal/compact"
	"net"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/modlix-india/analytics-engine/internal/config"
	"github.com/modlix-india/analytics-engine/internal/httpapi"
	"github.com/modlix-india/analytics-engine/internal/ingest"
	"github.com/modlix-india/analytics-engine/internal/metrics"
	"github.com/modlix-india/analytics-engine/internal/modlix"
	"github.com/modlix-india/analytics-engine/internal/objstore"
	"github.com/modlix-india/analytics-engine/internal/query"
	"github.com/modlix-india/analytics-engine/internal/replicate"
	"github.com/modlix-india/analytics-engine/internal/sdk"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

// version is stamped at build time: -ldflags "-X main.version=$GITHUB_SHA".
// Reported on analytics_build_info so a rollout is observable rather than a matter of trust.
var version = "dev"

func main() {
	// `engine -healthcheck` asks the engine already running in this container whether it is
	// well, and exits 0 or 1. It exists because the runtime image is distroless: there is no
	// shell, no curl and no wget in it, so a Docker HEALTHCHECK has exactly one executable to
	// work with — this one. Without it the compose healthcheck would have to be dropped, and
	// "up" would mean the process started rather than that it can serve.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		if err := healthcheck(); err != nil {
			slog.Error("unhealthy", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// healthcheck probes this container's own listener.
//
// Over the loopback and the configured port, so it follows ANALYTICS_LISTEN_ADDR rather than
// assuming 8080 and reporting a healthy container that nothing can reach.
func healthcheck() error {
	addr := config.ListenAddr()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address %q: %w", addr, err)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("/healthz answered %d", res.StatusCode)
	}
	return nil
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	log.Info("starting",
		"version", version,
		"node_id", cfg.NodeID,
		"data_dir", cfg.DataDir,
		// Logged at boot because it is the durability policy, and the one setting whose
		// effect is invisible until the moment it matters.
		// .String() because slog renders a time.Duration as raw nanoseconds in JSON, and
		// "100000000" is not what anyone wants to read while working out why freshness
		// looks wrong.
		"wal_sync_interval", cfg.WALSyncInterval.String(),
		"compact_interval", cfg.CompactInterval.String(),
	)

	m := metrics.New(version)

	// SIGTERM is what a container orchestrator sends, SIGINT what a terminal sends. Both mean
	// the same thing here: stop taking work and finish what is in hand.
	//
	// Created before anything that runs in the background, because each of those has to be
	// given a context that a shutdown actually reaches.
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A cancel of our own, on top of the signal context. Everything below can return an
	// error, and the background loops started below are waited on by deferred receives. A
	// deferred `stop()` is registered first and therefore runs LAST, so without this a
	// startup failure would block forever on loops nobody had told to stop: the process
	// exits only when someone kills it, and a supervisor sees a running engine that is
	// serving nothing. Measured: a port conflict hung for 180s and was still going.
	ctx, stopLoops := context.WithCancel(sigCtx)
	defer stopLoops()

	w, err := wal.Open(wal.Options{
		Dir:          filepath.Join(cfg.DataDir, "wal"),
		SegmentBytes: cfg.WALSegmentBytes,
		SegmentAge:   cfg.WALSegmentAge,
		SyncInterval: cfg.WALSyncInterval,
		Log:          log,
		OnSync: func(d time.Duration, records int) {
			m.WALSyncSeconds.Observe(d.Seconds())
			m.WALUnsyncedEvents.Set(0)
		},
	})
	if err != nil {
		return err
	}
	// Closing the WAL performs a final group commit, so a clean shutdown loses nothing that
	// was already acknowledged.
	defer func() {
		if err := w.Close(); err != nil {
			log.Error("wal close", "err", err)
		}
	}()

	// The identity resolver is the default: the host is the site. Configuring a security
	// service swaps in the Modlix one, which maps hosts and path-prefixed URLs onto
	// appCode_clientCode. The engine itself learns neither vocabulary.
	var resolver ingest.SiteResolver = ingest.HostResolver{}
	if cfg.SecurityURL != "" {
		pathHosts := map[string]bool{}
		for _, h := range cfg.PathHosts {
			pathHosts[h] = true
		}
		if len(pathHosts) == 0 {
			log.Warn("no ANALYTICS_PATH_HOSTS: apps served from a shared host with a path prefix " +
				"will not be measured, because a path prefix cannot be believed without one")
		}

		mr := &modlix.Resolver{
			Security:    modlix.NewSecurity(cfg.SecurityURL, 5*time.Second),
			Shared:      sharedCache(cfg, log),
			PathHosts:   pathHosts,
			Log:         log,
			LocalTTL:    cfg.ResolveTTL,
			NegativeTTL: cfg.ResolveNegativeTTL,
			Budget:      cfg.ResolveBudget,
			OnResolve:   func(outcome string) { m.SitesResolved.WithLabelValues(outcome).Inc() },
		}
		// Subscribes to the platform's eviction announcements, so a changed client URL
		// reaches this node without waiting out a TTL.
		mr.Start(ctx)
		resolver = mr
		log.Info("modlix resolver enabled", "security", cfg.SecurityURL, "redis", cfg.RedisAddr != "",
			"path_hosts", cfg.PathHosts)
	}

	ing := ingest.New(ingest.Options{
		Sink:           w,
		Resolver:       resolver,
		Log:            log,
		MaxBatchEvents: cfg.MaxBatchEvents,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		OnEvent: func(outcome string) {
			m.EventsReceived.WithLabelValues(outcome).Inc()
		},
	})

	// Object storage is optional. Without it everything stays local, which is correct for
	// development and for a single node with its own backups.
	var remote objstore.Store
	if cfg.S3Bucket != "" {
		s3, err := objstore.NewS3(objstore.S3Config{
			Endpoint: cfg.S3Endpoint, Region: cfg.S3Region, Bucket: cfg.S3Bucket,
			AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey,
			Prefix: cfg.S3Prefix, UseSSL: cfg.S3UseSSL,
		})
		if err != nil {
			return err
		}
		remote = s3
		log.Info("object storage enabled", "bucket", cfg.S3Bucket, "prefix", cfg.S3Prefix, "tls", cfg.S3UseSSL,
			"local_retention", cfg.LocalRetention.String(), "remote_retention", cfg.RemoteRetention.String())
	} else {
		log.Info("object storage not configured: all data stays on local disk")
	}

	comp := compact.New(compact.Options{
		WAL: w, DataDir: cfg.DataDir, NodeID: cfg.NodeID,
		Interval: cfg.CompactInterval, Log: log,
		OnLag: func(d time.Duration) { m.CompactionLagSeconds.Set(d.Seconds()) },
	})

	// Runs until the context is cancelled, then compacts once more. compactDone gates
	// shutdown on that final pass finishing, or the last segments would sit unqueryable
	// until the process next starts.
	compactDone := make(chan struct{})
	go func() {
		defer close(compactDone)
		comp.Run(ctx)
	}()
	// stopLoops before the receive, not just at the end of run: the wait is only safe if the
	// loop has been told to finish, and each wait therefore guarantees its own precondition
	// rather than relying on the order the defers were registered in.
	defer func() { stopLoops(); <-compactDone }()

	if remote != nil {
		rep := replicate.New(replicate.Options{
			Store: remote, WAL: w, DataDir: cfg.DataDir, NodeID: cfg.NodeID, Log: log,
			Interval:        cfg.ReplicateInterval,
			LocalRetention:  cfg.LocalRetention,
			RemoteRetention: cfg.RemoteRetention,
			OnUpload:        func(kind string, n int64) { m.UploadedBytes.WithLabelValues(kind).Add(float64(n)) },
		})
		// Gated on shutdown like the compactor, so the last compaction's Parquet is not left
		// only on a local disk that may be about to go away with the container.
		replicateDone := make(chan struct{})
		go func() {
			defer close(replicateDone)
			rep.Run(ctx)
		}()
		defer func() { stopLoops(); <-replicateDone }()
	}

	// Reads are denied unless a secret is configured. An embedder replaces SharedSecret with
	// an authorizer that checks access per site against its own security service.
	var auth query.Authorizer = query.DenyAll{}
	if cfg.QuerySecret != "" {
		auth = query.SharedSecret{Secret: cfg.QuerySecret}
	} else {
		log.Warn("query endpoint will deny every request: ANALYTICS_QUERY_SECRET is not set")
	}

	srv := httpapi.New(httpapi.Options{
		Addr:    cfg.ListenAddr,
		Log:     log,
		Metrics: m,
		Ingest:  ing.Handle,
		SDK:     sdk.Handler(),
		Query: &query.Handler{
			Engine: &query.Engine{
				DataDir: cfg.DataDir, Remote: remote, Log: log,
				DefaultTimezone: cfg.DefaultTimezone,
			},
			Auth: auth, Log: log,
		},
		// A node whose disk has stopped accepting writes is alive but must not be sent
		// traffic. Reporting the WAL's last sync error is what takes it out of rotation
		// instead of letting it accept events it cannot keep.
		Ready: func(context.Context) error { return w.Err() },
	})

	if err := srv.Start(ctx); err != nil {
		return err
	}

	log.Info("stopped")
	return nil
}

// sharedCache is the resolver's cross-node cache.
//
// Without Redis each node keeps its own in-process one: correct, and fine for a single node,
// but it resolves every host once per node and hears none of the platform's eviction
// announcements. That is a fleet-size decision rather than a correctness one, so it is a
// missing variable rather than a refusal — said out loud at boot, because the failure it
// leads to (one node serving stale sites after a URL change) is hard to attribute later.
func sharedCache(cfg config.Config, log *slog.Logger) modlix.Shared {
	if cfg.RedisAddr == "" {
		log.Warn("no ANALYTICS_REDIS_ADDR: site resolutions are cached per node and no eviction reaches them")
		return modlix.NewMemoryShared()
	}
	opts, err := redisOptions(cfg)
	if err != nil {
		// A malformed address is a deployment mistake, and the alternative is a node that
		// silently caches per-process and never hears an eviction.
		log.Error("ANALYTICS_REDIS_ADDR could not be parsed; falling back to a per-node cache",
			"addr", cfg.RedisAddr, "err", err)
		return modlix.NewMemoryShared()
	}

	return &modlix.RedisShared{
		Client: redis.NewClient(opts),
		Prefix: cfg.RedisPrefix,
		TTL:    cfg.ResolveTTL,
		Log:    log,
	}
}

// redisOptions turns the configured address into a client, with TLS where the address asks
// for it.
//
// The platform's Redis in every OCI environment is a managed instance reached over
// `rediss://` — TLS is not optional there, it is the only thing the endpoint speaks. Built
// with a bare Addr and no TLSConfig, as this was, the client cannot connect at all: the
// resolver falls back to caching per node and hears no eviction, so a URL change leaves some
// nodes serving the old site indefinitely. Nothing in the logs says "TLS"; it reads as a
// connection that will not come up.
//
// A bare `host:6379` keeps meaning plaintext, unlike the S3 endpoint a few lines away in
// config.go where a bare hostname implies TLS. The two defaults differ because the
// populations do: an S3 endpoint without a scheme is a real provider on the internet, while
// a Redis address without one is the local or in-VCN instance this has always been pointed
// at, and flipping it would break every existing deployment on upgrade.
func redisOptions(cfg config.Config) (*redis.Options, error) {
	if !strings.Contains(cfg.RedisAddr, "://") {
		return &redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		}, nil
	}

	// ParseURL understands `rediss://` and sets a TLSConfig for it, along with any user,
	// password and database index carried in the URL.
	opts, err := redis.ParseURL(cfg.RedisAddr)
	if err != nil {
		return nil, err
	}
	// The separate variables win when set, because the deployed configuration keeps the
	// password out of the URL — the same split the Java side uses.
	if cfg.RedisPassword != "" {
		opts.Password = cfg.RedisPassword
	}
	if cfg.RedisDB != 0 {
		opts.DB = cfg.RedisDB
	}
	return opts, nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	// JSON to stdout: the deployed stacks collect container stdout, and a structured line is
	// the difference between grepping and querying when an incident spans several nodes.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
