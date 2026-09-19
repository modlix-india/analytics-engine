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

	"github.com/modlix-india/analytics-engine/internal/compact"
	"github.com/modlix-india/analytics-engine/internal/config"
	"github.com/modlix-india/analytics-engine/internal/httpapi"
	"github.com/modlix-india/analytics-engine/internal/ingest"
	"github.com/modlix-india/analytics-engine/internal/metrics"
	"github.com/modlix-india/analytics-engine/internal/objstore"
	"github.com/modlix-india/analytics-engine/internal/query"
	"github.com/modlix-india/analytics-engine/internal/replicate"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

// version is stamped at build time: -ldflags "-X main.version=$GITHUB_SHA".
// Reported on analytics_build_info so a rollout is observable rather than a matter of trust.
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
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

	ing := ingest.New(ingest.Options{
		Sink: w,
		// The identity resolver: the host is the site. An embedder replaces this with one
		// that maps hosts onto its own tenancy, without that vocabulary reaching this repo.
		Resolver:       ingest.HostResolver{},
		Log:            log,
		MaxBatchEvents: cfg.MaxBatchEvents,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		OnEvent: func(outcome string) {
			m.EventsReceived.WithLabelValues(outcome).Inc()
		},
	})

	// SIGTERM is what a container orchestrator sends, SIGINT what a terminal sends. Both mean
	// the same thing here: stop taking work and finish what is in hand.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	defer func() { <-compactDone }()

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
		defer func() { <-replicateDone }()
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
		Query: &query.Handler{
			Engine: &query.Engine{DataDir: cfg.DataDir, Remote: remote, Log: log},
			Auth:   auth, Log: log,
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

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	// JSON to stdout: the deployed stacks collect container stdout, and a structured line is
	// the difference between grepping and querying when an incident spans several nodes.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
