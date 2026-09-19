// Package config loads the engine's settings from the environment.
//
// Environment variables rather than a file, for the same reason the bridge does it: these
// processes are configured by the compose file on the host, and inventing a second mechanism
// would mean two places to look when a setting is wrong.
//
// Every duration and size below is a policy choice with a consequence, so each carries the
// reasoning rather than only the value. A reader changing one should be able to see what it
// costs without reading the code that consumes it.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// NodeID names this process within the fleet. It becomes an element of every Parquet
	// filename this node writes, which is what lets several nodes write into one shared
	// bucket with no coordination and no name collisions.
	//
	// It must therefore be stable across restarts and unique across the fleet. A container
	// hostname satisfies neither reliably; a deliberate name satisfies both.
	NodeID string

	// DataDir holds the WAL and the local Parquet tier. One directory so a deployment mounts
	// one volume, and so "how much disk is analytics using" has one answer.
	DataDir string

	ListenAddr string

	// WALSegmentBytes and WALSegmentAge bound a segment, whichever is reached first.
	//
	// The age bound exists for the quiet case: without it a site receiving a handful of events
	// an hour would keep one segment open for days, and everything in it stays unqueryable
	// until compaction, which only runs on a closed segment.
	WALSegmentBytes int64
	WALSegmentAge   time.Duration

	// WALSyncInterval is the group-commit window: appends are acknowledged once the batch
	// containing them is fsynced, and batches close on this interval.
	//
	// This is the durability policy, stated as a number. Per-event fsync would cap throughput
	// at what the disk does — hundreds a second on network storage — while group commit
	// reaches tens of thousands. The price is that a hard kill loses at most this much time
	// of events. For analytics that is the right trade, but it is a trade, and it is the kind
	// of thing that is invisible until somebody assumes otherwise.
	WALSyncInterval time.Duration

	// CompactInterval is how often closed WAL segments are folded into Parquet. It sets query
	// freshness: a dashboard cannot see an event until its segment has been compacted.
	CompactInterval time.Duration

	// MaxBatchEvents and MaxBodyBytes bound one ingest request.
	//
	// These are not tuning knobs, they are the first line of defence on a public endpoint. An
	// unbounded batch is an out-of-memory vector that needs no authentication to reach.
	MaxBatchEvents int
	MaxBodyBytes   int64

	// QuerySecret authorises reads. Empty means the query endpoint denies everything, which
	// is the right default: an analytics endpoint that is open because nobody set a variable
	// is a data leak that announces itself to no one.
	QuerySecret string

	// Object storage. Empty Bucket disables replication entirely and everything stays local,
	// which is the right default for development and for a single-node deployment that has
	// its own backups.
	S3Endpoint  string
	S3Region    string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Prefix    string

	// S3UseSSL is derived from the endpoint's scheme when it has one, so that writing
	// http://localhost:9000 simply works against a local server. TLS otherwise, because an
	// endpoint given as a bare hostname is a real provider and defaulting that to plaintext
	// would ship credentials in the clear.
	S3UseSSL bool

	ReplicateInterval time.Duration

	// LocalRetention is how long Parquet stays on local disk after being uploaded; queries
	// reach through to the bucket for anything older. Zero keeps everything local.
	LocalRetention time.Duration

	// RemoteRetention is how long it stays in the bucket. Zero keeps it forever, deliberately:
	// deleting a customer's history because a variable was unset is not recoverable.
	RemoteRetention time.Duration

	LogLevel string
}

// Load reads the environment, applies defaults, and refuses to return a Config it knows is
// unusable.
//
// It fails loudly rather than defaulting NodeID, because every other setting here has a
// sensible default and that one does not: a node silently sharing an identity with another
// would have both writing the same Parquet filenames into the same bucket, and the symptom
// would be quietly missing data rather than an error.
func Load() (Config, error) {
	c := Config{
		NodeID:          os.Getenv("ANALYTICS_NODE_ID"),
		DataDir:         envStr("ANALYTICS_DATA_DIR", "/var/lib/analytics"),
		ListenAddr:      envStr("ANALYTICS_LISTEN_ADDR", ":8080"),
		WALSegmentBytes: envInt64("ANALYTICS_WAL_SEGMENT_BYTES", 64<<20),
		WALSegmentAge:   envDur("ANALYTICS_WAL_SEGMENT_AGE", 5*time.Minute),
		WALSyncInterval: envDur("ANALYTICS_WAL_SYNC_INTERVAL", 100*time.Millisecond),
		CompactInterval: envDur("ANALYTICS_COMPACT_INTERVAL", 5*time.Minute),
		MaxBatchEvents:  envInt("ANALYTICS_MAX_BATCH_EVENTS", 500),
		MaxBodyBytes:    envInt64("ANALYTICS_MAX_BODY_BYTES", 1<<20),
		QuerySecret:     os.Getenv("ANALYTICS_QUERY_SECRET"),

		// Both spellings are accepted. The platform's existing stacks already pass OCI_S3_*
		// to their containers, so a deployment can reuse that environment rather than
		// duplicating four secrets under new names.
		S3Endpoint:  firstEnv("ANALYTICS_S3_ENDPOINT", "OCI_S3_ENDPOINT"),
		S3Region:    firstEnv("ANALYTICS_S3_REGION", "OCI_S3_REGION"),
		S3Bucket:    firstEnv("ANALYTICS_S3_BUCKET", "OCI_S3_BUCKET"),
		S3AccessKey: firstEnv("ANALYTICS_S3_ACCESS_KEY", "OCI_S3_ACCESS_KEY"),
		S3SecretKey: firstEnv("ANALYTICS_S3_SECRET_KEY", "OCI_S3_SECRET_KEY"),
		S3Prefix:    envStr("ANALYTICS_S3_PREFIX", ""),

		ReplicateInterval: envDur("ANALYTICS_REPLICATE_INTERVAL", time.Minute),
		LocalRetention:    envDur("ANALYTICS_LOCAL_RETENTION", 0),
		RemoteRetention:   envDur("ANALYTICS_REMOTE_RETENTION", 0),

		LogLevel: envStr("ANALYTICS_LOG_LEVEL", "info"),
	}

	if strings.TrimSpace(c.NodeID) == "" {
		return Config{}, fmt.Errorf("ANALYTICS_NODE_ID is required and has no safe default")
	}
	if strings.ContainsAny(c.NodeID, "/\\. ") {
		// It lands in a filename. Rejecting here turns a whole class of confusing path bugs
		// into one clear refusal at boot.
		return Config{}, fmt.Errorf("ANALYTICS_NODE_ID %q must not contain a separator, dot or space", c.NodeID)
	}
	if c.WALSyncInterval <= 0 {
		return Config{}, fmt.Errorf("ANALYTICS_WAL_SYNC_INTERVAL must be positive")
	}
	c.S3UseSSL = deriveSSL(c.S3Endpoint)

	if c.S3Bucket != "" && (c.S3Endpoint == "" || c.S3AccessKey == "" || c.S3SecretKey == "") {
		// Half-configured object storage is worse than none: the node would look replicated,
		// prune nothing, and fail every upload into the log.
		return Config{}, fmt.Errorf("S3 bucket is set but endpoint, access key or secret key is missing")
	}
	if c.LocalRetention > 0 && c.S3Bucket == "" {
		// Local retention deletes data once it is uploaded. With nowhere to upload to, it
		// would be a data-loss setting wearing a cost-saving name.
		return Config{}, fmt.Errorf("ANALYTICS_LOCAL_RETENTION requires object storage to be configured")
	}

	return c, nil
}

// deriveSSL decides whether to speak TLS to the endpoint.
//
// An explicit ANALYTICS_S3_USE_SSL wins. Otherwise the endpoint's own scheme decides, and a
// bare hostname means TLS — the safe direction, since a bare hostname in a deployment is a
// real provider and sending credentials to it in the clear is not a mistake worth making
// convenient.
func deriveSSL(endpoint string) bool {
	if v := strings.TrimSpace(os.Getenv("ANALYTICS_S3_USE_SSL")); v != "" {
		return v == "true" || v == "1"
	}
	if strings.HasPrefix(endpoint, "http://") {
		return false
	}
	return true
}

// firstEnv returns the first of the given variables that is set.
func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v, err := strconv.Atoi(envStr(key, ""))
	if err != nil {
		return def
	}
	return v
}

func envInt64(key string, def int64) int64 {
	v, err := strconv.ParseInt(envStr(key, ""), 10, 64)
	if err != nil {
		return def
	}
	return v
}

func envDur(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(envStr(key, ""))
	if err != nil {
		return def
	}
	return v
}
