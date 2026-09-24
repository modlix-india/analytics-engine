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

	// VisitorSecret makes the daily visitor salt survive a restart, and makes a fleet agree
	// about who a visitor is.
	//
	// Without it each process draws a fresh random salt the first time it sees a day, so a
	// deploy at noon splits that day's visitors into two populations and two nodes never
	// agree at all. With it the day's salt is HMAC(secret, date): same daily rotation, same
	// unlinkability once the day passes, no restart artefact.
	//
	// It must be secret. Anyone holding it can re-derive today's visitor ids from an IP and
	// a user-agent, which is exactly what the rotation exists to prevent.
	VisitorSecret string

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

	// SecurityURL turns on the Modlix site resolver. Empty leaves the standalone one in
	// place, where the hostname IS the site — which is what the MIT repo is for and what
	// local development needs.
	//
	// This must be a service address on a network the outside world cannot reach: the
	// endpoints it calls are `/internal/` ones, protected by nginx rather than by the
	// gateway, whose `(.*internal.*)` rule does not do what its name suggests.
	SecurityURL string

	// PathHosts are the hosts on which a path-prefixed URL (/appCode/clientCode/page/...) is
	// allowed to name the site: the platform's own shared hosts. Comma-separated.
	//
	// Empty disables the path form, which is the safe default — a page always agrees with
	// its own Origin, so without a list of hosts we actually serve apps from, any site could
	// name any app by putting it in its own URL.
	PathHosts []string

	// ResolveBudget is the longest ingest will wait for a site lookup before dropping the
	// event. Small on purpose: the WAL append must never queue behind a dependency, and a
	// dropped pageview costs a rounding error where a stalled ingest path costs an outage.
	ResolveBudget time.Duration

	// ResolveTTL and ResolveNegativeTTL are how long a resolution and a non-resolution are
	// remembered. Separate settings because they are different risks: a stale real site
	// delays a customer's first numbers, while a short negative TTL lets a scanner turn
	// every request into a security call.
	ResolveTTL         time.Duration
	ResolveNegativeTTL time.Duration

	// Redis is the shared half of the resolver's cache, and the reason a fleet of nodes
	// resolves a host once rather than once each. Empty is valid: each node then keeps its
	// own in-process cache and hears no eviction announcements, which is correct for one
	// node and wrong for several.
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// RedisPrefix is `redis.cache.prefix` on the Java side — "cmn" in every environment
	// today. It has to match, because it is half of both the hash name we read and the
	// eviction messages we listen for. Set it wrong and the cache works perfectly and is
	// never invalidated, which is the hardest version of this bug to notice.
	RedisPrefix string

	// DefaultTimezone is the zone a query uses when the caller names none.
	//
	// UTC here, deliberately, even though Modlix's own default is Asia/Kolkata: this engine
	// has no opinion about where its operator lives, and a default that silently follows
	// the deployment produces numbers nobody can reproduce. The Modlix deployment sets it,
	// and `ui` resolves the real chain per request — request zone, then the client's
	// `security_client.TIME_ZONE`, then this.
	DefaultTimezone string

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
		VisitorSecret:   os.Getenv("ANALYTICS_VISITOR_SECRET"),

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

		SecurityURL:        envStr("ANALYTICS_SECURITY_URL", ""),
		PathHosts:          envList("ANALYTICS_PATH_HOSTS"),
		ResolveBudget:      envDur("ANALYTICS_RESOLVE_BUDGET", 250*time.Millisecond),
		ResolveTTL:         envDur("ANALYTICS_RESOLVE_TTL", 5*time.Minute),
		ResolveNegativeTTL: envDur("ANALYTICS_RESOLVE_NEGATIVE_TTL", time.Minute),

		RedisAddr:     envStr("ANALYTICS_REDIS_ADDR", ""),
		RedisPassword: os.Getenv("ANALYTICS_REDIS_PASSWORD"),
		RedisDB:       envInt("ANALYTICS_REDIS_DB", 0),
		RedisPrefix:   envStr("ANALYTICS_REDIS_PREFIX", "cmn"),

		DefaultTimezone: envStr("ANALYTICS_DEFAULT_TIMEZONE", "UTC"),

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
	if _, err := time.LoadLocation(c.DefaultTimezone); err != nil {
		// Refused at boot rather than per query: an unknown zone would otherwise surface
		// as every dashboard erroring at once, long after the variable was set.
		return Config{}, fmt.Errorf("ANALYTICS_DEFAULT_TIMEZONE %q is not an IANA zone: %w", c.DefaultTimezone, err)
	}
	if c.RedisAddr != "" && c.SecurityURL == "" {
		// Redis here exists only to share site resolutions between nodes. Configured
		// without a resolver it caches nothing and quietly suggests otherwise.
		return Config{}, fmt.Errorf("ANALYTICS_REDIS_ADDR is set but ANALYTICS_SECURITY_URL is not, so there is nothing to cache")
	}
	if c.LocalRetention > 0 && c.S3Bucket == "" {
		// Local retention deletes data once it is uploaded. With nowhere to upload to, it
		// would be a data-loss setting wearing a cost-saving name.
		return Config{}, fmt.Errorf("ANALYTICS_LOCAL_RETENTION requires object storage to be configured")
	}

	return c, nil
}

// ListenAddr is the address the engine serves on, read without loading the rest of the
// configuration.
//
// The healthcheck sub-command needs this and nothing else, and it must not go through Load():
// Load validates the whole configuration and fails when a required variable is missing, which
// would make an unhealthy answer indistinguishable from a misconfigured one.
func ListenAddr() string {
	return envStr("ANALYTICS_LISTEN_ADDR", ":8080")
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

// envList reads a comma-separated list, discarding blanks and surrounding space so that a
// value pasted across lines in a compose file behaves the way it looks.
func envList(key string) []string {
	raw := os.Getenv(key)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.ToLower(strings.TrimSpace(part)); p != "" {
			out = append(out, p)
		}
	}
	return out
}
