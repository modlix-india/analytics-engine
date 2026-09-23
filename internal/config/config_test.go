package config

import (
	"testing"
	"time"
)

// NodeID is the only setting with no safe default, because it lands in every Parquet filename
// this node writes. Two nodes sharing one would write colliding names into a shared bucket and
// the symptom would be missing data, not an error — so these cases guard a silent failure.
func TestLoadRejectsBadNodeID(t *testing.T) {
	for _, tc := range []struct{ name, id string }{
		{"empty", ""},
		{"whitespace only", "   "},
		{"contains slash", "node/1"},
		{"contains backslash", `node\1`},
		{"contains dot", "node.1"},
		{"contains space", "node 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ANALYTICS_NODE_ID", tc.id)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted node id %q; it must refuse", tc.id)
			}
		})
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}

	// Pinned deliberately: this is the durability policy. Changing it changes how much data a
	// hard kill loses, so it should not move without somebody editing this line too.
	if c.WALSyncInterval != 100*time.Millisecond {
		t.Errorf("WALSyncInterval = %v, want 100ms", c.WALSyncInterval)
	}
	if c.MaxBatchEvents <= 0 || c.MaxBodyBytes <= 0 {
		t.Errorf("ingest bounds must be positive, got events=%d bytes=%d", c.MaxBatchEvents, c.MaxBodyBytes)
	}
}

// A malformed duration falls back to the default rather than failing the process. That is the
// right call for a value with a sane default, but it is silent, so it is worth a test saying
// so on purpose rather than leaving it as an accident of envDur.
func TestMalformedDurationFallsBackToDefault(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("ANALYTICS_COMPACT_INTERVAL", "not-a-duration")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if c.CompactInterval != 5*time.Minute {
		t.Errorf("CompactInterval = %v, want the 5m default", c.CompactInterval)
	}
}

func TestZeroSyncIntervalRefused(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("ANALYTICS_WAL_SYNC_INTERVAL", "0s")

	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a zero sync interval; that would fsync per append forever")
	}
}

// Half-configured object storage is worse than none: the node would look replicated, prune
// nothing, and fail every upload into the log where nobody is watching.
func TestHalfConfiguredObjectStorageIsRefused(t *testing.T) {
	for _, missing := range []string{"ANALYTICS_S3_ENDPOINT", "ANALYTICS_S3_ACCESS_KEY", "ANALYTICS_S3_SECRET_KEY"} {
		t.Run("missing "+missing, func(t *testing.T) {
			t.Setenv("ANALYTICS_NODE_ID", "n1")
			t.Setenv("ANALYTICS_S3_BUCKET", "b")
			t.Setenv("ANALYTICS_S3_ENDPOINT", "e")
			t.Setenv("ANALYTICS_S3_ACCESS_KEY", "k")
			t.Setenv("ANALYTICS_S3_SECRET_KEY", "s")
			t.Setenv(missing, "")

			if _, err := Load(); err == nil {
				t.Errorf("Load() accepted a bucket with %s unset", missing)
			}
		})
	}
}

// Local retention deletes data once it has been uploaded. With nowhere to upload to it is a
// data-loss setting wearing a cost-saving name, so it must not be silently ignored.
func TestLocalRetentionWithoutObjectStorageIsRefused(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("ANALYTICS_LOCAL_RETENTION", "168h")

	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted local retention with no bucket configured")
	}
}

// The platform's existing stacks already pass OCI_S3_* into their containers, so a deployment
// should be able to reuse that environment rather than duplicating four secrets.
func TestOCIEnvironmentIsAccepted(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("OCI_S3_BUCKET", "analytics")
	t.Setenv("OCI_S3_ENDPOINT", "ns.compat.objectstorage.ap-mumbai-1.oraclecloud.com")
	t.Setenv("OCI_S3_ACCESS_KEY", "key")
	t.Setenv("OCI_S3_SECRET_KEY", "secret")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if c.S3Bucket != "analytics" || c.S3AccessKey != "key" {
		t.Errorf("OCI_S3_* was not picked up: %+v", c)
	}
}

// The engine's own names win where both are set, so a deployment can override the shared ones.
func TestAnalyticsEnvironmentOverridesOCI(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("OCI_S3_BUCKET", "shared")
	t.Setenv("ANALYTICS_S3_BUCKET", "dedicated")
	t.Setenv("OCI_S3_ENDPOINT", "e")
	t.Setenv("OCI_S3_ACCESS_KEY", "k")
	t.Setenv("OCI_S3_SECRET_KEY", "s")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if c.S3Bucket != "dedicated" {
		t.Errorf("S3Bucket = %q, want the ANALYTICS_ value to win", c.S3Bucket)
	}
}

// Zero retention must mean "keep forever" rather than "delete immediately".
func TestRetentionDefaultsAreZero(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if c.LocalRetention != 0 || c.RemoteRetention != 0 {
		t.Errorf("retention defaults are local=%v remote=%v, want both zero (keep forever)",
			c.LocalRetention, c.RemoteRetention)
	}
}

// TLS is derived from the endpoint's scheme, so http://localhost works for development while
// a bare hostname — which in a deployment means a real provider — still gets TLS. Defaulting
// the latter to plaintext would ship credentials in the clear to make local testing easier.
func TestS3TLSIsDerivedFromTheEndpointScheme(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     bool
	}{
		{"http://localhost:9000", false},
		{"https://ns.compat.objectstorage.ap-mumbai-1.oraclecloud.com", true},
		{"ns.compat.objectstorage.ap-mumbai-1.oraclecloud.com", true},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			t.Setenv("ANALYTICS_NODE_ID", "n1")
			t.Setenv("ANALYTICS_S3_BUCKET", "b")
			t.Setenv("ANALYTICS_S3_ENDPOINT", tc.endpoint)
			t.Setenv("ANALYTICS_S3_ACCESS_KEY", "k")
			t.Setenv("ANALYTICS_S3_SECRET_KEY", "s")

			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if c.S3UseSSL != tc.want {
				t.Errorf("S3UseSSL = %v for %q, want %v", c.S3UseSSL, tc.endpoint, tc.want)
			}
		})
	}
}

func TestS3TLSCanBeForced(t *testing.T) {
	t.Setenv("ANALYTICS_NODE_ID", "n1")
	t.Setenv("ANALYTICS_S3_BUCKET", "b")
	t.Setenv("ANALYTICS_S3_ENDPOINT", "minio.internal:9000")
	t.Setenv("ANALYTICS_S3_ACCESS_KEY", "k")
	t.Setenv("ANALYTICS_S3_SECRET_KEY", "s")
	t.Setenv("ANALYTICS_S3_USE_SSL", "false")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.S3UseSSL {
		t.Error("an explicit ANALYTICS_S3_USE_SSL=false was ignored")
	}
}
