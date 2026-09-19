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
