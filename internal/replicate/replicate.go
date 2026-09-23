// Package replicate ships the local tiers to object storage and prunes what is no longer
// needed locally.
//
// # The two jobs, kept separate
//
//   - WAL segments go up for DURABILITY, and come down again once their events exist as
//     Parquet. They close the window where a node dies holding events that exist nowhere else.
//   - Parquet goes up for QUERYABILITY and stays. It is what lets any node read any site's
//     history, which is what makes adding a node a configuration change rather than a
//     migration.
//
// # Ordering, which is the whole safety argument
//
// Nothing local is ever deleted before its replacement is durable somewhere else:
//
//	segment closed ──► uploaded ──► compacted to Parquet ──► local segment deleted (compactor)
//	                                         │
//	                                         └─► Parquet uploaded ──► remote WAL copy deleted
//	                                                              └─► local Parquet prunable
//
// A remote WAL copy is deleted only once the local segment has gone, and the compactor only
// deletes a segment after its Parquet is written and fsynced. Local Parquet is pruned only
// after a confirmed upload. Every step waits for the one before it, so a crash anywhere leaves
// data in at least one place.
package replicate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/modlix-india/analytics-engine/internal/objstore"
	"github.com/modlix-india/analytics-engine/internal/wal"
)

type Options struct {
	Store   objstore.Store
	WAL     *wal.WAL
	DataDir string
	NodeID  string
	Log     *slog.Logger

	Interval time.Duration

	// LocalRetention is how long Parquet stays on local disk after being uploaded. Recent
	// data is read far more often than old data, so keeping a window locally avoids paying a
	// network round trip for the queries people actually run. Zero keeps everything local.
	LocalRetention time.Duration

	// RemoteRetention is how long data stays in the bucket. Zero keeps it forever, which is
	// the right default: deleting a customer's history because a variable was unset is not a
	// failure anyone recovers from.
	RemoteRetention time.Duration

	OnUpload func(kind string, bytes int64)
}

type Replicator struct {
	opt Options
	log *slog.Logger

	mu       sync.Mutex
	uploaded map[string]bool
}

func New(o Options) *Replicator {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
	return &Replicator{opt: o, log: o.Log, uploaded: map[string]bool{}}
}

// Run replicates on an interval until the context is cancelled, then runs once more.
//
// The final pass matters: a clean shutdown should not leave the last compaction's Parquet
// only on a local disk that may be about to be discarded with the container.
func (r *Replicator) Run(ctx context.Context) {
	// Learn what is already there, so a restart does not re-upload the entire history. Uploads
	// are idempotent on key, so being wrong here is slow rather than incorrect.
	if err := r.loadRemoteIndex(ctx); err != nil {
		r.log.Warn("replicate: could not list the bucket; will re-upload rather than assume", "err", err)
	}

	t := time.NewTicker(r.opt.Interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			r.RunOnce(ctx)
		case <-ctx.Done():
			// A fresh context: the one we were given is already cancelled, and the final
			// upload is the point of this branch.
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			r.RunOnce(final)
			cancel()
			return
		}
	}
}

func (r *Replicator) loadRemoteIndex(ctx context.Context) error {
	for _, prefix := range []string{"data/", "rollup/", r.walPrefix()} {
		objs, err := r.opt.Store.List(ctx, prefix)
		if err != nil {
			return err
		}
		r.mu.Lock()
		for _, o := range objs {
			r.uploaded[o.Key] = true
		}
		r.mu.Unlock()
	}
	return nil
}

func (r *Replicator) walPrefix() string {
	// Namespaced by node: WAL segments are node-private until compacted, unlike Parquet, whose
	// whole purpose is to be shared.
	return "wal/" + r.opt.NodeID + "/"
}

func (r *Replicator) RunOnce(ctx context.Context) {
	if err := r.uploadWAL(ctx); err != nil {
		r.log.Error("replicate: wal", "err", err)
	}
	for _, tier := range []string{"data", "rollup"} {
		if err := r.uploadTier(ctx, tier); err != nil {
			r.log.Error("replicate: parquet", "tier", tier, "err", err)
		}
	}
	if err := r.dropCompactedWAL(ctx); err != nil {
		r.log.Error("replicate: dropping compacted wal copies", "err", err)
	}
	if err := r.pruneLocal(); err != nil {
		r.log.Error("replicate: local retention", "err", err)
	}
	if err := r.pruneRemote(ctx); err != nil {
		r.log.Error("replicate: remote retention", "err", err)
	}
}

// uploadWAL replicates closed segments.
//
// Only closed ones: the active segment is still being appended to, so uploading it would ship
// a prefix and immediately need to ship it again.
func (r *Replicator) uploadWAL(ctx context.Context) error {
	if r.opt.WAL == nil {
		return nil
	}
	segs, err := r.opt.WAL.ClosedSegments()
	if err != nil {
		return err
	}
	for _, seg := range segs {
		key := r.walPrefix() + filepath.Base(seg)
		if err := r.upload(ctx, key, seg, "wal"); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) uploadTier(ctx context.Context, tier string) error {
	root := filepath.Join(r.opt.DataDir, tier)

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // nothing written yet
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		return r.upload(ctx, tier+"/"+filepath.ToSlash(rel), path, tier)
	})
}

func (r *Replicator) upload(ctx context.Context, key, path, kind string) error {
	r.mu.Lock()
	done := r.uploaded[key]
	r.mu.Unlock()
	if done {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Compaction removed it between the listing and here. Not an error: the events
			// are in Parquet, which is what we were protecting.
			return nil
		}
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}

	if err := r.opt.Store.Put(ctx, key, f, st.Size()); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}

	r.mu.Lock()
	r.uploaded[key] = true
	r.mu.Unlock()

	if r.opt.OnUpload != nil {
		r.opt.OnUpload(kind, st.Size())
	}
	r.log.Debug("replicate: uploaded", "key", key, "bytes", st.Size())
	return nil
}

// dropCompactedWAL removes remote segment copies whose local file has gone.
//
// The compactor deletes a segment only after its Parquet is written and fsynced, so a missing
// local segment is proof that its events survive in a form that answers queries. Keeping the
// WAL copy past that point costs storage and protects nothing.
func (r *Replicator) dropCompactedWAL(ctx context.Context) error {
	objs, err := r.opt.Store.List(ctx, r.walPrefix())
	if err != nil {
		return err
	}

	for _, o := range objs {
		local := filepath.Join(r.opt.DataDir, "wal", filepath.Base(o.Key))
		if _, err := os.Stat(local); err == nil {
			continue // still uncompacted
		} else if !os.IsNotExist(err) {
			return err
		}

		if err := r.opt.Store.Delete(ctx, o.Key); err != nil {
			return err
		}
		r.mu.Lock()
		delete(r.uploaded, o.Key)
		r.mu.Unlock()
	}
	return nil
}

// pruneLocal deletes local Parquet past the local retention window, and only if it is known to
// be uploaded.
func (r *Replicator) pruneLocal() error {
	if r.opt.LocalRetention <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().Add(-r.opt.LocalRetention)

	for _, tier := range []string{"data", "rollup"} {
		root := filepath.Join(r.opt.DataDir, tier)

		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".parquet" {
				return nil
			}

			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			date, ok := dateOf(filepath.ToSlash(rel))
			if !ok || !date.Before(cutoff) {
				return nil
			}

			key := tier + "/" + filepath.ToSlash(rel)
			r.mu.Lock()
			up := r.uploaded[key]
			r.mu.Unlock()
			if !up {
				// Never delete local data we have not confirmed is elsewhere. A bucket that
				// is unreachable must cost disk, not data.
				return nil
			}
			return os.Remove(path)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) pruneRemote(ctx context.Context) error {
	if r.opt.RemoteRetention <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().Add(-r.opt.RemoteRetention)

	for _, tier := range []string{"data/", "rollup/"} {
		objs, err := r.opt.Store.List(ctx, tier)
		if err != nil {
			return err
		}
		for _, o := range objs {
			// Dated from the KEY, not from the object's modification time. An object rewritten
			// by a re-run of compaction would otherwise look young and outlive its cohort,
			// leaving a partition with holes in it.
			date, ok := dateOf(strings.TrimPrefix(o.Key, tier))
			if !ok || !date.Before(cutoff) {
				continue
			}
			if err := r.opt.Store.Delete(ctx, o.Key); err != nil {
				return err
			}
			r.mu.Lock()
			delete(r.uploaded, o.Key)
			r.mu.Unlock()
		}
	}
	return nil
}

// dateOf extracts the partition date from a "{site}/{date}/{file}" path.
func dateOf(rel string) (time.Time, bool) {
	parts := strings.Split(rel, "/")
	if len(parts) < 2 {
		return time.Time{}, false
	}
	d, err := time.ParseInLocation("2006-01-02", parts[len(parts)-2], time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}
