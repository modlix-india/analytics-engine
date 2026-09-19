// Package objstore is the engine's object storage seam.
//
// Four operations, deliberately. A narrow interface is what lets the replicator be tested
// against an in-memory fake rather than against a real bucket, and it is what keeps the
// engine portable across S3-compatible providers — which in practice differ in exactly the
// features a wider interface would have used.
//
// # What object storage is for here
//
// Two different jobs, and conflating them is how the design goes wrong:
//
//   - WAL segments are replicated for DURABILITY. They close the window where a node dies
//     holding events that exist nowhere else. They are row-oriented and answer no query.
//   - Parquet is replicated for QUERYABILITY, and it is what makes a site's history readable
//     by any node. That is the property that turns adding a node into a configuration change:
//     the new node takes its share of new traffic and reads everything prior from the bucket,
//     with no backfill and no rebalancing.
package objstore

import (
	"context"
	"io"
	"time"
)

type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

type Store interface {
	// Put writes an object. Implementations must be idempotent on key: re-uploading the same
	// file must be harmless, because the replicator re-uploads rather than tracking state it
	// would have to keep durable.
	Put(ctx context.Context, key string, r io.Reader, size int64) error

	// Get opens an object. The caller closes it.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// List enumerates keys under a prefix, recursively.
	List(ctx context.Context, prefix string) ([]Object, error)

	// Delete removes an object. Deleting a key that does not exist is not an error: the
	// replicator deletes remote WAL copies once their Parquet is durable, and both it and a
	// previous run may reach the same key.
	Delete(ctx context.Context, key string) error
}
