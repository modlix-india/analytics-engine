package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// rowGroupRows caps a row group.
//
// Row-group statistics are what let a reader skip a time range without decompressing it, so
// this is the granularity of time pruning inside a file. Smaller groups prune better and
// compress slightly worse; 50k rows is a few seconds of traffic at the design rate and a few
// hundred KB compressed, which is a reasonable unit to skip or read whole.
const rowGroupRows = 50_000

// WriteFile writes rows to a Parquet file, atomically.
//
// Atomicity is not optional here. The compactor and the query path run concurrently, and a
// reader that opens a half-written Parquet file does not get a short read — it gets a missing
// or truncated footer, which surfaces as a corrupt-file error on a file that is about to
// become perfectly valid. Writing to a temporary name and renaming means a reader sees either
// nothing or the complete file.
func WriteFile(path string, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	// Sorted by server time so row-group statistics form non-overlapping ranges, which is
	// what makes time pruning effective. Stable, because events arriving in the same
	// millisecond should keep their arrival order rather than being shuffled.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TSServer < rows[j].TSServer })

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	// Best-effort cleanup: a leftover .tmp from a crash is harmless — it is never read, and
	// the next run of the same compaction overwrites it — but leaving them around would
	// slowly fill a volume.
	defer os.Remove(tmp)

	w := parquet.NewGenericWriter[Row](f, parquet.MaxRowsPerRowGroup(rowGroupRows))

	if _, err := w.Write(rows); err != nil {
		f.Close()
		return fmt.Errorf("parquet write: %w", err)
	}
	if err := w.Close(); err != nil {
		f.Close()
		return fmt.Errorf("parquet close: %w", err)
	}

	// fsync before the rename, or the rename can be durable while the bytes it points at are
	// not — which is the failure that produces a zero-length Parquet file after a power cut.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// ReadFile reads a Parquet file back. Used by tests and by the query layer.
func ReadFile(path string) ([]Row, error) {
	rows, err := parquet.ReadFile[Row](path)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
