package rollup

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// WriteFile writes rollup rows atomically, for the same reason store.WriteFile does: the query
// path reads this directory while compaction writes it, and a reader must see a whole file or
// no file.
func WriteFile(path string, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)

	w := parquet.NewGenericWriter[Row](f)
	if _, err := w.Write(rows); err != nil {
		f.Close()
		return err
	}
	if err := w.Close(); err != nil {
		f.Close()
		return err
	}
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

	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func ReadFile(path string) ([]Row, error) {
	return parquet.ReadFile[Row](path)
}

// SortForWrite orders rows deterministically. Exposed for tests that build rows by hand.
func SortForWrite(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Hour != b.Hour {
			return a.Hour < b.Hour
		}
		if a.Event != b.Event {
			return a.Event < b.Event
		}
		if a.Dim != b.Dim {
			return a.Dim < b.Dim
		}
		return a.Key < b.Key
	})
}
