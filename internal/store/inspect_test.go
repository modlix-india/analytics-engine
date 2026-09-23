package store

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestInspect is a debugging aid, not a unit test: point AE_INSPECT_DIR at a data directory
// and it reports what is actually stored. Skipped unless that variable is set, so it costs
// nothing in CI while giving a way to check a real directory without a separate tool.
func TestInspect(t *testing.T) {
	root := os.Getenv("AE_INSPECT_DIR")
	if root == "" {
		t.Skip("set AE_INSPECT_DIR to inspect a data directory")
	}

	total, unsorted := 0, 0
	perSite := map[string]int{}
	visitors, paths := map[string]bool{}, map[string]bool{}
	var sample Row

	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".parquet" {
			return nil
		}
		rows, err := ReadFile(p)
		if err != nil {
			t.Errorf("ReadFile(%s): %v", p, err)
			return nil
		}
		for i, r := range rows {
			total++
			perSite[r.Site]++
			visitors[r.Visitor] = true
			paths[r.Path] = true
			if i > 0 && rows[i-1].TSServer > r.TSServer {
				unsorted++
			}
			if total == 1 {
				sample = r
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("rows              %d", total)
	t.Logf("out-of-order      %d", unsorted)
	t.Logf("distinct visitors %d", len(visitors))
	t.Logf("distinct paths    %d", len(paths))

	sites := make([]string, 0, len(perSite))
	for s := range perSite {
		sites = append(sites, s)
	}
	sort.Strings(sites)
	for _, s := range sites {
		t.Logf("  %-16s %d", s, perSite[s])
	}
	t.Logf("sample: site=%s event=%s path=%s channel=%s browser=%s os=%s device=%s platform=%s country=%s utm_source=%s label=%s",
		sample.Site, sample.Name, sample.Path, sample.Channel, sample.Browser,
		sample.OS, sample.Device, sample.Platform, sample.Country, sample.UTMSource, sample.Label)

	if unsorted > 0 {
		t.Errorf("%d rows are out of ts_server order; row-group pruning depends on the sort", unsorted)
	}
}
