package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory Store for tests.
//
// It exists so the replicator's logic — what to upload, what to delete, what to prune — is
// tested without a bucket, a container or a network. The S3 implementation is thin enough
// that its risk is configuration rather than logic, and configuration is not what a unit test
// would have caught anyway.
type Memory struct {
	mu   sync.Mutex
	objs map[string][]byte
	when map[string]time.Time

	// FailPut, when set, makes every Put fail. The replicator must not delete anything local
	// on the strength of an upload that did not happen, and that is worth testing directly.
	FailPut error
}

func NewMemory() *Memory {
	return &Memory{objs: map[string][]byte{}, when: map[string]time.Time{}}
}

func (m *Memory) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailPut != nil {
		return m.FailPut
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.objs[key] = b
	m.when[key] = time.Now()
	return nil
}

func (m *Memory) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("objstore: %q not found", key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *Memory) List(_ context.Context, prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []Object
	for k, v := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, Object{Key: k, Size: int64(len(v)), Modified: m.when[k]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	delete(m.when, key)
	return nil
}

// Keys is a test helper.
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objs))
	for k := range m.objs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
