package objstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

// The S3 implementation against a real server.
//
// Opt-in, because it needs one running. The Memory fake covers the replicator's logic, but it
// cannot catch the things that actually go wrong with object storage — path-style addressing,
// prefix handling, whether a missing key errors on Get or on the first Read. Those are
// properties of a server, so they need a server.
//
//	docker run -d --rm -p 19000:9000 --name ae-minio \
//	  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//	  minio/minio server /data
//	AE_S3_ENDPOINT=localhost:19000 AE_S3_BUCKET=test go test ./internal/objstore/ -run TestS3
func s3FromEnv(t *testing.T) *S3 {
	t.Helper()
	endpoint := os.Getenv("AE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set AE_S3_ENDPOINT to run the object storage integration tests")
	}

	s, err := NewS3(S3Config{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    os.Getenv("AE_S3_BUCKET"),
		AccessKey: os.Getenv("AE_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("AE_S3_SECRET_KEY"),
		Prefix:    "itest",
		UseSSL:    os.Getenv("AE_S3_SSL") == "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestS3RoundTrip(t *testing.T) {
	s := s3FromEnv(t)
	ctx := context.Background()

	body := []byte("parquet bytes, notionally")
	key := "data/s.example/2026-09-19/n1-000000000001.parquet"

	if err := s.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { s.Delete(ctx, key) })

	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("round trip changed the bytes: %q", got)
	}
}

// List must return engine-relative keys. If the deployment prefix leaked out, every caller
// would compare against paths that do not match its own and re-upload everything forever.
func TestS3ListStripsThePrefix(t *testing.T) {
	s := s3FromEnv(t)
	ctx := context.Background()

	key := "rollup/s.example/2026-09-19/n1-000000000002.parquet"
	if err := s.Put(ctx, key, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Delete(ctx, key) })

	objs, err := s.List(ctx, "rollup/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	var found bool
	for _, o := range objs {
		if strings.HasPrefix(o.Key, "itest") {
			t.Errorf("List returned %q with the deployment prefix still attached", o.Key)
		}
		if o.Key == key {
			found = true
		}
	}
	if !found {
		t.Errorf("List did not return %q; got %d objects", key, len(objs))
	}
}

// Get on a missing key must fail immediately rather than on first read, or callers cannot tell
// "not found" from a truncated download.
func TestS3GetMissingKeyFailsEagerly(t *testing.T) {
	s := s3FromEnv(t)

	if _, err := s.Get(context.Background(), "data/nope/2026-01-01/missing.parquet"); err == nil {
		t.Error("Get on a missing key returned no error")
	}
}

// Deleting a key that is not there is not an error: the replicator drops remote WAL copies
// once compacted, and a previous run may have reached the same key.
func TestS3DeleteMissingKeyIsNotAnError(t *testing.T) {
	s := s3FromEnv(t)

	if err := s.Delete(context.Background(), "data/nope/2026-01-01/missing.parquet"); err != nil {
		t.Errorf("Delete on a missing key = %v, want nil", err)
	}
}
