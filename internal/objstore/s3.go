package objstore

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 talks to any S3-compatible endpoint.
//
// Written against OCI Object Storage, which is what the rest of this platform already uses
// for backups, and which speaks the S3 API at
// https://{namespace}.compat.objectstorage.{region}.oraclecloud.com.
type S3 struct {
	client *minio.Client
	bucket string
	prefix string
}

type S3Config struct {
	// Endpoint without a scheme, e.g. "ns.compat.objectstorage.ap-mumbai-1.oraclecloud.com".
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string

	// Prefix namespaces this deployment inside a shared bucket, so dev, stage and production
	// can use one bucket without any chance of a retention sweep in one reaching another.
	Prefix string

	// UseSSL should be true everywhere except against a local test server.
	UseSSL bool
}

func NewS3(cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("objstore: bucket is required")
	}

	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "https://"), "http://")
	endpoint = strings.TrimSuffix(endpoint, "/")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("objstore: %w", err)
	}

	return &S3{client: client, bucket: cfg.Bucket, prefix: strings.Trim(cfg.Prefix, "/")}, nil
}

func (s *S3) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), r, size, minio.PutObjectOptions{})
	return err
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy: it returns without contacting the server, so a missing key surfaces
	// on the first Read rather than here. Stat now, so a caller can distinguish "not found"
	// from a truncated read.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		return nil, err
	}
	return obj, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object

	for o := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix: s.key(prefix), Recursive: true,
	}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, Object{
			// Reported without the deployment prefix, so callers deal only in engine-relative
			// keys and the prefix stays an implementation detail of this type.
			Key:      strings.TrimPrefix(strings.TrimPrefix(o.Key, s.prefix), "/"),
			Size:     o.Size,
			Modified: o.LastModified,
		})
	}
	return out, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, s.key(key), minio.RemoveObjectOptions{})
}
