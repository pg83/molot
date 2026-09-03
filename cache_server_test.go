package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeObjectGetter struct {
	objects map[string][]byte
	gets    int
}

func (f *fakeObjectGetter) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets++
	data, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]

	if !ok {
		return nil, &types.NoSuchKey{}
	}

	n := int64(len(data))

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: &n,
	}, nil
}

func newTestCacheSrv(s3cli objectGetter, indexPath string) *cacheSrv {
	return &cacheSrv{
		s3:          s3cli,
		blobBucket:  "molot",
		s3Root:      "molot",
		indexBucket: "cix",
		indexKey:    "complete",
		indexTTL:    time.Minute,
		indexPath:   indexPath,
	}
}

func TestCacheResolveUsesBatchIndexAndMemoryCache(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"cix/complete": []byte("one\nthree\n"),
	}}
	srv := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))
	srv.refreshIndex(context.Background())

	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/v1/resolve", strings.NewReader(`["one","two","three"]`))
		res := httptest.NewRecorder()
		srv.handleResolve(res, req)

		if res.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
		}

		if got := strings.TrimSpace(res.Body.String()); got != `["one","three"]` {
			t.Fatalf("resolve=%s", got)
		}
	}

	if fake.gets != 1 {
		t.Fatalf("index GETs=%d, want 1", fake.gets)
	}
}

func TestCacheBlobStreamsObjectAndDistinguishesNotFound(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"molot/molot/one/result.zstd": []byte("blob"),
	}}
	srv := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))

	res := httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

	if res.Code != http.StatusOK || res.Body.String() != "blob" {
		t.Fatalf("status=%d body=%q", res.Code, res.Body.String())
	}

	res = httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/missing", nil))

	if res.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%q", res.Code, res.Body.String())
	}
}

func TestCacheInitialIndexUsesLocalSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "complete")

	if err := os.WriteFile(path, []byte("local\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	fake := &fakeObjectGetter{objects: map[string][]byte{
		"cix/complete": []byte("remote\n"),
	}}
	srv := newTestCacheSrv(fake, path)
	srv.initializeIndex(context.Background())

	if _, ok := srv.indexSnapshot()["local"]; !ok {
		t.Fatal("local uid missing from initial index")
	}

	if fake.gets != 0 {
		t.Fatalf("index GETs=%d, want 0", fake.gets)
	}

	srv.refreshIndex(context.Background())

	if _, ok := srv.indexSnapshot()["remote"]; !ok {
		t.Fatal("remote uid missing after refresh")
	}

	data, err := os.ReadFile(path)

	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "remote\n" {
		t.Fatalf("snapshot=%q, want %q", data, "remote\\n")
	}
}

func TestCacheInitialIndexFallsBackToS3AndSavesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "complete")
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"cix/complete": []byte("remote\n"),
	}}
	srv := newTestCacheSrv(fake, path)
	srv.initializeIndex(context.Background())

	if _, ok := srv.indexSnapshot()["remote"]; !ok {
		t.Fatal("remote uid missing from initial index")
	}

	data, err := os.ReadFile(path)

	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "remote\n" {
		t.Fatalf("snapshot=%q, want %q", data, "remote\\n")
	}
}
