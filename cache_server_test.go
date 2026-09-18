package main

import (
	"bytes"
	"context"
	"encoding/json"
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
		stats:       newStatsQueue(),
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

func TestCacheResolveV2ReturnsHashesFromHashedIndex(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"cix/complete": []byte("one d41d8cd98f00b204e9800998ecf8427e\nthree\n"),
	}}
	srv := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))
	srv.refreshIndex(context.Background())

	req := httptest.NewRequest(http.MethodPost, "/v2/resolve", strings.NewReader(`["one","two","three"]`))
	res := httptest.NewRecorder()
	srv.handleResolveV2(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}

	var got map[string]string

	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"one": "d41d8cd98f00b204e9800998ecf8427e", "three": ""}

	if len(got) != len(want) || got["one"] != want["one"] || got["three"] != want["three"] {
		t.Fatalf("resolve v2=%v", got)
	}

	// v1 must keep serving the plain list off the same hashed index.
	res = httptest.NewRecorder()
	srv.handleResolve(res, httptest.NewRequest(http.MethodPost, "/v1/resolve", strings.NewReader(`["one","three"]`)))

	if got := strings.TrimSpace(res.Body.String()); got != `["one","three"]` {
		t.Fatalf("resolve v1=%s", got)
	}
}

func TestCacheBlobStreamsObjectAndDistinguishesNotFound(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"molot/molot/one/result.zstd": []byte("blob"),
	}}
	srv := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))
	srv.setIndex([]byte("one\nmissing\n"))

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

func TestCacheBlobRequiresCurrentIndex(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"molot/molot/one/result.zstd": []byte("blob"),
	}}
	srv := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))

	for _, tc := range []struct {
		name   string
		index  string
		status int
		gets   int
	}{
		{"empty index", "", http.StatusNotFound, 0},
		{"unlisted object exists in S3", "other\n", http.StatusNotFound, 0},
		{"added to index", "one\n", http.StatusOK, 1},
		{"removed from index", "other\n", http.StatusNotFound, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv.setIndex([]byte(tc.index))
			res := httptest.NewRecorder()
			srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

			if res.Code != tc.status {
				t.Fatalf("status=%d, want %d", res.Code, tc.status)
			}

			if fake.gets != tc.gets {
				t.Fatalf("S3 GETs=%d, want %d", fake.gets, tc.gets)
			}
		})
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
