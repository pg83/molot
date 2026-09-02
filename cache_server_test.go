package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

func newTestCacheSrv(s3cli objectGetter) *cacheSrv {
	return &cacheSrv{
		s3:          s3cli,
		blobBucket:  "molot",
		s3Root:      "molot",
		indexBucket: "cix",
		indexKey:    "complete",
		indexTTL:    time.Minute,
	}
}

func TestCacheResolveUsesBatchIndexAndMemoryCache(t *testing.T) {
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"cix/complete": []byte("one\nthree\n"),
	}}
	srv := newTestCacheSrv(fake)
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
	srv := newTestCacheSrv(fake)

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
