package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func testBlobKV(t *testing.T, handler http.HandlerFunc) *blobKV {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return newBlobKV(server.URL, "molot", time.Second)
}

func testBlobKVMiss(t *testing.T) *blobKV {
	t.Helper()

	return testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func TestCacheBlobKVWarmsThenServesWithoutIndex(t *testing.T) {
	var mu sync.Mutex
	var value []byte
	var calls []string
	kv := testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		calls = append(calls, r.Method)

		if r.URL.Query().Get("key") != "one+two&three" {
			t.Errorf("key=%q", r.URL.Query().Get("key"))
		}

		switch r.Method + " " + r.URL.Path {
		case "GET /v1/molot/get":
			if value == nil {
				w.WriteHeader(http.StatusNotFound)

				return
			}

			w.Write(value)
		case "PUT /v1/molot/put":
			var err error
			value, err = io.ReadAll(r.Body)

			if err != nil {
				t.Error(err)
			}

			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	fake := &fakeObjectGetter{objects: map[string][]byte{
		"molot/molot/one+two&three/result.zstd": []byte("artifact"),
	}}
	srv := newTestCacheSrv(fake, "")
	srv.kv = kv
	srv.setIndex([]byte("one+two&three\n"))

	for range 2 {
		res := httptest.NewRecorder()
		srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one+two&three", nil))

		if res.Code != http.StatusOK || res.Body.String() != "artifact" {
			t.Fatalf("status=%d body=%q", res.Code, res.Body.String())
		}

		if res.Header().Get("Content-Type") != "application/zstd" || res.Header().Get("Content-Length") != "8" {
			t.Fatalf("headers=%v", res.Header())
		}

		srv.setIndex(nil)
	}

	mu.Lock()
	defer mu.Unlock()

	if fake.gets != 1 || strings.Join(calls, ",") != "GET,PUT,GET" || string(value) != "artifact" {
		t.Fatalf("S3 GETs=%d KV calls=%v value=%q", fake.gets, calls, value)
	}
}

func TestCacheBlobKVEmptyValueIsAHit(t *testing.T) {
	srv := newTestCacheSrv(&fakeObjectGetter{}, "")
	srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	res := httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

	if res.Code != http.StatusOK || res.Body.Len() != 0 || res.Header().Get("Content-Length") != "0" {
		t.Fatalf("status=%d body=%q headers=%v", res.Code, res.Body.String(), res.Header())
	}
}

func TestCacheBlobKVFailuresNeverReachS3(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kvStatus int
		indexed  bool
		status   int
	}{
		{"miss outside index", http.StatusNotFound, false, http.StatusNotFound},
		{"unavailable", http.StatusServiceUnavailable, true, http.StatusInternalServerError},
		{"failure outside index", http.StatusInternalServerError, false, http.StatusInternalServerError},
		{"unexpected success", http.StatusNoContent, true, http.StatusInternalServerError},
		{"redirect", http.StatusFound, true, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			fake := &fakeObjectGetter{objects: map[string][]byte{"molot/molot/one/result.zstd": []byte("artifact")}}
			srv := newTestCacheSrv(fake, "")
			srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/missing")
				w.WriteHeader(tc.kvStatus)
			})

			if tc.indexed {
				srv.setIndex([]byte("one\n"))
			}

			res := httptest.NewRecorder()
			srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

			if res.Code != tc.status || fake.gets != 0 || calls.Load() != 1 {
				t.Fatalf("status=%d S3 GETs=%d KV calls=%d", res.Code, fake.gets, calls.Load())
			}
		})
	}
}

func TestCacheBlobKVTransportFailuresNeverReachS3(t *testing.T) {
	for _, kind := range []string{"connection", "timeout", "truncated", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "timeout":
					<-r.Context().Done()
				case "truncated":
					w.Header().Set("Content-Length", "100")
					io.WriteString(w, "partial")
				case "oversized":
					w.Header().Set("Content-Length", strconv.Itoa(maxBlobKVSize+1))
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer server.Close()

			if kind == "connection" {
				server.Close()
			}

			fake := &fakeObjectGetter{}
			srv := newTestCacheSrv(fake, "")
			srv.kv = newBlobKV(server.URL, "molot", 20*time.Millisecond)
			srv.setIndex([]byte("one\n"))
			res := httptest.NewRecorder()
			srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

			if res.Code != http.StatusInternalServerError || fake.gets != 0 {
				t.Fatalf("status=%d S3 GETs=%d body=%q", res.Code, fake.gets, res.Body.String())
			}
		})
	}
}

func TestCacheBlobKVWriteFailureStillReturnsArtifact(t *testing.T) {
	for _, status := range []int{http.StatusRequestEntityTooLarge, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var puts atomic.Int32
			fake := &fakeObjectGetter{objects: map[string][]byte{"molot/molot/one/result.zstd": []byte("artifact")}}
			srv := newTestCacheSrv(fake, "")
			srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.WriteHeader(http.StatusNotFound)

					return
				}

				puts.Add(1)
				io.Copy(io.Discard, r.Body)
				w.WriteHeader(status)
			})
			srv.setIndex([]byte("one\n"))
			res := httptest.NewRecorder()
			srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

			if res.Code != http.StatusOK || res.Body.String() != "artifact" || fake.gets != 1 || puts.Load() != 1 {
				t.Fatalf("status=%d body=%q S3 GETs=%d KV PUTs=%d", res.Code, res.Body.String(), fake.gets, puts.Load())
			}
		})
	}
}

func TestCacheResolveDoesNotConsultKV(t *testing.T) {
	srv := newTestCacheSrv(&fakeObjectGetter{}, "")
	srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("resolve contacted KV")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv.setIndex([]byte("indexed hash\n"))

	for version, handle := range map[string]http.HandlerFunc{"v1": srv.handleResolve, "v2": srv.handleResolveV2} {
		res := httptest.NewRecorder()
		handle(res, httptest.NewRequest(http.MethodPost, "/"+version+"/resolve", strings.NewReader(`["indexed","unlisted"]`)))

		if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "indexed") || strings.Contains(res.Body.String(), "unlisted") {
			t.Fatalf("%s status=%d body=%q", version, res.Code, res.Body.String())
		}
	}
}

type objectGetterFunc func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)

func (f objectGetterFunc) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f(ctx, in, opts...)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)

	return len(p), nil
}

type countedResponse struct {
	header http.Header
	hash   hash.Hash
	size   int64
}

func (w *countedResponse) Header() http.Header {
	return w.header
}

func (w *countedResponse) WriteHeader(int) {}

func (w *countedResponse) Write(p []byte) (int, error) {
	w.size += int64(len(p))

	return w.hash.Write(p)
}

func TestCacheBlobKVSizeLimit(t *testing.T) {
	for _, knownSize := range []bool{true, false} {
		for _, size := range []int64{maxBlobKVSize, maxBlobKVSize + 1} {
			t.Run(strconv.FormatBool(knownSize)+"/"+strconv.FormatInt(size, 10), func(t *testing.T) {
				var putSize atomic.Int64
				fake := objectGetterFunc(func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					resp := &s3.GetObjectOutput{Body: io.NopCloser(io.LimitReader(zeroReader{}, size))}

					if knownSize {
						resp.ContentLength = aws.Int64(size)
					}

					return resp, nil
				})
				srv := newTestCacheSrv(fake, "")
				srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet {
						w.WriteHeader(http.StatusNotFound)

						return
					}

					n, err := io.Copy(io.Discard, r.Body)

					if err != nil {
						t.Error(err)
					}

					putSize.Add(n)
					w.WriteHeader(http.StatusNoContent)
				})
				srv.kv.client.Timeout = 30 * time.Second
				srv.setIndex([]byte("one\n"))
				res := &countedResponse{header: make(http.Header), hash: md5.New()}
				srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))
				want := md5.New()
				io.Copy(want, io.LimitReader(zeroReader{}, size))

				if res.size != size || !bytes.Equal(res.hash.Sum(nil), want.Sum(nil)) {
					t.Fatalf("response size=%d, want %d; hash matches=%v", res.size, size, bytes.Equal(res.hash.Sum(nil), want.Sum(nil)))
				}

				wantPutSize := int64(0)

				if size <= maxBlobKVSize {
					wantPutSize = size
				}

				if putSize.Load() != wantPutSize {
					t.Fatalf("KV PUT bytes=%d, want %d", putSize.Load(), wantPutSize)
				}
			})
		}
	}
}

func TestCacheBlobDoesNotCacheTruncatedS3Object(t *testing.T) {
	var puts atomic.Int32
	fake := objectGetterFunc(func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("partial")), ContentLength: aws.Int64(100)}, nil
	})
	srv := newTestCacheSrv(fake, "")
	srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}

		w.WriteHeader(http.StatusNotFound)
	})
	srv.setIndex([]byte("one\n"))
	res := httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodGet, "/v1/blob/one", nil))

	if res.Code != http.StatusInternalServerError || puts.Load() != 0 {
		t.Fatalf("status=%d KV PUTs=%d", res.Code, puts.Load())
	}
}

func TestBlobKVConfiguration(t *testing.T) {
	for _, tc := range []struct{ endpoint, bucket string }{
		{"", "molot"},
		{"http://%", "molot"},
		{"ftp://localhost", "molot"},
		{"http://", "molot"},
		{"http://localhost?query", "molot"},
		{"http://localhost#fragment", "molot"},
		{"http://localhost", ""},
		{"http://localhost", "a/b"},
	} {
		if exc := Try(func() {
			newBlobKV(tc.endpoint, tc.bucket, time.Second)
		}); exc == nil {
			t.Fatalf("invalid configuration accepted: %+v", tc)
		}
	}

	for _, timeout := range []time.Duration{0, -time.Second} {
		if exc := Try(func() {
			newBlobKV("http://localhost", "molot", timeout)
		}); exc == nil {
			t.Fatalf("invalid timeout %s accepted", timeout)
		}
	}

	kv := newBlobKV("http://localhost/", "custom bucket", 7*time.Second)

	if kv.baseURL != "http://localhost/v1/custom%20bucket" || kv.client.Timeout != 7*time.Second {
		t.Fatalf("base URL=%q timeout=%s", kv.baseURL, kv.client.Timeout)
	}
}

func TestCacheRequiresExplicitKVSettings(t *testing.T) {
	settings := []struct{ flag, env, value string }{
		{"kv-endpoint", "MOLOT_CACHE_KV_ENDPOINT", "http://127.0.0.1:1"},
		{"kv-bucket", "MOLOT_CACHE_KV_BUCKET", "test-bucket"},
		{"kv-timeout", "MOLOT_CACHE_KV_TIMEOUT", "2s"},
	}

	for _, missing := range settings {
		t.Run(missing.flag+"/unset", func(t *testing.T) {
			args := []string{"--listen", "127.0.0.1:0"}

			for _, setting := range settings {
				t.Setenv(setting.env, "")

				if setting.flag != missing.flag {
					args = append(args, "--"+setting.flag, setting.value)
				}
			}

			exc := Try(func() {
				cacheMain(args)
			})

			if exc == nil || !strings.Contains(exc.Error(), "--"+missing.flag) || !strings.Contains(exc.Error(), "required") {
				t.Fatalf("missing setting did not fail configuration: %v", exc)
			}
		})

		t.Run(missing.flag+"/explicitly empty", func(t *testing.T) {
			for _, setting := range settings {
				t.Setenv(setting.env, setting.value)
			}

			exc := Try(func() {
				cacheMain([]string{"--listen", "127.0.0.1:0", "--" + missing.flag, ""})
			})

			if exc == nil || !strings.Contains(exc.Error(), "--"+missing.flag) || !strings.Contains(exc.Error(), "required") {
				t.Fatalf("empty setting fell back to environment: %v", exc)
			}
		})
	}
}

func TestBlobKVGetPreservesCancellation(t *testing.T) {
	kv := newBlobKV("http://127.0.0.1:1", "molot", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exc := Try(func() {
		kv.get(ctx, "one")
	})

	if !errors.Is(exc.AsError(), context.Canceled) {
		t.Fatalf("exception=%v, want context cancellation", exc)
	}
}
