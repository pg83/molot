package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeStoreObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
	heads   map[string]int
	gets    int
	puts    int
	err     error
}

func (f *fakeStoreObjects) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++

	if f.err != nil {
		return nil, f.err
	}

	data, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]

	if !ok {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(int64(len(data)))}, nil
}

func (f *fakeStoreObjects) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	f.heads[key]++

	if f.err != nil {
		return nil, f.err
	}

	data, ok := f.objects[key]

	if !ok {
		return nil, &types.NotFound{}
	}

	return &s3.HeadObjectOutput{ETag: aws.String(fmt.Sprintf("\"%x\"", md5.Sum(data)))}, nil
}

func (f *fakeStoreObjects) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++

	if f.err != nil {
		return nil, f.err
	}

	data, err := io.ReadAll(in.Body)

	if err != nil {
		return nil, err
	}

	if in.ContentLength == nil || *in.ContentLength != int64(len(data)) {
		return nil, errors.New("incorrect upload length")
	}

	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = data

	return &s3.PutObjectOutput{}, nil
}

func newTestStore(t *testing.T) (*storeSrv, *fakeStoreObjects) {
	t.Helper()
	fake := &fakeStoreObjects{objects: make(map[string][]byte), heads: make(map[string]int)}
	base := newTestCacheSrv(fake, filepath.Join(t.TempDir(), "complete"))
	base.kv = testBlobKVMiss(t)
	base.setIndex(nil)

	return &storeSrv{cacheSrv: base, storage: fake}, fake
}

func storeRequest(s *storeSrv, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))

	return w
}

func TestStoreGETFallsThroughKVFailuresWithoutIndex(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError, http.StatusNoContent, http.StatusTemporaryRedirect} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv, fake := newTestStore(t)
			fake.objects["molot/molot/one/result.zstd"] = []byte("archive")
			var gets, puts atomic.Int32
			srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					gets.Add(1)
				} else {
					puts.Add(1)
				}

				w.WriteHeader(status)
			})
			res := storeRequest(srv, http.MethodGet, "/v1/blob/one", "")

			if res.Code != http.StatusOK || res.Body.String() != "archive" || fake.gets != 1 || gets.Load() != 1 || puts.Load() != 1 {
				t.Fatalf("status=%d body=%q S3 GETs=%d KV GETs=%d PUTs=%d", res.Code, res.Body, fake.gets, gets.Load(), puts.Load())
			}
		})
	}

	srv, fake := newTestStore(t)
	fake.objects["molot/molot/one/result.zstd"] = []byte("archive")
	srv.kv = newBlobKV("http://127.0.0.1:1", "molot", time.Second)
	res := storeRequest(srv, http.MethodGet, "/v1/blob/one", "")

	if res.Code != http.StatusOK || res.Body.String() != "archive" {
		t.Fatalf("transport failure: status=%d body=%q", res.Code, res.Body)
	}
}

func TestStoreGETWarmsKVThenReadsIt(t *testing.T) {
	srv, fake := newTestStore(t)
	fake.objects["molot/molot/one/result.zstd"] = []byte("archive")
	var mu sync.Mutex
	var data []byte
	srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.Method == http.MethodPut {
			data = Throw2(io.ReadAll(r.Body))
			w.WriteHeader(http.StatusNoContent)
		} else if data == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(data)
		}
	})

	for range 2 {
		res := storeRequest(srv, http.MethodGet, "/v1/blob/one", "")

		if res.Code != http.StatusOK || res.Body.String() != "archive" {
			t.Fatalf("status=%d body=%q", res.Code, res.Body)
		}
	}

	if fake.gets != 1 || len(fake.heads) != 0 {
		t.Fatalf("S3 GETs=%d HEADs=%v", fake.gets, fake.heads)
	}
}

func TestStorePUTDoesNotTouchKV(t *testing.T) {
	srv, fake := newTestStore(t)
	var calls atomic.Int32
	srv.kv = testBlobKV(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	// Unknown request length is accepted; the buffered body supplies the S3 length.
	req := httptest.NewRequest(http.MethodPut, "/v1/blob/one+two&three", io.NopCloser(strings.NewReader("archive")))
	res := httptest.NewRecorder()
	srv.handleBlob(res, req)

	if res.Code != http.StatusNoContent || string(fake.objects["molot/molot/one+two&three/result.zstd"]) != "archive" || calls.Load() != 0 {
		t.Fatalf("status=%d objects=%v KV calls=%d", res.Code, fake.objects, calls.Load())
	}

	fake.err = errors.New("S3 unavailable")
	res = storeRequest(srv, http.MethodPut, "/v1/blob/two", "archive")

	if res.Code != http.StatusInternalServerError || calls.Load() != 0 {
		t.Fatalf("failed upload: status=%d KV calls=%d", res.Code, calls.Load())
	}
}

func TestStorePUTSDKRetriesBufferedBody(t *testing.T) {
	var attempts atomic.Int32
	data := bytes.Repeat([]byte("archive"), 32768)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)

		if err != nil || !bytes.Equal(body, data) || r.URL.Path != "/molot/molot/one/result.zstd" {
			t.Errorf("upload path=%s size=%d error=%v", r.URL.Path, len(body), err)
		}

		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `<Error><Code>SlowDown</Code><Message>retry</Message></Error>`)

			return
		}

		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
	}))
	defer backend.Close()
	srv, _ := newTestStore(t)
	srv.storage = newS3Client(&Config{S3Endpt: backend.URL, AWSRegion: "us-east-1", AWSKey: "test", AWSSecret: "test"})
	srv.kv = nil // PUT must not use KV, even after a retry.
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "must-not-create-files"))
	res := httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodPut, "/v1/blob/one", bytes.NewReader(data)))

	if res.Code != http.StatusNoContent || attempts.Load() != 2 {
		t.Fatalf("status=%d attempts=%d body=%s", res.Code, attempts.Load(), res.Body)
	}
}

type brokenUpload struct{}

func (brokenUpload) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestStorePUTReadFailureDoesNotWriteS3(t *testing.T) {
	srv, fake := newTestStore(t)
	res := httptest.NewRecorder()
	srv.handleBlob(res, httptest.NewRequest(http.MethodPut, "/v1/blob/one", brokenUpload{}))

	if res.Code != http.StatusInternalServerError || fake.puts != 0 {
		t.Fatalf("status=%d S3 PUTs=%d", res.Code, fake.puts)
	}
}

func TestStoreResolveIsAuthoritativeAndCachesOnlyPositiveResults(t *testing.T) {
	srv, fake := newTestStore(t)
	srv.kv = nil // Resolve never uses the byte cache.
	srv.setIndex([]byte("indexed\n"))
	fake.objects["molot/molot/fresh/result.zstd"] = []byte("archive")
	digest := fmt.Sprintf("%x", md5.Sum([]byte("archive")))
	request := `["indexed","fresh","missing","fresh"]`

	for _, version := range []string{"v1", "v2"} {
		res := storeRequest(srv, http.MethodPost, "/"+version+"/resolve", request)

		if res.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", res.Code, res.Body)
		}

		if version == "v1" {
			var result []string
			Throw(json.Unmarshal(res.Body.Bytes(), &result))

			if !reflect.DeepEqual(result, []string{"indexed", "fresh", "fresh"}) {
				t.Fatalf("v1 result=%v", result)
			}
		} else {
			var result map[string]string
			Throw(json.Unmarshal(res.Body.Bytes(), &result))

			if !reflect.DeepEqual(result, map[string]string{"indexed": "", "fresh": digest}) {
				t.Fatalf("v2 result=%v", result)
			}
		}
	}

	wantHeads := map[string]int{"molot/molot/fresh/result.zstd": 1, "molot/molot/missing/result.zstd": 2}

	if !reflect.DeepEqual(fake.heads, wantHeads) || fake.gets != 0 || len(srv.stats.pending) != 2 {
		t.Fatalf("HEADs=%v GETs=%d stats batches=%d", fake.heads, fake.gets, len(srv.stats.pending))
	}

	fake.objects["molot/molot/missing/result.zstd"] = []byte("new")
	res := storeRequest(srv, http.MethodPost, "/v1/resolve", `["missing"]`)

	if strings.TrimSpace(res.Body.String()) != `["missing"]` {
		t.Fatalf("negative result was cached: %s", res.Body)
	}
}

func TestStoreRefreshClearsEntirePositiveCache(t *testing.T) {
	srv, fake := newTestStore(t)
	fake.objects["molot/molot/fresh/result.zstd"] = []byte("archive")
	storeRequest(srv, http.MethodPost, "/v1/resolve", `["fresh"]`)
	fake.err = errors.New("index download failed")

	if exc := Try(func() { srv.refreshIndex(context.Background()) }); exc == nil {
		t.Fatal("index refresh must fail")
	}

	res := storeRequest(srv, http.MethodPost, "/v1/resolve", `["fresh"]`)

	if strings.TrimSpace(res.Body.String()) != `["fresh"]` {
		t.Fatal("failed refresh cleared the cache")
	}

	fake.err = nil
	fake.objects["cix/complete"] = []byte("indexed\n")
	delete(fake.objects, "molot/molot/fresh/result.zstd")
	srv.refreshIndex(context.Background())
	res = storeRequest(srv, http.MethodPost, "/v1/resolve", `["fresh","indexed"]`)

	if strings.TrimSpace(res.Body.String()) != `["indexed"]` || len(srv.known) != 0 || fake.heads["molot/molot/fresh/result.zstd"] != 2 {
		t.Fatalf("resolve=%s known=%v HEADs=%v", res.Body, srv.known, fake.heads)
	}
}

func TestStoreResolveErrorsDoNotBecomeMisses(t *testing.T) {
	srv, fake := newTestStore(t)
	fake.err = errors.New("S3 unavailable")

	for _, path := range []string{"/v1/resolve", "/v2/resolve"} {
		res := storeRequest(srv, http.MethodPost, path, `["unknown"]`)

		if res.Code != http.StatusInternalServerError || len(srv.known) != 0 {
			t.Fatalf("path=%s status=%d known=%v", path, res.Code, srv.known)
		}
	}
}

func TestStoreResolveETagAttestation(t *testing.T) {
	for etag, want := range map[string]string{
		`"D41D8CD98F00B204E9800998ECF8427E"`:   "d41d8cd98f00b204e9800998ecf8427e",
		"d41d8cd98f00b204e9800998ecf8427e":     "d41d8cd98f00b204e9800998ecf8427e",
		`"d41d8cd98f00b204e9800998ecf8427e-2"`: "",
		"not a digest":                         "",
		strings.Repeat("z", 32):                "",
		"":                                     "",
	} {
		if got := etagMD5(etag); got != want {
			t.Fatalf("etag=%q digest=%q, want %q", etag, got, want)
		}
	}
}

func TestStoreRequiresExplicitSettings(t *testing.T) {
	for _, name := range []string{"INDEX_BUCKET", "INDEX_KEY", "KV_ENDPOINT", "KV_BUCKET", "KV_TIMEOUT"} {
		t.Setenv("MOLOT_STORE_"+name, "")
	}

	settings := []string{"--listen", "127.0.0.1:0", "--index-bucket", "molot", "--index-key", "complete",
		"--index-ttl", "30s", "--kv-endpoint", "http://127.0.0.1:1", "--kv-bucket", "molot", "--kv-timeout", "1s"}

	for omitted := 0; omitted < len(settings); omitted += 2 {
		args := append([]string{}, settings[:omitted]...)
		args = append(args, settings[omitted+2:]...)
		exc := Try(func() { storeMain(args) })

		if exc == nil || !strings.Contains(exc.Error(), settings[omitted]) {
			t.Fatalf("missing %s: exception=%v", settings[omitted], exc)
		}
	}
}
