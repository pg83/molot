package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const maxResolveBody = 64 << 20

type objectGetter interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type cacheSrv struct {
	s3          objectGetter
	kv          *blobKV
	blobBucket  string
	s3Root      string
	indexBucket string
	indexKey    string
	indexTTL    time.Duration
	indexPath   string

	mu    sync.RWMutex
	index map[string]string
	known map[string]string // store's positive HEAD results, cleared with the index

	stats *statsQueue
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func cacheMain(args []string) {
	artifactServerMain("cache", args)
}

func artifactServerMain(command string, args []string) {
	fs := flag.NewFlagSet("molot "+command, flag.ContinueOnError)
	prefix := "MOLOT_" + strings.ToUpper(command) + "_"
	indexBucketValue := os.Getenv(prefix + "INDEX_BUCKET")
	indexKeyValue := os.Getenv(prefix + "INDEX_KEY")
	var indexInterval time.Duration

	if command == "cache" {
		indexBucketValue = envDefault(prefix+"INDEX_BUCKET", "cix")
		indexKeyValue = envDefault(prefix+"INDEX_KEY", "complete")
		indexInterval = 30 * time.Second
	}

	listen := fs.String("listen", "", "HTTP listen address, e.g. 0.0.0.0:8054")
	indexBucket := fs.String("index-bucket", indexBucketValue, "S3 bucket containing the uid index")
	indexKey := fs.String("index-key", indexKeyValue, "S3 object containing one uid per line")
	indexTTL := fs.Duration("index-ttl", indexInterval, "positive background uid index refresh interval (required for store)")
	kvEndpoint := fs.String("kv-endpoint", os.Getenv(prefix+"KV_ENDPOINT"), "required KV front URL")
	kvBucket := fs.String("kv-bucket", os.Getenv(prefix+"KV_BUCKET"), "required KV bucket for artifact bytes")
	kvTimeout := fs.String("kv-timeout", os.Getenv(prefix+"KV_TIMEOUT"), "required positive KV request timeout, as a Go duration")

	Throw(fs.Parse(args))

	if *listen == "" {
		ThrowFmt("%s: --listen is required", command)
	}

	if *indexBucket == "" || *indexKey == "" {
		ThrowFmt("%s: --index-bucket and --index-key must not be empty", command)
	}

	if *indexTTL <= 0 {
		ThrowFmt("%s: --index-ttl must be positive", command)
	}

	if *kvEndpoint == "" || *kvBucket == "" || *kvTimeout == "" {
		ThrowFmt("%s: --kv-endpoint, --kv-bucket and --kv-timeout are required", command)
	}

	kv := newBlobKV(*kvEndpoint, *kvBucket, Throw2(time.ParseDuration(*kvTimeout)))
	cfg := loadS3Config()
	srv := &cacheSrv{
		s3:          cfg.S3Cli,
		kv:          kv,
		blobBucket:  cfg.S3Bucket,
		s3Root:      cfg.S3Root,
		indexBucket: *indexBucket,
		indexKey:    *indexKey,
		indexTTL:    *indexTTL,
		indexPath:   filepath.Join(os.TempDir(), "complete"),
		stats:       newStatsQueue(),
	}
	host := Throw2(os.Hostname())
	go srv.statsLoop(cfg.S3Cli, cfg.S3Bucket, host)
	refreshCtx, stopRefresh := context.WithCancel(context.Background())
	defer stopRefresh()

	srv.initializeIndex(refreshCtx)
	go srv.refreshLoop(refreshCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/resolve", srv.handleResolve)
	mux.HandleFunc("/v2/resolve", srv.handleResolveV2)
	mux.HandleFunc("/v1/blob/", srv.handleBlob)

	if command == "store" {
		mux = (&storeSrv{cacheSrv: srv, storage: cfg.S3Cli}).routes()
	}

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigs
		fmt.Fprintf(os.Stderr, "molot %s: signal: %v\n", command, sig)
		stopRefresh()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		Try(func() {
			Throw(server.Shutdown(ctx))
		}).Catch(func(exc *Exception) {
			fmt.Fprintf(os.Stderr, "molot %s: shutdown: %v\n", command, exc)
		})
	}()

	fmt.Fprintf(os.Stderr, "molot %s: listening on %s index=s3://%s/%s blobs=s3://%s/%s/<uid>/result.zstd\n",
		command, *listen, *indexBucket, *indexKey, cfg.S3Bucket, cfg.S3Root)

	err := server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		Throw(err)
	}
}

func sendCacheException(w http.ResponseWriter, r *http.Request, e *Exception) {
	var he *HTTPError

	if errors.As(e.AsError(), &he) {
		httpError(w, he.Status, he.Msg)

		return
	}

	fmt.Fprintf(os.Stderr, "molot: %s %s: %s\n", r.Method, r.URL.Path, e.Error())

	httpError(w, http.StatusInternalServerError, e.Error())
}

// handleResolveV2 answers with a uid -> md5 object instead of v1's
// bare uid list, so clients can verify fetched blobs. The hash is ""
// when the index has no attestation for that uid.
func (s *cacheSrv) handleResolveV2(w http.ResponseWriter, r *http.Request) {
	s.serveResolve(w, r, writeResolveV2)
}

func writeResolveV2(w http.ResponseWriter, requested []string, index map[string]string) {
	available := make(map[string]string, len(requested))

	for _, uid := range requested {
		if md5, ok := index[uid]; ok {
			available[uid] = md5
		}
	}

	w.Header().Set("Content-Type", "application/json")
	Throw(json.NewEncoder(w).Encode(available))
}

func (s *cacheSrv) handleResolve(w http.ResponseWriter, r *http.Request) {
	s.serveResolve(w, r, writeResolveV1)
}

func writeResolveV1(w http.ResponseWriter, requested []string, index map[string]string) {
	available := make([]string, 0, len(requested))

	for _, uid := range requested {
		if _, ok := index[uid]; ok {
			available = append(available, uid)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	Throw(json.NewEncoder(w).Encode(available))
}

func (s *cacheSrv) serveResolve(w http.ResponseWriter, r *http.Request, respond func(http.ResponseWriter, []string, map[string]string)) {
	Try(func() {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			ThrowHTTP(http.StatusMethodNotAllowed, "method not allowed")
		}

		var requested []string
		dec := json.NewDecoder(io.LimitReader(r.Body, maxResolveBody))

		if err := dec.Decode(&requested); err != nil {
			ThrowHTTP(http.StatusBadRequest, "bad JSON uid list: %v", err)
		}

		s.stats.put(requested)
		respond(w, requested, s.indexSnapshot())
	}).Catch(func(exc *Exception) {
		sendCacheException(w, r, exc)
	})
}

func (s *cacheSrv) loadIndex(ctx context.Context) []byte {
	resp := Throw2(s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.indexBucket),
		Key:    aws.String(s.indexKey),
	}))
	defer resp.Body.Close()

	return Throw2(io.ReadAll(resp.Body))
}

// parseIndex reads "uid" or "uid <md5>" lines; the hash column comes
// from the complete job's recursive listing (single-part ETag == MD5)
// and is empty for entries the listing could not attest.
func parseIndex(data []byte) map[string]string {
	index := make(map[string]string, len(data)/24)

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)

		switch len(fields) {
		case 1:
			index[fields[0]] = ""
		case 2:
			index[fields[0]] = fields[1]
		}
	}

	return index
}

func writeIndex(path string, data []byte) {
	f := Throw2(os.CreateTemp(filepath.Dir(path), ".complete.*"))
	tmp := f.Name()
	defer os.Remove(tmp)

	Throw2(f.Write(data))
	Throw(f.Close())
	Throw(os.Rename(tmp, path))
}

func (s *cacheSrv) setIndex(data []byte) {
	index := parseIndex(data)

	s.mu.Lock()
	s.index = index
	s.known = make(map[string]string)
	s.mu.Unlock()
}

func (s *cacheSrv) initializeIndex(ctx context.Context) {
	data, err := os.ReadFile(s.indexPath)

	if err == nil {
		s.setIndex(data)

		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		Throw(err)
	}

	s.refreshIndex(ctx)
}

func (s *cacheSrv) refreshIndex(ctx context.Context) {
	data := s.loadIndex(ctx)
	writeIndex(s.indexPath, data)
	s.setIndex(data)
}

func (s *cacheSrv) indexSnapshot() map[string]string {
	s.mu.RLock()
	index := s.index
	s.mu.RUnlock()

	return index
}

func (s *cacheSrv) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(s.indexTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			Try(func() {
				s.refreshIndex(ctx)
			}).Catch(func(exc *Exception) {
				if ctx.Err() == nil {
					fmt.Fprintln(os.Stderr, "molot: index refresh failed, keeping previous index:", exc)
				}
			})
		}
	}
}

func validCacheUID(uid string) bool {
	return uid != "" && !strings.ContainsAny(uid, "/\\") && uid != "." && uid != ".."
}

func (s *cacheSrv) handleBlob(w http.ResponseWriter, r *http.Request) {
	Try(func() {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			ThrowHTTP(http.StatusMethodNotAllowed, "method not allowed")
		}

		uid := strings.TrimPrefix(r.URL.Path, "/v1/blob/")

		if !validCacheUID(uid) {
			ThrowHTTP(http.StatusBadRequest, "bad uid")
		}

		if data := s.kv.get(r.Context(), uid); data != nil {
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			Throw2(w.Write(data))

			return
		}

		if _, indexed := s.indexSnapshot()[uid]; !indexed {
			ThrowHTTP(http.StatusNotFound, "uid not found")
		}

		s.serveS3Blob(w, r, uid)
	}).Catch(func(exc *Exception) {
		sendCacheException(w, r, exc)
	})
}

func (s *cacheSrv) serveS3Blob(w http.ResponseWriter, r *http.Request, uid string) {
	key := fmt.Sprintf("%s/%s/result.zstd", s.s3Root, uid)
	resp, err := s.s3.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket: aws.String(s.blobBucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if isNotFound(err) {
			ThrowHTTP(http.StatusNotFound, "uid not found")
		}

		Throw(err)
	}

	defer resp.Body.Close()

	var body io.Reader = resp.Body

	if resp.ContentLength == nil || *resp.ContentLength <= maxBlobKVSize {
		data := Throw2(io.ReadAll(io.LimitReader(resp.Body, maxBlobKVSize+1)))

		if len(data) <= maxBlobKVSize {
			if resp.ContentLength != nil && int64(len(data)) != *resp.ContentLength {
				Throw(io.ErrUnexpectedEOF)
			}

			Try(func() {
				s.kv.put(r.Context(), uid, data)
			}).Catch(func(exc *Exception) {
				fmt.Fprintln(os.Stderr, "molot: KV PUT failed:", exc)
			})
		}

		body = io.MultiReader(bytes.NewReader(data), resp.Body)
	}

	w.Header().Set("Content-Type", "application/zstd")

	if resp.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*resp.ContentLength, 10))
	}

	Throw2(io.Copy(w, body))
}
