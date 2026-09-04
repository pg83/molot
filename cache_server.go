package main

import (
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
	blobBucket  string
	s3Root      string
	indexBucket string
	indexKey    string
	indexTTL    time.Duration
	indexPath   string

	mu    sync.RWMutex
	index map[string]struct{}

	stats *statsQueue
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func cacheMain(args []string) {
	fs := flag.NewFlagSet("molot cache", flag.ContinueOnError)
	listen := fs.String("listen", "", "HTTP listen address, e.g. 0.0.0.0:8054")
	indexBucket := fs.String("index-bucket", envDefault("MOLOT_CACHE_INDEX_BUCKET", "cix"), "S3 bucket containing the uid index")
	indexKey := fs.String("index-key", envDefault("MOLOT_CACHE_INDEX_KEY", "complete"), "S3 object containing one uid per line")
	indexTTL := fs.Duration("index-ttl", 30*time.Second, "background uid index refresh interval")

	Throw(fs.Parse(args))

	if *listen == "" {
		ThrowFmt("cache: --listen is required")
	}

	if *indexBucket == "" || *indexKey == "" {
		ThrowFmt("cache: --index-bucket and --index-key must not be empty")
	}

	if *indexTTL <= 0 {
		ThrowFmt("cache: --index-ttl must be positive")
	}

	cfg := loadS3Config()
	srv := &cacheSrv{
		s3:          cfg.S3Cli,
		blobBucket:  cfg.S3Bucket,
		s3Root:      cfg.S3Root,
		indexBucket: *indexBucket,
		indexKey:    *indexKey,
		indexTTL:    *indexTTL,
		indexPath:   filepath.Join(os.TempDir(), "complete"),
		stats:       newStatsQueue(),
	}
	go srv.statsLoop(cfg.S3Cli, cfg.S3Bucket)
	refreshCtx, stopRefresh := context.WithCancel(context.Background())
	defer stopRefresh()

	srv.initializeIndex(refreshCtx)
	go srv.refreshLoop(refreshCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/resolve", srv.handleResolve)
	mux.HandleFunc("/v1/blob/", srv.handleBlob)

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigs
		fmt.Fprintln(os.Stderr, "molot cache: signal:", sig)
		stopRefresh()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = server.Shutdown(ctx)
	}()

	fmt.Fprintf(os.Stderr, "molot cache: listening on %s index=s3://%s/%s blobs=s3://%s/%s/<uid>/result.zstd\n",
		*listen, *indexBucket, *indexKey, cfg.S3Bucket, cfg.S3Root)

	err := server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		Throw(err)
	}
}

func sendCacheException(w http.ResponseWriter, r *http.Request, e *Exception) {
	fmt.Fprintf(os.Stderr, "molot cache: %s %s: %s\n", r.Method, r.URL.Path, e.Error())

	httpError(w, http.StatusInternalServerError, e.Error())
}

func (s *cacheSrv) handleResolve(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
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

		index := s.indexSnapshot()
		available := make([]string, 0, len(requested))

		for _, uid := range requested {
			if _, ok := index[uid]; ok {
				available = append(available, uid)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		Throw(json.NewEncoder(w).Encode(available))
	})

	if exc == nil {
		return
	}

	var he *HTTPError

	if errors.As(exc.AsError(), &he) {
		httpError(w, he.Status, he.Msg)

		return
	}

	sendCacheException(w, r, exc)
}

func (s *cacheSrv) loadIndex(ctx context.Context) []byte {
	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.indexBucket),
		Key:    aws.String(s.indexKey),
	})

	if err != nil {
		Throw(err)
	}

	defer resp.Body.Close()

	return Throw2(io.ReadAll(resp.Body))
}

func parseIndex(data []byte) map[string]struct{} {
	index := make(map[string]struct{}, len(data)/24)

	for _, line := range strings.Split(string(data), "\n") {
		uid := strings.TrimSpace(line)

		if uid != "" {
			index[uid] = struct{}{}
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

func (s *cacheSrv) indexSnapshot() map[string]struct{} {
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
			exc := Try(func() {
				s.refreshIndex(ctx)
			})

			if exc != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "molot cache: index refresh failed, keeping previous index:", exc)
			}
		}
	}
}

func validCacheUID(uid string) bool {
	return uid != "" && !strings.ContainsAny(uid, "/\\") && uid != "." && uid != ".."
}

func (s *cacheSrv) handleBlob(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			ThrowHTTP(http.StatusMethodNotAllowed, "method not allowed")
		}

		uid := strings.TrimPrefix(r.URL.Path, "/v1/blob/")

		if !validCacheUID(uid) {
			ThrowHTTP(http.StatusBadRequest, "bad uid")
		}

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

		w.Header().Set("Content-Type", "application/zstd")

		if resp.ContentLength != nil {
			w.Header().Set("Content-Length", strconv.FormatInt(*resp.ContentLength, 10))
		}

		Throw2(io.Copy(w, resp.Body))
	})

	if exc == nil {
		return
	}

	var he *HTTPError

	if errors.As(exc.AsError(), &he) {
		httpError(w, he.Status, he.Msg)

		return
	}

	sendCacheException(w, r, exc)
}
