package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type storeObjects interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type storeSrv struct {
	*cacheSrv
	storage storeObjects
}

func storeMain(args []string) {
	artifactServerMain("store", args)
}

func (s *storeSrv) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/resolve", s.handleResolve)
	mux.HandleFunc("/v2/resolve", s.handleResolveV2)
	mux.HandleFunc("/v1/blob/", s.handleBlob)

	return mux
}

func (s *storeSrv) handleResolve(w http.ResponseWriter, r *http.Request) {
	s.serveResolve(w, r, func(w http.ResponseWriter, requested []string, index map[string]string) {
		writeResolveV1(w, requested, s.resolve(r.Context(), requested, index))
	})
}

func (s *storeSrv) handleResolveV2(w http.ResponseWriter, r *http.Request) {
	s.serveResolve(w, r, func(w http.ResponseWriter, requested []string, index map[string]string) {
		writeResolveV2(w, requested, s.resolve(r.Context(), requested, index))
	})
}

func (s *storeSrv) resolve(ctx context.Context, requested []string, index map[string]string) map[string]string {
	available := make(map[string]string, len(requested))
	checked := make(map[string]bool, len(requested))

	for _, uid := range requested {
		if !validCacheUID(uid) {
			ThrowHTTP(http.StatusBadRequest, "bad uid")
		}
	}

	for _, uid := range requested {
		if checked[uid] {
			continue
		}

		checked[uid] = true

		if digest, ok := index[uid]; ok {
			available[uid] = digest

			continue
		}

		s.mu.RLock()
		digest, known := s.known[uid]
		s.mu.RUnlock()

		if known {
			available[uid] = digest

			continue
		}

		object, err := s.storage.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.blobBucket),
			Key:    aws.String(fmt.Sprintf("%s/%s/result.zstd", s.s3Root, uid)),
		})

		if isNotFound(err) {
			continue
		}

		Throw(err)
		digest = etagMD5(aws.ToString(object.ETag))
		available[uid] = digest

		s.mu.Lock()
		s.known[uid] = digest
		s.mu.Unlock()
	}

	return available
}

// Multipart ETags do not attest to the MD5 of the downloaded archive.
func etagMD5(etag string) string {
	digest := strings.Trim(etag, "\"")

	if len(digest) != 32 {
		return ""
	}

	if _, err := hex.DecodeString(digest); err != nil {
		return ""
	}

	return strings.ToLower(digest)
}

func (s *storeSrv) handleBlob(w http.ResponseWriter, r *http.Request) {
	Try(func() {
		if r.Method != http.MethodGet && r.Method != http.MethodPut {
			w.Header().Set("Allow", "GET, PUT")
			ThrowHTTP(http.StatusMethodNotAllowed, "method not allowed")
		}

		uid := strings.TrimPrefix(r.URL.Path, "/v1/blob/")

		if !validCacheUID(uid) {
			ThrowHTTP(http.StatusBadRequest, "bad uid")
		}

		if r.Method == http.MethodPut {
			s.putBlob(r, uid)
			w.WriteHeader(http.StatusNoContent)

			return
		}

		var data []byte

		Try(func() {
			data = s.kv.get(r.Context(), uid)
		}).Catch(func(exc *Exception) {
			fmt.Fprintln(os.Stderr, "molot store: KV GET failed:", exc)
		})

		if data != nil {
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			Throw2(w.Write(data))

			return
		}

		s.serveS3Blob(w, r, uid)
	}).Catch(func(exc *Exception) {
		sendCacheException(w, r, exc)
	})
}

func (s *storeSrv) putBlob(r *http.Request, uid string) {
	data := Throw2(io.ReadAll(r.Body))
	Throw2(s.storage.PutObject(r.Context(), &s3.PutObjectInput{
		Bucket:        aws.String(s.blobBucket),
		Key:           aws.String(fmt.Sprintf("%s/%s/result.zstd", s.s3Root, uid)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/zstd"),
	}))
}
