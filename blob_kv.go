package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxBlobKVSize = 64 << 20

type blobKV struct {
	baseURL string
	client  *http.Client
}

func newBlobKV(endpoint, bucket string, timeout time.Duration) *blobKV {
	u := Throw2(url.Parse(endpoint))

	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		ThrowFmt("cache: --kv-endpoint must be an HTTP(S) URL without query or fragment")
	}

	if bucket == "" || strings.ContainsAny(bucket, "/\\") {
		ThrowFmt("cache: --kv-bucket must be a nonempty bucket name")
	}

	if timeout <= 0 {
		ThrowFmt("cache: --kv-timeout must be positive")
	}

	return &blobKV{
		baseURL: strings.TrimRight(endpoint, "/") + "/v1/" + url.PathEscape(bucket),
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// get returns nil only for a missing key. All other failures throw.
func (kv *blobKV) get(ctx context.Context, uid string) []byte {
	req := Throw2(http.NewRequestWithContext(ctx, http.MethodGet, kv.baseURL+"/get?key="+url.QueryEscape(uid), nil))
	resp := Throw2(kv.client.Do(req))
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		ThrowFmt("KV GET: HTTP %d", resp.StatusCode)
	}

	if resp.ContentLength > maxBlobKVSize {
		ThrowFmt("KV GET: value exceeds 64 MiB")
	}

	data := Throw2(io.ReadAll(io.LimitReader(resp.Body, maxBlobKVSize+1)))

	if len(data) > maxBlobKVSize {
		ThrowFmt("KV GET: value exceeds 64 MiB")
	}

	return data
}

func (kv *blobKV) put(ctx context.Context, uid string, data []byte) {
	req := Throw2(http.NewRequestWithContext(ctx, http.MethodPut, kv.baseURL+"/put?key="+url.QueryEscape(uid), bytes.NewReader(data)))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp := Throw2(kv.client.Do(req))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		ThrowFmt("KV PUT: HTTP %d", resp.StatusCode)
	}
}
