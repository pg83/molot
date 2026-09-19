package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type storeClient struct {
	endpoint string
	client   *http.Client
}

func newStoreClient(endpoint string) *storeClient {
	u := Throw2(url.Parse(endpoint))

	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		ThrowFmt("MOLOT_STORE_ENDPOINT / --store-endpoint must be an explicit HTTP(S) URL without query or fragment")
	}

	return &storeClient{
		endpoint: strings.TrimRight(endpoint, "/"),
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (s *storeClient) blobURL(uid string) string {
	if !validCacheUID(uid) {
		ThrowFmt("invalid artifact UID %q", uid)
	}

	return s.endpoint + "/v1/blob/" + url.PathEscape(uid)
}

func (s *storeClient) get(ctx context.Context, uid string, dst io.Writer) {
	req := Throw2(http.NewRequestWithContext(ctx, http.MethodGet, s.blobURL(uid), nil))
	resp := Throw2(s.client.Do(req))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ThrowHTTP(resp.StatusCode, "store GET %s: HTTP %d", uid, resp.StatusCode)
	}

	Throw2(io.Copy(dst, resp.Body))
}

func (s *storeClient) put(ctx context.Context, uid string, file *os.File) {
	info := Throw2(file.Stat())
	req := Throw2(http.NewRequestWithContext(ctx, http.MethodPut, s.blobURL(uid), file))
	req.ContentLength = info.Size()
	req.Header.Set("Content-Type", "application/zstd")
	resp := Throw2(s.client.Do(req))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		ThrowFmt("store PUT %s: HTTP %d", uid, resp.StatusCode)
	}

	fmt.Fprintf(os.Stderr, "molot exec: pushed %s via store\n", uid)
}
