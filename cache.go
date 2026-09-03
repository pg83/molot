package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Cache is the in-memory set of completed gorn GUIDs. It is seeded once
// from a molot cache server's /v1/resolve and grows as this run finishes
// nodes; s3StatExists in the executor backstops anything the index has
// not caught up with yet.
type Cache struct {
	mu  sync.Mutex
	set map[string]bool
}

func newCache(seed map[string]bool) *Cache {
	if seed == nil {
		seed = map[string]bool{}
	}

	return &Cache{set: seed}
}

func (c *Cache) Has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.set[key]
}

func (c *Cache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.set[key] = true
}

const resolveAttemptTimeout = 30 * time.Second

func parseResolveEndpoints(raw string) []string {
	var result []string

	for _, item := range strings.Split(raw, ",") {
		endpoint := strings.TrimSpace(item)

		if endpoint == "" {
			continue
		}

		if !strings.Contains(endpoint, "://") {
			endpoint = "http://" + endpoint
		}

		result = append(result, strings.TrimRight(endpoint, "/"))
	}

	return result
}

func resolveFromEndpoint(endpoint string, payload []byte) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), resolveAttemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/resolve", bytes.NewReader(payload))

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var available []string

	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&available); err != nil {
		return nil, fmt.Errorf("bad response: %v", err)
	}

	return available, nil
}

// resolveCompleted asks the first answering cache endpoint which of the
// graph's uids already have a result. Every failure is non-fatal: with
// no answer the run starts from an empty set and the executor falls back
// to per-node S3 stats.
func resolveCompleted(raw string, uids []string) map[string]bool {
	result := map[string]bool{}
	endpoints := parseResolveEndpoints(raw)

	if len(endpoints) == 0 || len(uids) == 0 {
		return result
	}

	payload := Throw2(json.Marshal(uids))

	for _, endpoint := range endpoints {
		available, err := resolveFromEndpoint(endpoint, payload)

		if err != nil {
			fmt.Fprintf(os.Stderr, "molot exec: resolve %s: %v, trying next endpoint\n", endpoint, err)

			continue
		}

		for _, uid := range available {
			result[uid] = true
		}

		fmt.Fprintf(os.Stderr, "molot exec: resolved %d/%d nodes via %s\n", len(result), len(uids), endpoint)

		return result
	}

	fmt.Fprintln(os.Stderr, "molot exec: no usable resolve endpoints, falling back to per-node S3 stats")

	return result
}
