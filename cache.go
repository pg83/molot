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
// from a molot store's authoritative /v1/resolve and grows as this run
// finishes nodes.
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

func resolveFromEndpoint(endpoint string, payload []byte) []string {
	ctx, cancel := context.WithTimeout(context.Background(), resolveAttemptTimeout)
	defer cancel()

	req := Throw2(http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/resolve", bytes.NewReader(payload)))
	req.Header.Set("Content-Type", "application/json")
	resp := Throw2(http.DefaultClient.Do(req))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := Throw2(io.ReadAll(io.LimitReader(resp.Body, 4096)))
		ThrowFmt("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var available []string
	Throw(json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&available))

	return available
}

// resolveCompleted uses the first successful authoritative answer. When all
// stores fail, the graph must not be scheduled as though every UID were absent.
func resolveCompleted(raw string, uids []string) map[string]bool {
	result := map[string]bool{}
	endpoints := parseResolveEndpoints(raw)

	if len(endpoints) == 0 {
		ThrowFmt("no store resolve endpoints")
	}

	if len(uids) == 0 {
		return result
	}

	payload := Throw2(json.Marshal(uids))

	for _, endpoint := range endpoints {
		exc := Try(func() {
			for _, uid := range resolveFromEndpoint(endpoint, payload) {
				result[uid] = true
			}
		})

		if exc == nil {
			fmt.Fprintf(os.Stderr, "molot exec: resolved %d/%d nodes via %s\n", len(result), len(uids), endpoint)

			return result
		}

		fmt.Fprintf(os.Stderr, "molot exec: resolve %s: %v, trying next endpoint\n", endpoint, exc)
	}

	ThrowFmt("molot exec: no usable authoritative resolve endpoints")

	return result
}
