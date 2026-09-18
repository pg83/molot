package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestResolvePrefersMolotResolveOverIXPackageCache(t *testing.T) {
	t.Setenv("MOLOT_RESOLVE", "10.0.0.1:8054")
	t.Setenv("IX_PACKAGE_CACHE", "10.0.0.2:8054")
	c := &Config{}
	overlayFromEnv(c)

	if c.Resolve != "10.0.0.1:8054" {
		t.Fatalf("Resolve=%q", c.Resolve)
	}

	t.Setenv("MOLOT_RESOLVE", "")
	c = &Config{}
	overlayFromEnv(c)

	if c.Resolve != "10.0.0.2:8054" {
		t.Fatalf("Resolve=%q", c.Resolve)
	}
}

func TestResolveCompletedSkipsFailedAnswersWithoutPartialResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"HTTP failure", http.StatusServiceUnavailable, "unavailable"},
		{"invalid JSON", http.StatusOK, "not JSON"},
		{"partial list", http.StatusOK, `["leaked",`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer failed.Close()

			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `["done"]`)
			}))
			defer good.Close()

			seed := resolveCompleted(failed.URL+","+good.URL, []string{"leaked", "done", "pending"})

			if len(seed) != 1 || !seed["done"] {
				t.Fatalf("seed=%v, want only done", seed)
			}
		})
	}
}

func TestResolveCompletedEmptyAnswerStopsFallback(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()

	seed := resolveCompleted(server.URL+","+server.URL, []string{"uid"})

	if len(seed) != 0 || calls.Load() != 1 {
		t.Fatalf("seed=%v endpoint calls=%d", seed, calls.Load())
	}
}

func TestResolveFromEndpointThrows(t *testing.T) {
	if exc := Try(func() {
		resolveFromEndpoint("http://%", []byte(`[]`))
	}); exc == nil {
		t.Fatal("invalid URL must throw")
	}
}

func TestResolveCompletedSeedsFromFirstAnsweringEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/resolve" {
			http.NotFound(w, r)

			return
		}

		var requested []string

		if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
			t.Fatal(err)
		}

		if len(requested) != 2 {
			t.Fatalf("requested=%v", requested)
		}

		_ = json.NewEncoder(w).Encode([]string{"done"})
	}))
	defer server.Close()

	// The dead endpoint before the live one must be skipped, not fatal.
	raw := "127.0.0.1:1," + server.URL
	cache := newCache(resolveCompleted(raw, []string{"done", "pending"}))

	if !cache.Has("done") || cache.Has("pending") {
		t.Fatal("bad seed")
	}

	cache.Add("pending")

	if !cache.Has("pending") {
		t.Fatal("in-memory add lost")
	}
}

func TestResolveCompletedFailureIsEmptyNotFatal(t *testing.T) {
	seed := resolveCompleted("127.0.0.1:1", []string{"uid"})

	if len(seed) != 0 {
		t.Fatalf("seed=%v", seed)
	}

	if len(resolveCompleted("", []string{"uid"})) != 0 {
		t.Fatal("empty endpoint list must not resolve")
	}
}
