package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStatsQueueTakesEverythingAccumulated(t *testing.T) {
	q := newStatsQueue()
	q.put([]string{"a", "b"})
	q.put([]string{"b", "c"})

	batch := q.takeAll()

	if len(batch) != 2 {
		t.Fatalf("batch=%v", batch)
	}

	body := string(statsChunk(batch))

	if body != "\"a\"\n\"b\"\n\"b\"\n\"c\"\n" {
		t.Fatalf("chunk=%q", body)
	}
}

func TestStatsChunkKeyRoundTrips(t *testing.T) {
	key := statsChunkKey(testTime(), "lab1")

	if !strings.HasPrefix(key, "queue/") {
		t.Fatalf("key=%q", key)
	}

	ts := parseChunkTS(key)

	if ts != testTime().Unix() {
		t.Fatalf("ts=%d", ts)
	}

	for _, key := range []string{"queue/garbage", "queue/not-a-timestamp", "queue/999999999999999999999999-host-rand"} {
		if exc := Try(func() {
			parseChunkTS(key)
		}); exc == nil {
			t.Fatalf("invalid key %q parsed", key)
		}
	}
}

func TestStatsSkipsInvalidKeysInMergeAndDelete(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	var saved map[string]int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		requests = append(requests, r.Method+" "+r.URL.Path)

		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>queue/500-lab-rand</Key></Contents><Contents><Key>queue/garbage</Key></Contents></ListBucketResult>`)
		case r.Method == http.MethodGet && r.URL.Path == "/molot/stats":
			fmt.Fprint(w, `{"old":100,"fresh":900}`)
		case r.Method == http.MethodGet && r.URL.Path == "/molot/queue/500-lab-rand":
			fmt.Fprint(w, "\"old\"\n\"fresh\"\n\"new\"\n")
		case r.Method == http.MethodPut && r.URL.Path == "/molot/stats":
			if err := json.NewDecoder(r.Body).Decode(&saved); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
			}
		case r.Method == http.MethodDelete && r.URL.Path == "/molot/queue/500-lab-rand":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	t.Setenv("S3_BUCKET", "molot")
	t.Setenv("S3_ENDPOINT", server.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	statsMain(nil)

	mu.Lock()
	defer mu.Unlock()

	if len(saved) != 3 || saved["old"] != 500 || saved["fresh"] != 900 || saved["new"] != 500 {
		t.Fatalf("saved stats=%v", saved)
	}

	if len(requests) != 5 || requests[4] != "DELETE /molot/queue/500-lab-rand" {
		t.Fatalf("requests=%v", requests)
	}
}

func TestMergeChunkKeepsLatestTimestamp(t *testing.T) {
	stats := map[string]int64{"old": 100, "fresh": 900}

	mergeChunk(stats, 500, bufio.NewScanner(strings.NewReader("\"old\"\n\"fresh\"\n\"new\"\n\n")))

	if stats["old"] != 500 || stats["fresh"] != 900 || stats["new"] != 500 {
		t.Fatalf("stats=%v", stats)
	}
}

func testTime() time.Time {
	return time.Unix(1757000000, 0)
}
