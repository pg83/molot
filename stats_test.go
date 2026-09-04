package main

import (
	"bufio"
	"strings"
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

	ts, err := parseChunkTS(key)

	if err != nil || ts != testTime().Unix() {
		t.Fatalf("ts=%d err=%v", ts, err)
	}

	if _, err := parseChunkTS("queue/garbage"); err == nil {
		t.Fatal("garbage key parsed")
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
