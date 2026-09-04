package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const statsQueuePrefix = "queue/"
const statsKey = "stats"

// statsQueue is the unbounded hand-off between /v1/resolve handlers and
// the single chunk-writer goroutine. Producers never block; the consumer
// takes everything accumulated so far in one swap, which is where the
// coalescing happens while a previous chunk upload is in flight.
type statsQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending [][]string
}

func newStatsQueue() *statsQueue {
	q := &statsQueue{}
	q.cond = sync.NewCond(&q.mu)

	return q
}

func (q *statsQueue) put(uids []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending = append(q.pending, uids)
	q.cond.Signal()
}

func (q *statsQueue) takeAll() [][]string {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.pending) == 0 {
		q.cond.Wait()
	}

	batch := q.pending
	q.pending = nil

	return batch
}

// statsChunk flattens a batch into a jsonline body: one JSON-encoded
// uid string per line.
func statsChunk(batch [][]string) []byte {
	var body bytes.Buffer

	for _, uids := range batch {
		for _, uid := range uids {
			body.Write(Throw2(json.Marshal(uid)))
			body.WriteByte('\n')
		}
	}

	return body.Bytes()
}

func statsChunkKey(now time.Time, host string) string {
	return fmt.Sprintf("%s%d-%s-%08x", statsQueuePrefix, now.Unix(), host, rand.Uint32())
}

func (s *cacheSrv) statsLoop(uploader objectPutter, bucket string) {
	host := Throw2(os.Hostname())

	for {
		body := statsChunk(s.stats.takeAll())

		if len(body) == 0 {
			continue
		}

		key := statsChunkKey(time.Now(), host)
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})

		if err != nil {
			fmt.Fprintf(os.Stderr, "molot cache: stats chunk %s: %v\n", key, err)
		}
	}
}

type objectPutter interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// parseChunkTS extracts the unix-seconds timestamp from a chunk key of
// the form queue/<ts>-<host>-<rand>.
func parseChunkTS(key string) (int64, error) {
	name := strings.TrimPrefix(key, statsQueuePrefix)
	ts, _, ok := strings.Cut(name, "-")

	if !ok {
		return 0, fmt.Errorf("chunk key %q has no timestamp", key)
	}

	return strconv.ParseInt(ts, 10, 64)
}

func mergeChunk(stats map[string]int64, ts int64, lines *bufio.Scanner) {
	for lines.Scan() {
		line := strings.TrimSpace(lines.Text())

		if line == "" {
			continue
		}

		var uid string
		Throw(json.Unmarshal([]byte(line), &uid))

		if stats[uid] < ts {
			stats[uid] = ts
		}
	}

	Throw(lines.Err())
}

func statsMain(args []string) {
	fs := flag.NewFlagSet("molot stats", flag.ContinueOnError)
	Throw(fs.Parse(args))

	cfg := loadS3Config()
	ctx := context.Background()

	// Pin the chunk list up front: chunks written while we merge stay
	// for the next run, and the delete below touches only what was read.
	var chunks []string
	var token *string

	for {
		page := Throw2(cfg.S3Cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(cfg.S3Bucket),
			Prefix:            aws.String(statsQueuePrefix),
			ContinuationToken: token,
		}))

		for _, object := range page.Contents {
			chunks = append(chunks, *object.Key)
		}

		if page.NextContinuationToken == nil {
			break
		}

		token = page.NextContinuationToken
	}

	stats := map[string]int64{}

	if resp, err := cfg.S3Cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(statsKey),
	}); err == nil {
		Throw(json.NewDecoder(resp.Body).Decode(&stats))
		Throw(resp.Body.Close())
	} else if !isNoSuchKey(err) {
		Throw(err)
	}

	before := len(stats)

	for _, key := range chunks {
		ts, err := parseChunkTS(key)

		if err != nil {
			// Leave the alien object in place: visible in every run's
			// log instead of silently destroyed.
			fmt.Fprintf(os.Stderr, "molot stats: skip %v\n", err)

			continue
		}

		resp := Throw2(cfg.S3Cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(key),
		}))
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
		mergeChunk(stats, ts, scanner)
		Throw(resp.Body.Close())
	}

	Throw2(cfg.S3Cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(statsKey),
		Body:   bytes.NewReader(Throw2(json.Marshal(stats))),
	}))

	deleteChunks(ctx, cfg, chunks)

	fmt.Fprintf(os.Stderr, "molot stats: merged %d chunks, %d -> %d uids tracked\n",
		len(chunks), before, len(stats))
}

func deleteChunks(ctx context.Context, cfg *Config, chunks []string) {
	deletable := make([]types.ObjectIdentifier, 0, len(chunks))

	for _, key := range chunks {
		if _, err := parseChunkTS(key); err == nil {
			deletable = append(deletable, types.ObjectIdentifier{Key: aws.String(key)})
		}
	}

	for start := 0; start < len(deletable); start += 1000 {
		end := min(start+1000, len(deletable))

		Throw2(cfg.S3Cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(cfg.S3Bucket),
			Delete: &types.Delete{Objects: deletable[start:end]},
		}))
	}
}

func isNoSuchKey(err error) bool {
	var noSuchKey *types.NoSuchKey

	return errors.As(err, &noSuchKey)
}
