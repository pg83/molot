package main

import (
	"context"
	"fmt"
	"time"
)

type NodeRec struct {
	UID        string    `json:"uid"`
	Out        string    `json:"out"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Failed     bool      `json:"failed,omitempty"`
	Cached     bool      `json:"cached,omitempty"`
}

type Run struct {
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Targets   []string  `json:"targets"`
	Failed    bool      `json:"failed,omitempty"`
	Nodes     []NodeRec `json:"nodes"`
}

// Ledger is a single-writer accumulator of NodeRec events. The collector
// goroutine owns the slice; Add sends events through ch, Snapshot copies
// the current state out via a reply channel (used by the heartbeat
// goroutine), Close stops accepting new events and returns the final
// slice. No mutex — every read/write to recs happens inside the
// collector.
type Ledger struct {
	ch     chan NodeRec
	snap   chan chan []NodeRec
	closed chan []NodeRec
}

func newLedger() *Ledger {
	l := &Ledger{
		ch:     make(chan NodeRec, 256),
		snap:   make(chan chan []NodeRec),
		closed: make(chan []NodeRec, 1),
	}

	go func() {
		recs := []NodeRec{}

		for {
			select {
			case r, ok := <-l.ch:
				if !ok {
					l.closed <- recs

					return
				}

				recs = append(recs, r)
			case reply := <-l.snap:
				cp := make([]NodeRec, len(recs))
				copy(cp, recs)

				reply <- cp
			}
		}
	}()

	return l
}

func (l *Ledger) Add(r NodeRec) {
	l.ch <- r
}

func (l *Ledger) Snapshot() []NodeRec {
	reply := make(chan []NodeRec, 1)
	l.snap <- reply

	return <-reply
}

func (l *Ledger) Close() []NodeRec {
	close(l.ch)

	return <-l.closed
}

// HeartbeatPeriod is how often a running molot re-uploads the running
// manifest to refresh the S3 LastModified timestamp. The UI uses this
// constant as the basis of its stuck-marker threshold.
const HeartbeatPeriod = 30 * time.Second

// runKey / graphKey return in-bucket keys for the Run manifest and the
// full Graph blob. ISO8601 with milliseconds: lex-sort matches
// chronological order, listing is human-readable, no UUID needed —
// molot invocations don't fire at sub-ms cadence.
func runKey(started time.Time) string {
	return fmt.Sprintf("runs/%s.json", tsFmt(started))
}

func graphKey(started time.Time) string {
	return fmt.Sprintf("graphs/%s.json", tsFmt(started))
}

func tsFmt(t time.Time) string {
	return t.UTC().Format("2006-01-02T15-04-05.000Z")
}

func uploadLedger(cfg *Config, run Run) {
	s3PutJSON(context.Background(), cfg.S3Cli, cfg.S3Bucket, runKey(run.StartedAt), run)
}

func uploadGraph(cfg *Config, started time.Time, g *Graph) {
	s3PutJSON(context.Background(), cfg.S3Cli, cfg.S3Bucket, graphKey(started), g)
}
