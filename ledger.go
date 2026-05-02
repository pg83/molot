package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
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
// goroutine owns the slice; Add sends events through ch, Close stops
// accepting new events and returns the final slice. No mutex — every
// read/write to recs happens inside the collector.
type Ledger struct {
	ch     chan NodeRec
	closed chan []NodeRec
}

func newLedger() *Ledger {
	l := &Ledger{
		ch:     make(chan NodeRec, 256),
		closed: make(chan []NodeRec, 1),
	}

	go func() {
		var recs []NodeRec

		for r := range l.ch {
			recs = append(recs, r)
		}

		l.closed <- recs
	}()

	return l
}

func (l *Ledger) Add(r NodeRec) {
	l.ch <- r
}

func (l *Ledger) Close() []NodeRec {
	close(l.ch)

	return <-l.closed
}

// runKey returns the S3 key for a Run manifest. ISO8601 with milliseconds
// in UTC: lex-sort matches chronological order, mc ls is human-readable,
// no UUID needed (collisions inside one millisecond ignored — molot
// invocations don't fire at sub-ms cadence).
func runKey(bucket string, started time.Time) string {
	return fmt.Sprintf("molot/%s/runs/%s.json", bucket, tsFmt(started))
}

func graphKey(bucket string, started time.Time) string {
	return fmt.Sprintf("molot/%s/graphs/%s.json", bucket, tsFmt(started))
}

func tsFmt(t time.Time) string {
	return t.UTC().Format("2006-01-02T15-04-05.000Z")
}

func uploadLedger(cfg *Config, run Run) {
	uploadJSON(cfg, runKey(cfg.S3Bucket, run.StartedAt), run, "ledger")
}

func uploadGraph(cfg *Config, started time.Time, g *Graph) {
	uploadJSON(cfg, graphKey(cfg.S3Bucket, started), g, "graph")
}

func uploadJSON(cfg *Config, key string, body any, label string) {
	data := Throw2(json.MarshalIndent(body, "", "  "))

	f := Throw2(os.CreateTemp(".", "molot-"+label+"-*.json"))
	defer os.Remove(f.Name())

	Throw2(f.Write(data))
	Throw(f.Close())

	mcCfg := Throw2(os.MkdirTemp(".", "mc-"+label+"-"))
	defer os.RemoveAll(mcCfg)

	cmd := exec.Command("minio-client", "--config-dir", mcCfg, "cp", f.Name(), key)
	cmd.Env = append(os.Environ(), "MC_HOST_molot="+cfg.MCHost)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ThrowFmt("%s upload: %v\n%s", label, err, stderr.String())
	}
}
