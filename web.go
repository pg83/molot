package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintln(w, msg)
}

// sendHTTPException is the boundary between the Throw flow and HTTP.
// ThrowHTTP raises a typed HTTPError; anything else (mc cat / json
// parse / template Execute) is unexpected and maps to 500. Always
// log the original to stderr so a 500 in the browser leaves a trail.
func sendHTTPException(w http.ResponseWriter, r *http.Request, e *Exception) {
	fmt.Fprintf(os.Stderr, "molot web: %s %s: %s\n", r.Method, r.URL.Path, e.Error())

	var he *HTTPError

	if errors.As(e.AsError(), &he) {
		httpError(w, he.Status, he.Msg)

		return
	}

	httpError(w, http.StatusInternalServerError, e.Error())
}

func webMain(args []string) {
	fs := flag.NewFlagSet("molot web", flag.ContinueOnError)

	listen := fs.String("listen", "", "HTTP listen address, e.g. :8051")

	Throw(fs.Parse(args))

	if *listen == "" {
		ThrowFmt("web: --listen is required")
	}

	cfg := loadConfig(nil)

	srv := &webSrv{
		cfg:  cfg,
		http: &http.Client{Timeout: 10 * time.Second},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/archive", srv.handleArchive)
	mux.HandleFunc("/run/", srv.handleRun)
	mux.HandleFunc("/node/", srv.handleNodeStream)

	server := &http.Server{Addr: *listen, Handler: mux}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigs
		fmt.Fprintln(os.Stderr, "molot web: signal:", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = server.Shutdown(ctx)
	}()

	fmt.Fprintln(os.Stderr, "molot web: listening on", *listen, "bucket=", cfg.S3Bucket, "gorn=", cfg.GornAPI)

	err := server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		Throw(err)
	}
}

type webSrv struct {
	cfg  *Config
	http *http.Client
}

type runRow struct {
	Key      string
	Argv     string
	GitRev   string
	Duration string
	Status   string
	Failed   int
	Done     uint64
	Total    uint64
}

type indexData struct {
	Runs   []runRow
	Now    string
	Bucket string
}

var indexTmpl = template.Must(template.New("index").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>molot runs (running)</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container-fluid py-4">
  <div class="d-flex justify-content-between align-items-baseline mb-3">
    <h1 class="mb-0">molot runs (running)</h1>
    <small class="text-muted">
      refresh 5s · {{.Now}} · bucket {{.Bucket}} ·
      <a href="/archive">archive &rarr;</a>
    </small>
  </div>

  <table class="table table-sm table-striped table-bordered bg-white">
    <thead class="table-dark">
      <tr><th>argv</th><th>rev</th><th>duration</th><th>status</th><th>failed / done / total</th></tr>
    </thead>
    <tbody>
    {{range .Runs}}
      <tr class="table-info">
        <td><a href="/run/{{.Key}}"><code>{{if .Argv}}{{.Argv}}{{else}}—{{end}}</code></a></td>
        <td><code title="{{.GitRev}}">{{if .GitRev}}{{slice .GitRev 0 12}}{{else}}—{{end}}</code></td>
        <td>{{.Duration}}</td>
        <td><strong>{{.Status}}</strong></td>
        <td>{{if .Failed}}<strong class="text-danger">{{.Failed}}</strong>{{else}}—{{end}} / <span class="text-success">{{.Done}}</span> / {{.Total}}</td>
      </tr>
    {{else}}
      <tr><td colspan="5" class="text-muted">no running runs — see <a href="/archive">archive</a></td></tr>
    {{end}}
    </tbody>
  </table>
</div>
</body>
</html>`))

type archiveData struct {
	Runs       []runRow
	Now        string
	Bucket     string
	NextBefore string
	HasNext    bool
}

var archiveTmpl = template.Must(template.New("archive").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>molot runs archive</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container-fluid py-4">
  <div class="d-flex justify-content-between align-items-baseline mb-3">
    <h1 class="mb-0">molot runs archive</h1>
    <small class="text-muted">
      <a href="/">&larr; live</a> · {{.Now}} · bucket {{.Bucket}}
    </small>
  </div>

  <table class="table table-sm table-striped table-bordered bg-white">
    <thead class="table-dark">
      <tr><th>argv</th><th>rev</th><th>duration</th><th>status</th><th>failed / done / total</th></tr>
    </thead>
    <tbody>
    {{range .Runs}}
      <tr class="{{if eq .Status "running"}}table-info{{else if eq .Status "failed"}}table-danger{{else if eq .Status "stuck"}}table-secondary{{else}}table-success{{end}}">
        <td><a href="/run/{{.Key}}"><code>{{if .Argv}}{{.Argv}}{{else}}—{{end}}</code></a></td>
        <td><code title="{{.GitRev}}">{{if .GitRev}}{{slice .GitRev 0 12}}{{else}}—{{end}}</code></td>
        <td>{{.Duration}}</td>
        <td><strong>{{.Status}}</strong></td>
        <td>{{if .Failed}}<strong class="text-danger">{{.Failed}}</strong>{{else}}—{{end}} / <span class="text-success">{{.Done}}</span> / {{.Total}}</td>
      </tr>
    {{else}}
      <tr><td colspan="5" class="text-muted">no more runs</td></tr>
    {{end}}
    </tbody>
  </table>

  {{if .HasNext}}
  <div>
    <a class="btn btn-secondary btn-sm" href="/archive?before={{.NextBefore}}">older &rarr;</a>
  </div>
  {{end}}
</div>
</body>
</html>`))

type nodeRow struct {
	UID       string
	Out       string
	StartedAt string
	Duration  string
	Failed    bool
	Cached    bool
	BrokenBy  string
}

type runData struct {
	Key        string
	StartedAt  string
	EndedAt    string
	Failed     bool
	Nodes      []nodeRow
	NumNodes   int
	TotalNodes int
}

var runTmpl = template.Must(template.New("run").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>molot run {{.Key}}</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container-fluid py-4">
  <a href="/" class="text-decoration-none">&larr; runs</a>
  <h1 class="mt-2">run {{.StartedAt}} {{if .Failed}}<span class="badge bg-danger">failed</span>{{end}}</h1>
  <p class="text-muted">ended: <code>{{.EndedAt}}</code> · {{.NumNodes}} failed nodes (out of {{.TotalNodes}} total)</p>

  <table class="table table-sm table-striped table-bordered bg-white">
    <thead class="table-dark">
      <tr><th>uid</th><th>out</th><th>started</th><th>duration</th><th>status</th></tr>
    </thead>
    <tbody>
    {{range .Nodes}}
      <tr id="{{.UID}}" class="{{if .Failed}}{{if .BrokenBy}}table-warning{{else}}table-danger{{end}}{{end}}">
        <td><a href="/node/{{.UID}}/stderr"><code>{{.UID}}</code></a></td>
        <td><code>{{.Out}}</code></td>
        <td><small>{{.StartedAt}}</small></td>
        <td>{{.Duration}}</td>
        <td>
          {{if .Failed}}
            {{if .BrokenBy}}
              <span class="text-warning">broken by</span> <a href="#{{.BrokenBy}}"><code>{{.BrokenBy}}</code></a>
            {{else}}
              <strong class="text-danger">failed</strong>
            {{end}}
          {{else if .Cached}}
            <span class="text-secondary">cached</span>
          {{else}}
            ok
          {{end}}
        </td>
      </tr>
    {{end}}
    </tbody>
  </table>
</div>
</body>
</html>`))

func (s *webSrv) handleIndex(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
		if r.URL.Path != "/" {
			ThrowHTTP(http.StatusNotFound, "not found")
		}

		data := indexData{
			Now:    time.Now().UTC().Format(time.RFC3339),
			Bucket: s.cfg.S3Bucket,
		}

		// Running heartbeat re-uploads runs/<key>.json every
		// HeartbeatPeriod, so a running run's S3 LastModified is always
		// fresh. Filter on that BEFORE fetching JSONs — the LIST already
		// has LastModified, and ended/stuck runs lose their fresh window
		// within 3*heartbeat. Fetching only the freshly-modified subset
		// drops the per-page-load GET count from ~50 to typically 0–5.
		cutoff := time.Now().Add(-3 * HeartbeatPeriod)
		entries := s.listRuns("", 200)

		for _, e := range entries {
			if e.LastModified.Before(cutoff) {
				continue
			}

			run := s.fetchRun(e.Key)

			// LastModified being fresh means either still running or
			// ended-within-the-last-90s; the EndedAt check filters out
			// the latter.
			if !run.EndedAt.IsZero() {
				continue
			}

			row := runRow{
				Key:      e.Key,
				Argv:     strings.Join(run.Argv, " "),
				GitRev:   run.GitRev,
				Done:     run.Done,
				Total:    run.Total,
				Status:   "running",
				Duration: runDuration(run).Truncate(time.Second).String(),
			}

			for _, n := range run.Nodes {
				if n.Failed {
					row.Failed++
				}
			}

			data.Runs = append(data.Runs, row)
		}

		var buf bytes.Buffer
		Throw(indexTmpl.Execute(&buf, data))

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		Throw2(w.Write(buf.Bytes()))
	})

	exc.Catch(func(e *Exception) {
		sendHTTPException(w, r, e)
	})
}

func (s *webSrv) handleArchive(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
		const pageSize = 50

		before := r.URL.Query().Get("before")

		data := archiveData{
			Now:    time.Now().UTC().Format(time.RFC3339),
			Bucket: s.cfg.S3Bucket,
		}

		// Fetch pageSize+1 to detect "has next page" without a separate
		// count query. The (pageSize+1)th entry is dropped and its key
		// becomes the cursor for the next page.
		entries := s.listRuns(before, pageSize+1)

		if len(entries) > pageSize {
			data.HasNext = true
			data.NextBefore = entries[pageSize-1].Key
			entries = entries[:pageSize]
		}

		for _, e := range entries {
			run := s.fetchRun(e.Key)

			row := runRow{
				Key:      e.Key,
				Argv:     strings.Join(run.Argv, " "),
				GitRev:   run.GitRev,
				Done:     run.Done,
				Total:    run.Total,
				Status:   runStatus(run, e.LastModified),
				Duration: runDuration(run).Truncate(time.Second).String(),
			}

			for _, n := range run.Nodes {
				if n.Failed {
					row.Failed++
				}
			}

			data.Runs = append(data.Runs, row)
		}

		var buf bytes.Buffer
		Throw(archiveTmpl.Execute(&buf, data))

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		Throw2(w.Write(buf.Bytes()))
	})

	exc.Catch(func(e *Exception) {
		sendHTTPException(w, r, e)
	})
}

func (s *webSrv) handleRun(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
		key := strings.TrimPrefix(r.URL.Path, "/run/")

		if key == "" || strings.Contains(key, "/") {
			ThrowHTTP(http.StatusBadRequest, "bad key")
		}

		data := runData{Key: key}
		run := s.fetchRun(key)

		data.StartedAt = run.StartedAt.UTC().Format("2006-01-02 15:04:05.000Z")

		if !run.EndedAt.IsZero() {
			data.EndedAt = run.EndedAt.UTC().Format("2006-01-02 15:04:05.000Z")
		} else {
			data.EndedAt = "(running)"
		}

		data.Failed = run.Failed
		data.TotalNodes = len(run.Nodes)

		for _, n := range run.Nodes {
			if !n.Failed {
				continue
			}

			data.Nodes = append(data.Nodes, nodeRow{
				UID:       n.UID,
				Out:       n.Out,
				StartedAt: n.StartedAt.UTC().Format("15:04:05.000"),
				Duration:  n.FinishedAt.Sub(n.StartedAt).Truncate(time.Millisecond).String(),
				Failed:    n.Failed,
				Cached:    n.Cached,
			})
		}

		data.NumNodes = len(data.Nodes)

		fillBrokenBy(data.Nodes, s.fetchGraph(key), run.Nodes)

		var buf bytes.Buffer
		Throw(runTmpl.Execute(&buf, data))

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		Throw2(w.Write(buf.Bytes()))
	})

	exc.Catch(func(e *Exception) {
		sendHTTPException(w, r, e)
	})
}

func (s *webSrv) handleNodeStream(w http.ResponseWriter, r *http.Request) {
	exc := Try(func() {
		rest := strings.TrimPrefix(r.URL.Path, "/node/")
		parts := strings.SplitN(rest, "/", 2)

		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			ThrowHTTP(http.StatusBadRequest, "expected /node/<uid>/<file>")
		}

		uid, name := parts[0], parts[1]

		url := fmt.Sprintf("%s/v1/tasks/%s/content/%s?root=%s", strings.TrimRight(s.cfg.GornAPI, "/"), uid, name, s.cfg.S3Root)

		req := Throw2(http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil))
		resp := Throw2(s.http.Do(req))
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body := Throw2(io.ReadAll(resp.Body))
			ThrowHTTP(resp.StatusCode, "gorn-control HTTP %d: %s", resp.StatusCode, body)
		}

		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}

		Throw2(io.Copy(w, resp.Body))
	})

	exc.Catch(func(e *Exception) {
		sendHTTPException(w, r, e)
	})
}

// runStatus classifies a run for the index coloring. EndedAt zero +
// fresh LastModified (within 3× heartbeat) = running. EndedAt zero +
// stale LastModified = stuck (molot crashed before final upload, no
// heartbeat refreshing the marker). EndedAt set + Failed = failed.
// Otherwise ok.
func runStatus(r Run, lastMod time.Time) string {
	if r.EndedAt.IsZero() {
		if time.Since(lastMod) > 3*HeartbeatPeriod {
			return "stuck"
		}

		return "running"
	}

	if r.Failed {
		return "failed"
	}

	return "ok"
}

func runDuration(r Run) time.Duration {
	if r.EndedAt.IsZero() {
		return time.Now().UTC().Sub(r.StartedAt)
	}

	return r.EndedAt.Sub(r.StartedAt)
}

// fillBrokenBy walks the stored Graph to find, for each Failed nodeRow,
// the first dep (by InDirs order) that's also Failed in the same run.
// "broken by dep" is a derived view: the manifest stores only Failed bool
// per node, the dep chain comes from the Graph.
func fillBrokenBy(rows []nodeRow, g *Graph, recs []NodeRec) {
	if g == nil {
		return
	}

	failed := map[string]bool{}

	for _, r := range recs {
		if r.Failed {
			failed[r.UID] = true
		}
	}

	uidByOut := map[string]string{}

	for _, n := range g.Nodes {
		for _, od := range n.OutDirs {
			uidByOut[od+"/touch"] = n.UID
		}
	}

	nodeByUID := map[string]*Node{}

	for i := range g.Nodes {
		nodeByUID[g.Nodes[i].UID] = &g.Nodes[i]
	}

	for i := range rows {
		if !rows[i].Failed {
			continue
		}

		gn := nodeByUID[rows[i].UID]

		if gn == nil {
			continue
		}

		for _, in := range gn.InDirs {
			depUID := uidByOut[in+"/touch"]

			if depUID != "" && failed[depUID] {
				rows[i].BrokenBy = depUID

				break
			}
		}
	}
}

type runEntry struct {
	Key          string
	LastModified time.Time
}

// listRuns lists run entries newest-first; ISO keys lex-sort chrono.
// `before` is an exclusive cursor (key < before) for pagination; empty
// means "from newest". `limit` caps the page size. LastModified comes
// from S3 and feeds runStatus's stuck-vs-running heuristic.
func (s *webSrv) listRuns(before string, limit int) []runEntry {
	full := s3List(context.Background(), s.cfg.S3Cli, s.cfg.S3Bucket, "runs/")

	var entries []runEntry

	for _, e := range full {
		k := strings.TrimPrefix(e.Key, "runs/")
		k = strings.TrimSuffix(k, ".json")

		if k == "" {
			continue
		}

		if before != "" && k >= before {
			continue
		}

		entries = append(entries, runEntry{Key: k, LastModified: e.LastModified})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key > entries[j].Key
	})

	if len(entries) > limit {
		entries = entries[:limit]
	}

	return entries
}

func (s *webSrv) fetchRun(key string) Run {
	var run Run
	s3GetJSON(context.Background(), s.cfg.S3Cli, s.cfg.S3Bucket, "runs/"+key+".json", &run)

	return run
}

func (s *webSrv) fetchGraph(key string) *Graph {
	g := &Graph{}
	s3GetJSON(context.Background(), s.cfg.S3Cli, s.cfg.S3Bucket, "graphs/"+key+".json", g)

	return g
}
