package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

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
	Key       string
	StartedAt string
	Targets   string
	Total     int
	Failed    int
	Cached    int
	Duration  string
	Errored   bool
}

type indexData struct {
	Runs   []runRow
	Now    string
	Bucket string
	Error  string
}

var indexTmpl = template.Must(template.New("index").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>molot runs</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container-fluid py-4">
  <div class="d-flex justify-content-between align-items-baseline mb-3">
    <h1 class="mb-0">molot runs</h1>
    <small class="text-muted">refresh 5s · {{.Now}} · bucket {{.Bucket}}</small>
  </div>

  {{if .Error}}<div class="alert alert-danger"><code>{{.Error}}</code></div>{{end}}

  <table class="table table-sm table-striped table-bordered bg-white">
    <thead class="table-dark">
      <tr><th>started</th><th>targets</th><th>nodes</th><th>cached</th><th>failed</th><th>duration</th></tr>
    </thead>
    <tbody>
    {{range .Runs}}
      <tr class="{{if .Errored}}table-danger{{end}}">
        <td><a href="/run/{{.Key}}"><code>{{.StartedAt}}</code></a></td>
        <td><code>{{.Targets}}</code></td>
        <td>{{.Total}}</td>
        <td>{{.Cached}}</td>
        <td>{{if .Failed}}<strong>{{.Failed}}</strong>{{else}}0{{end}}</td>
        <td>{{.Duration}}</td>
      </tr>
    {{else}}
      <tr><td colspan="6" class="text-muted">no runs in s3://{{$.Bucket}}/molot/{{$.Bucket}}/runs/ yet</td></tr>
    {{end}}
    </tbody>
  </table>
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
	Error     string
}

type runData struct {
	Key       string
	StartedAt string
	EndedAt   string
	Targets   string
	Failed    bool
	Nodes     []nodeRow
	Error     string
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
  <p class="text-muted">targets: <code>{{.Targets}}</code> · ended: <code>{{.EndedAt}}</code></p>

  {{if .Error}}<div class="alert alert-danger"><code>{{.Error}}</code></div>{{end}}

  <table class="table table-sm table-striped table-bordered bg-white">
    <thead class="table-dark">
      <tr><th>uid</th><th>out</th><th>started</th><th>duration</th><th>status</th><th>error</th></tr>
    </thead>
    <tbody>
    {{range .Nodes}}
      <tr class="{{if .Failed}}{{if .BrokenBy}}table-warning{{else}}table-danger{{end}}{{end}}">
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
        <td><small><code>{{.Error}}</code></small></td>
      </tr>
    {{end}}
    </tbody>
  </table>
</div>
</body>
</html>`))

func (s *webSrv) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)

		return
	}

	data := indexData{
		Now:    time.Now().UTC().Format(time.RFC3339),
		Bucket: s.cfg.S3Bucket,
	}

	exc := Try(func() {
		keys := s.listRuns()

		for _, k := range keys {
			run := s.fetchRun(k)

			row := runRow{
				Key:       k,
				StartedAt: run.StartedAt.UTC().Format("2006-01-02 15:04:05Z"),
				Targets:   strings.Join(run.Targets, " "),
				Total:     len(run.Nodes),
				Errored:   run.Failed,
				Duration:  run.EndedAt.Sub(run.StartedAt).Truncate(time.Second).String(),
			}

			for _, n := range run.Nodes {
				if n.Failed {
					row.Failed++
				}

				if n.Cached {
					row.Cached++
				}
			}

			data.Runs = append(data.Runs, row)
		}
	})

	exc.Catch(func(e *Exception) {
		data.Error = e.Error()
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = indexTmpl.Execute(w, data)
}

func (s *webSrv) handleRun(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/run/")

	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "bad key", http.StatusBadRequest)

		return
	}

	data := runData{Key: key}

	exc := Try(func() {
		run := s.fetchRun(key)

		data.StartedAt = run.StartedAt.UTC().Format("2006-01-02 15:04:05.000Z")
		data.EndedAt = run.EndedAt.UTC().Format("2006-01-02 15:04:05.000Z")
		data.Targets = strings.Join(run.Targets, " ")
		data.Failed = run.Failed
		data.Nodes = make([]nodeRow, len(run.Nodes))

		for i, n := range run.Nodes {
			data.Nodes[i] = nodeRow{
				UID:       n.UID,
				Out:       n.Out,
				StartedAt: n.StartedAt.UTC().Format("15:04:05.000"),
				Duration:  n.FinishedAt.Sub(n.StartedAt).Truncate(time.Millisecond).String(),
				Failed:    n.Failed,
				Cached:    n.Cached,
				BrokenBy:  n.BrokenBy,
				Error:     n.Error,
			}
		}
	})

	exc.Catch(func(e *Exception) {
		data.Error = e.Error()
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = runTmpl.Execute(w, data)
}

func (s *webSrv) handleNodeStream(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/node/")
	parts := strings.Split(rest, "/")

	if len(parts) != 2 || parts[0] == "" {
		http.Error(w, "expected /node/<uid>/{stderr,stdout}", http.StatusBadRequest)

		return
	}

	uid, kind := parts[0], parts[1]

	if kind != "stderr" && kind != "stdout" {
		http.Error(w, "expected stderr or stdout", http.StatusBadRequest)

		return
	}

	url := fmt.Sprintf("%s/v1/tasks/%s/output?root=%s", strings.TrimRight(s.cfg.GornAPI, "/"), uid, s.cfg.S3Root)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	resp, err := s.http.Do(req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	if resp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("gorn-control HTTP %d: %s", resp.StatusCode, body), http.StatusBadGateway)

		return
	}

	var out struct {
		StdoutB64 string `json:"stdout_b64"`
		StderrB64 string `json:"stderr_b64"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	enc := out.StderrB64

	if kind == "stdout" {
		enc = out.StdoutB64
	}

	dec, err := base64.StdEncoding.DecodeString(enc)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(dec)
}

// listRuns returns the most-recent N run keys, sorted newest-first. ISO
// keys lex-sort chronologically, so reverse sort gives newest first.
func (s *webSrv) listRuns() []string {
	const limit = 200

	mcCfg := Throw2(os.MkdirTemp(".", "mc-listruns-"))
	defer os.RemoveAll(mcCfg)

	cmd := exec.Command("minio-client", "--config-dir", mcCfg, "ls", "--json", fmt.Sprintf("molot/%s/runs/", s.cfg.S3Bucket))
	cmd.Env = append(os.Environ(), "MC_HOST_molot="+s.cfg.MCHost)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ThrowFmt("mc ls runs: %v\n%s", err, stderr.String())
	}

	var keys []string

	for _, line := range strings.Split(stdout.String(), "\n") {
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		var rec struct {
			Key string `json:"key"`
		}

		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}

		k := strings.TrimSuffix(rec.Key, ".json")

		if k == "" || strings.HasSuffix(rec.Key, "/") {
			continue
		}

		keys = append(keys, k)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(keys)))

	if len(keys) > limit {
		keys = keys[:limit]
	}

	return keys
}

func (s *webSrv) fetchRun(key string) Run {
	mcCfg := Throw2(os.MkdirTemp(".", "mc-fetchrun-"))
	defer os.RemoveAll(mcCfg)

	cmd := exec.Command("minio-client", "--config-dir", mcCfg, "cat", fmt.Sprintf("molot/%s/runs/%s.json", s.cfg.S3Bucket, key))
	cmd.Env = append(os.Environ(), "MC_HOST_molot="+s.cfg.MCHost)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ThrowFmt("mc cat %s: %v\n%s", key, err, stderr.String())
	}

	var run Run
	Throw(json.Unmarshal(stdout.Bytes(), &run))

	return run
}
