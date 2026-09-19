package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerArtifactsRoundTripThroughStore(t *testing.T) {
	srv, fake := newTestStore(t)
	server := httptest.NewServer(srv.routes())
	defer server.Close()
	client := newStoreClient(server.URL)
	work := t.TempDir()
	output := filepath.Join(work, "one-built")
	input := filepath.Join(work, "one-fetched")
	Throw(os.Mkdir(output, 0755))
	Throw(os.Mkdir(input, 0755))
	Throw(os.WriteFile(filepath.Join(output, "artifact"), []byte("build result"), 0644))

	pushOutput(client, work, ExecTask{UID: "one", OutDir: output})
	fetchDeps(client, work, []string{input})
	data := Throw2(os.ReadFile(filepath.Join(input, "artifact")))

	if string(data) != "build result" || fake.puts != 1 || fake.gets != 1 || len(fake.heads) != 0 {
		t.Fatalf("data=%q PUTs=%d GETs=%d HEADs=%v", data, fake.puts, fake.gets, fake.heads)
	}
}

func TestStoreClientEscapesUIDAndPropagatesErrors(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusInternalServerError, http.StatusTemporaryRedirect} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/blob/one+two&three?four" || r.URL.RawQuery != "" {
					t.Errorf("path=%q query=%q", r.URL.Path, r.URL.RawQuery)
				}

				w.WriteHeader(status)
				fmt.Fprint(w, "archive")
			}))
			defer server.Close()
			var data bytes.Buffer
			exc := Try(func() {
				newStoreClient(server.URL).get(context.Background(), "one+two&three?four", &data)
			})

			if (exc == nil) != (status == http.StatusOK) {
				t.Fatalf("status=%d error=%v", status, exc)
			}

			if status == http.StatusOK && data.String() != "archive" {
				t.Fatalf("download=%q", data.String())
			}
		})
	}
}

func TestStoreClientPUTFailuresAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := newStoreClient(server.URL)
	file := Throw2(os.CreateTemp(t.TempDir(), "output"))
	defer file.Close()
	Throw2(file.WriteString("archive"))
	Throw2(file.Seek(0, io.SeekStart))

	if exc := Try(func() { client.put(context.Background(), "one", file) }); exc == nil {
		t.Fatal("failed upload must throw")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exc := Try(func() { client.get(ctx, "one", io.Discard) })

	if !errors.Is(exc.AsError(), context.Canceled) {
		t.Fatalf("cancellation error=%v", exc)
	}
}

func TestStoreClientRequiresExplicitEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "localhost:8064", "ftp://localhost", "http://", "http://host?query", "http://host#fragment"} {
		if exc := Try(func() { newStoreClient(endpoint) }); exc == nil {
			t.Fatalf("invalid endpoint %q accepted", endpoint)
		}
	}

	t.Setenv("MOLOT_STORE_ENDPOINT", "")
	cfg := &Config{StoreEndpoint: "http://configured"}
	overlayFromEnv(cfg)

	if cfg.StoreEndpoint != "" {
		t.Fatal("explicitly empty environment must not fall back to configuration")
	}
}

func TestExecutorUsesResolveAndDispatchWithoutS3Checks(t *testing.T) {
	work := t.TempDir()
	argsFile := filepath.Join(work, "args")
	taskFile := filepath.Join(work, "task")
	t.Setenv("TEST_GORN_ARGS", argsFile)
	t.Setenv("TEST_GORN_TASK", taskFile)
	gorn := filepath.Join(work, "gorn")
	Throw(os.WriteFile(gorn, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$TEST_GORN_ARGS\"\ncat > \"$TEST_GORN_TASK\"\n"), 0755))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	graph := &Graph{Nodes: []Node{{UID: "one", OutDirs: []string{"/ix/store/one-pkg"}}}}
	cfg := &Config{Resolve: server.URL, GornBin: gorn, StoreEndpoint: "http://worker-store", S3Root: "molot"}
	ex := newExecutor(graph, cfg, nil)

	// S3Cli is nil: either removed per-node HEAD would panic here.
	if ex.executeNode(&graph.Nodes[0]) || !ex.cache.Has("one") {
		t.Fatal("node did not complete")
	}

	args := string(Throw2(os.ReadFile(argsFile)))

	if !strings.Contains(args, "MOLOT_STORE_ENDPOINT=http://worker-store\n") {
		t.Fatalf("worker store endpoint not forwarded: %s", args)
	}

	Throw(os.Remove(argsFile))

	if ex.executeNode(&graph.Nodes[0]) {
		t.Fatal("cached node failed")
	}

	if _, err := os.Stat(argsFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cached node was dispatched: %v", err)
	}
}
