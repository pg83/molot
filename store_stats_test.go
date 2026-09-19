package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStoreResolvePersistsStats(t *testing.T) {
	if os.Getenv("MOLOT_STORE_TEST_CHILD") == "1" {
		var args []string
		Throw(json.Unmarshal([]byte(os.Getenv("MOLOT_STORE_TEST_ARGS")), &args))
		storeMain(args)

		return
	}

	chunks := make(chan []byte, 8)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/cix/complete":
			fmt.Fprint(w, "indexed\n")
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/molot/queue/"):
			body, err := io.ReadAll(r.Body)

			if err != nil {
				t.Error(err)
			}

			chunks <- body
		default:
			t.Errorf("unexpected S3 request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer backend.Close()
	listener := Throw2(net.Listen("tcp", "127.0.0.1:0"))
	address := listener.Addr().String()
	Throw(listener.Close())
	args := []string{"--listen", address, "--index-bucket", "cix", "--index-key", "complete", "--index-ttl", "1h",
		"--kv-endpoint", "http://127.0.0.1:1", "--kv-bucket", "molot", "--kv-timeout", "1s"}
	encoded := Throw2(json.Marshal(args))
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreResolvePersistsStats$")
	cmd.Env = append(os.Environ(), "MOLOT_STORE_TEST_CHILD=1", "MOLOT_STORE_TEST_ARGS="+string(encoded),
		"S3_ENDPOINT="+backend.URL, "S3_BUCKET=molot", "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test",
		"AWS_REGION=us-east-1", "MOLOT_S3_ROOT=molot", "TMPDIR="+t.TempDir())
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	Throw(cmd.Start())
	done := make(chan error, 1)

	go func() {
		done <- cmd.Wait()
	}()

	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("store process: %v\n%s", err, logs.String())
			}
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done
			t.Errorf("store shutdown timed out\n%s", logs.String())
		}
	})

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)

	for {
		response, err := client.Get("http://" + address + "/v1/resolve")

		if err == nil {
			response.Body.Close()

			if response.StatusCode == http.StatusMethodNotAllowed {
				break
			}
		}

		if time.Now().After(deadline) {
			t.Fatal("store failed to start")
		}

		time.Sleep(10 * time.Millisecond)
	}

	for _, version := range []string{"v1", "v2"} {
		response := Throw2(client.Post("http://"+address+"/"+version+"/resolve", "application/json", strings.NewReader(`["indexed","absent","indexed"]`)))
		io.Copy(io.Discard, response.Body)
		response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s resolve: HTTP %d", version, response.StatusCode)
		}
	}

	var recorded []string

	for len(recorded) < 6 {
		select {
		case body := <-chunks:
			decoder := json.NewDecoder(bytes.NewReader(body))

			for decoder.More() {
				var uid string
				Throw(decoder.Decode(&uid))
				recorded = append(recorded, uid)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("resolve stats not persisted to MinIO: %v", recorded)
		}
	}

	if !reflect.DeepEqual(recorded, []string{"indexed", "absent", "indexed", "indexed", "absent", "indexed"}) {
		t.Fatalf("persisted stats=%v", recorded)
	}
}
