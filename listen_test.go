package main

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	addr := l.Addr().String()
	l.Close()

	return addr
}

func TestListenIsRepeatable(t *testing.T) {
	var listens listenAddrs

	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Var(&listens, "listen", "")

	if err := fs.Parse([]string{"--listen", "127.0.0.1:8064", "--listen", "192.168.103.16:8064"}); err != nil {
		t.Fatal(err)
	}

	if got := listens.String(); got != "127.0.0.1:8064,192.168.103.16:8064" {
		t.Fatalf("listens=%q", got)
	}
}

func TestServeAllServesEveryAddressUntilAllShutDown(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "pong")
	})

	servers := []*http.Server{{Addr: freeAddr(t), Handler: mux}, {Addr: freeAddr(t), Handler: mux}}
	done := make(chan *Exception, 1)

	go func() {
		done <- Try(func() { serveAll(servers) })
	}()

	for _, server := range servers {
		var body string

		// The listeners come up asynchronously; poll until each answers.
		for i := 0; i < 200 && body == ""; i++ {
			resp, err := http.Get("http://" + server.Addr + "/ping")

			if err != nil {
				time.Sleep(10 * time.Millisecond)

				continue
			}

			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(b)
		}

		if body != "pong" {
			t.Fatalf("%s: body=%q", server.Addr, body)
		}
	}

	servers[0].Shutdown(context.Background())

	select {
	case exc := <-done:
		t.Fatalf("serveAll returned with one server still up: %v", exc)
	case <-time.After(100 * time.Millisecond):
	}

	servers[1].Shutdown(context.Background())

	if exc := <-done; exc != nil {
		t.Fatalf("serveAll after shutdown: %v", exc)
	}
}

func TestServeAllThrowsOnBusyAddress(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	defer busy.Close()

	exc := Try(func() {
		serveAll([]*http.Server{{Addr: busy.Addr().String(), Handler: http.NewServeMux()}})
	})

	if exc == nil || !strings.Contains(exc.Error(), "address already in use") {
		t.Fatalf("exception=%v", exc)
	}
}
