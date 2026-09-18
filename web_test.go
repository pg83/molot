package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebNodeStreamExceptionBoundary(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, "node output")
			}))
			defer upstream.Close()

			srv := &webSrv{cfg: &Config{GornAPI: upstream.URL, S3Root: "molot"}, http: upstream.Client()}
			res := httptest.NewRecorder()
			srv.handleNodeStream(res, httptest.NewRequest(http.MethodGet, "/node/uid/stderr", nil))

			if res.Code != status {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}

			if status == http.StatusOK && res.Body.String() != "node output" {
				t.Fatalf("body=%q", res.Body.String())
			}
		})
	}
}
