package main

import (
	"errors"
	"testing"
	"time"
)

func TestFailNodeExitsByDefault(t *testing.T) {
	exitCode := 0
	ex := &Executor{
		cfg: &Config{},
		exit: func(code int) {
			exitCode = code
		},
	}
	rec := NodeRec{UID: "broken", Out: "/ix/store/broken", StartedAt: time.Now(), FinishedAt: time.Now()}

	if !ex.failNode(rec, rec.Out, New(errors.New("boom"))) {
		t.Fatal("failed node must propagate failure")
	}

	if exitCode != 2 {
		t.Fatalf("default failure exit code = %d, want 2", exitCode)
	}
}

func TestFailNodeKeepsGoingWithIXEnv(t *testing.T) {
	exited := false
	ex := &Executor{
		cfg: &Config{KeepGoing: true},
		exit: func(int) {
			exited = true
		},
	}
	rec := NodeRec{UID: "broken", Out: "/ix/store/broken", StartedAt: time.Now(), FinishedAt: time.Now()}

	if !ex.failNode(rec, rec.Out, New(errors.New("boom"))) {
		t.Fatal("failed node must propagate failure")
	}

	if exited {
		t.Fatal("IX_KEEP_GOING=yes must keep the executor alive after a node failure")
	}
}
