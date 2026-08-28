package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFailNodeCancelsByDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ex := &Executor{
		cfg:    &Config{},
		ctx:    ctx,
		cancel: cancel,
	}
	rec := NodeRec{UID: "broken", Out: "/ix/store/broken", StartedAt: time.Now(), FinishedAt: time.Now()}

	if !ex.failNode(rec, rec.Out, New(errors.New("boom"))) {
		t.Fatal("failed node must propagate failure")
	}

	if ex.ctx.Err() != context.Canceled {
		t.Fatalf("default failure did not cancel executor: %v", ex.ctx.Err())
	}
}

func TestFailNodeKeepsGoingOnlyWithFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ex := &Executor{
		cfg:    &Config{KeepGoing: true},
		ctx:    ctx,
		cancel: cancel,
	}
	rec := NodeRec{UID: "broken", Out: "/ix/store/broken", StartedAt: time.Now(), FinishedAt: time.Now()}

	if !ex.failNode(rec, rec.Out, New(errors.New("boom"))) {
		t.Fatal("failed node must propagate failure")
	}

	if ex.ctx.Err() != nil {
		t.Fatalf("-k canceled executor: %v", ex.ctx.Err())
	}
}
