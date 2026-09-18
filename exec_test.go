package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRunCmdsPreservesScriptExitAndStops(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "unexpected")
	task := ExecTask{
		Net: true,
		Cmds: []Cmd{
			{Args: []string{"/bin/sh", "-c", "exit 7"}},
			{Args: []string{"/bin/sh", "-c", `echo ran > "$1"`, "sh", marker}},
		},
	}

	if code := runCmds(task); code != 7 {
		t.Fatalf("script exit=%d, want 7", code)
	}

	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("command after failure ran: %v", err)
	}
}

func TestRunCmdsThrowsSpawnFailure(t *testing.T) {
	task := ExecTask{
		Net:  true,
		Cmds: []Cmd{{Args: []string{filepath.Join(t.TempDir(), "missing-command")}}},
	}
	exc := Try(func() {
		runCmds(task)
	})

	if !errors.Is(exc.AsError(), os.ErrNotExist) {
		t.Fatalf("exception=%v, want spawn failure", exc)
	}
}
