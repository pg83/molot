package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// protocolVersion is the wire-protocol marker for `molot exec`. IX's
// ops_molot.py mixes `molot hash` (sha256 of this string, first 12 hex)
// into every uid through die/sh0.sh's `{{molot}}` comment. Bump this
// string on any breaking change to the ExecTask schema, exit-code
// semantics, mount layout, or fetch/push contract — the bump
// invalidates every cached artifact in the fleet, mirroring the role
// the wrap.sh.tmpl hash used to play before the shell wrapper went away.
const protocolVersion = "v2.exec.json.stdin.predict"

var tmplHash = func() string {
	h := sha256.Sum256([]byte(protocolVersion))

	return hex.EncodeToString(h[:])[:12]
}()

func dispatchNode(ex *Executor, n *Node) {
	taskJSON := buildExecJSON(n)

	if ex.cfg.Dump {
		fmt.Fprintf(os.Stderr, "---- task json for %s ----\n%s\n---- end ----\n", n.OutDirs[0], taskJSON)
	}

	slots := 1

	if n.Pool == "full" {
		slots = ex.cfg.FullSlots
	}

	args := []string{
		"ignite",
		"--wait",
		"--guid", n.UID,
		"--descr", n.OutDirs[0],
		"--api", ex.cfg.GornAPI,
		"--root", ex.cfg.S3Root,
		"--retry-error", strconv.Itoa(InfraExitCode),
		"--slots", strconv.Itoa(slots),
		"--env", "AWS_ACCESS_KEY_ID=" + ex.cfg.AWSKey,
		"--env", "AWS_SECRET_ACCESS_KEY=" + ex.cfg.AWSSecret,
		"--env", "AWS_REGION=" + ex.cfg.AWSRegion,
		"--env", "S3_ENDPOINT=" + ex.cfg.S3Endpt,
		"--env", "S3_BUCKET=" + ex.cfg.S3Bucket,
		"--env", "MOLOT_S3_ROOT=" + ex.cfg.S3Root,
		"--", "molot", "exec",
	}

	quiet := ex.cfg.Quiet

	delay := time.Second
	const maxDelay = 60 * time.Second

	for {
		cmd := exec.Command(ex.cfg.GornBin, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

		// gorn ignite in `-- argv` mode reads its own stdin and embeds
		// it as the worker-side cmd's stdin (base64'd inside the
		// synthesized script body — survives ARG_MAX). Worker's
		// `molot exec` reads the JSON off its stdin and runs the task.
		cmd.Stdin = strings.NewReader(taskJSON)

		var stdout, stderr bytes.Buffer

		if quiet {
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
		} else {
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
		}

		err := cmd.Run()

		if err == nil {
			verifyResult(ex, n)

			return
		}

		// ProcessState nil means the subprocess never started (fork/exec
		// failed — "too many open files", ENOMEM, transient spawn errors).
		// Retry with exp backoff. Real task failures have ProcessState set
		// and propagate as a regular fail.
		//
		// E2BIG ("argument list too long") is deterministic given the same
		// argv, so bail immediately instead of hammering pointlessly.
		if cmd.ProcessState == nil {
			if errors.Is(err, syscall.E2BIG) {
				ThrowFmt("node %s: spawn refused with E2BIG (argv too large for the kernel): %v", n.UID, err)
			}

			// [delay/2, 3*delay/2) jitter so many concurrent retriers
			// don't rethunder in lock-step.
			sleep := delay/2 + time.Duration(rand.Int64N(int64(delay)))

			fmt.Fprintln(os.Stderr, clr(clrY, fmt.Sprintf("%s: spawn error (%v), retrying in %v", n.OutDirs[0], err, sleep)))
			time.Sleep(sleep)

			delay *= 2

			if delay > maxDelay {
				delay = maxDelay
			}

			continue
		}

		if quiet {
			fmt.Fprintln(os.Stderr, clr(clrR, "---- stdout of failed node "+n.OutDirs[0]+" ----"))
			_, _ = os.Stderr.Write(stdout.Bytes())
			fmt.Fprintln(os.Stderr, clr(clrR, "---- stderr of failed node "+n.OutDirs[0]+" ----"))
			_, _ = os.Stderr.Write(stderr.Bytes())
			fmt.Fprintln(os.Stderr, clr(clrR, "---- end ----"))
		}

		ThrowFmt("node %s (out=%s) failed via gorn ignite: %v", n.UID, n.OutDirs[0], err)
	}
}

// verifyResult HEAD-checks that the producer actually uploaded
// result.zstd for this node. gorn considers a task "done" the moment
// result.json lands — but the tar+upload happens *after* that in
// `molot exec`, so a kill/OOM/disk-full between exit and upload leaves
// gorn happy (exit=0, result.json present) with no artifact. Downstream
// nodes then fail when they try to pull the missing dep, hundreds of
// lines away from the real cause. Failing here, on the producer itself,
// makes the root cause obvious.
func verifyResult(ex *Executor, n *Node) {
	key := ex.cfg.ResultObjectKey(n.UID)

	if s3StatExists(context.Background(), ex.cfg.S3Cli, ex.cfg.S3Bucket, key) {
		return
	}

	ThrowFmt("node %s (out=%s): gorn reported success but result.zstd is missing at s3://%s/%s",
		n.UID, n.OutDirs[0], ex.cfg.S3Bucket, key)
}

func buildExecJSON(n *Node) string {
	et := ExecTask{
		UID:     n.UID,
		InDirs:  n.InDirs,
		OutDir:  n.OutDirs[0],
		Cmds:    n.Cmds,
		Net:     n.Pool == "network",
		Predict: n.Predict,
	}

	data := Throw2(json.Marshal(et))

	return string(data)
}
