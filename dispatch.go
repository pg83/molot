package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"text/template"
	"time"
)

//go:embed wrap.sh.tmpl
var wrapTmplSrc string

// guidPrefix scopes molot's gorn GUIDs into their own namespace AND binds them
// to the wrap-script template hash. Any change to wrap.sh.tmpl shifts the
// prefix, which in turn invalidates every cached gorn result (idempotency
// key = GUID = prefix+uid). This is the CAS contract: node uid alone is
// insufficient — the executor's side of the contract (download/mount/run/
// upload mechanics) is equally part of what determines correctness.
var guidPrefix = func() string {
	h := sha256.Sum256([]byte(wrapTmplSrc))

	return "molot-" + hex.EncodeToString(h[:])[:12] + "-"
}()

func gornGUID(uid string) string {
	return guidPrefix + uid
}

var wrapTmpl = template.Must(template.New("wrap").Funcs(template.FuncMap{
	"shT":      shT,
	"shStore":  shQuote,
	"shUpper":  shUpper,
	"archT":    func(i int) string { return shT(fmt.Sprintf("/dep.%d.tar.zst", i)) },
	"outT":     func() string { return shT("/out.tar.zst") },
	"depS3":    func(in string) string { return shS3(fmt.Sprintf("/gorn/%s/result.zstd", gornGUID(parseUIDFromStorePath(in)))) },
	"selfS3":   func(uid string) string { return shS3(fmt.Sprintf("/gorn/%s/result.zstd", gornGUID(uid))) },
	"stdinB64": func(c Cmd) string { return shQuote(base64.StdEncoding.EncodeToString([]byte(c.Stdin))) },
	"cmdLine":  cmdLine,
}).Parse(wrapTmplSrc))

type wrapCtx struct {
	UID     string
	InDirs  []string
	Out     string
	Cmds    []Cmd
	Threads int
}

func dispatchNode(ex *Executor, n *Node) {
	script := buildWrapScript(ex, n)

	if os.Getenv("MOLOT_DUMP") != "" {
		fmt.Fprintf(os.Stderr, "---- molot wrap script for %s ----\n%s---- end ----\n", n.UID, script)
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(script))
	payload := fmt.Sprintf("echo %s | base64 -d | sh", encoded)

	args := []string{
		"ignite",
		"--wait",
		"--guid", gornGUID(n.UID),
		"--api", ex.cfg.GornAPI,
		"--env", "AWS_ACCESS_KEY_ID=" + ex.cfg.AWSKey,
		"--env", "AWS_SECRET_ACCESS_KEY=" + ex.cfg.AWSSecret,
		"--env", "AWS_REGION=" + ex.cfg.AWSRegion,
		"--env", "S3_ENDPOINT=" + ex.cfg.S3Endpt,
		"--env", "S3_BUCKET=" + ex.cfg.S3Bucket,
		"--",
		"sh", "-c", payload,
	}

	quiet := os.Getenv("MOLOT_QUIET") != ""

	delay := time.Second
	const maxDelay = 60 * time.Second

	for {
		cmd := exec.Command(ex.cfg.GornBin, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

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
			return
		}

		// ProcessState nil means the subprocess never started (fork/exec
		// failed — "too many open files", ENOMEM, transient spawn errors).
		// Retry with exp backoff. Real task failures have ProcessState set
		// and propagate as a regular fail.
		if cmd.ProcessState == nil {
			// [delay/2, 3*delay/2) jitter so many concurrent retriers
			// don't rethunder in lock-step.
			sleep := delay/2 + time.Duration(rand.Int64N(int64(delay)))

			fmt.Fprintf(os.Stderr, "molot: node %s: spawn error (%v), retrying in %v\n", n.UID, err, sleep)
			time.Sleep(sleep)

			delay *= 2

			if delay > maxDelay {
				delay = maxDelay
			}

			continue
		}

		if quiet {
			fmt.Fprintf(os.Stderr, "---- molot: stdout of failed node %s ----\n", n.UID)
			_, _ = os.Stderr.Write(stdout.Bytes())
			fmt.Fprintf(os.Stderr, "---- molot: stderr of failed node %s ----\n", n.UID)
			_, _ = os.Stderr.Write(stderr.Bytes())
			fmt.Fprintf(os.Stderr, "---- end ----\n")
		}

		ThrowFmt("node %s (out=%s) failed via gorn ignite: %v", n.UID, n.OutDirs[0], err)
	}
}

func buildWrapScript(ex *Executor, n *Node) string {
	ctx := wrapCtx{
		UID:     n.UID,
		InDirs:  n.InDirs,
		Out:     n.OutDirs[0],
		Cmds:    n.Cmds,
		Threads: ex.threads(),
	}

	var buf strings.Builder
	Throw(wrapTmpl.Execute(&buf, ctx))

	return buf.String()
}

func cmdLine(c Cmd) string {
	if len(c.Args) == 0 {
		ThrowFmt("cmd with empty args")
	}

	// IX_RANDOM / make_thrs: stock assemble.go injects these on every cmd
	// (see as.go:env). IX_RANDOM is used by fast_rm for trash-dir suffixes;
	// make_thrs picks parallelism for make. Computed at cmd invocation so
	// IX_RANDOM differs per cmd like as.go does.
	parts := []string{
		"env", "-i",
		`"IX_RANDOM=$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')"`,
		`"make_thrs=$MOLOT_MAKE_THRS"`,
	}

	for k, v := range c.Env {
		parts = append(parts, shQuote(k+"="+v))
	}

	for _, a := range c.Args {
		parts = append(parts, shQuote(a))
	}

	return strings.Join(parts, " ")
}

func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shT emits `"$T"'<suffix>'` — single-quoted literal concatenated after the
// expanded $T. Needed because shQuote alone would single-quote $T itself,
// suppressing the expansion.
func shT(suffix string) string {
	return `"$T"` + shQuote(suffix)
}

// shUpper translates an absolute /ix/store/<uid>-<name> path into the
// corresponding path inside the overlay upper dir, i.e.
// "$T"'/ovl/upper/<uid>-<name>'. Used to address the upper layer directly
// (before overlay is mounted, or for operations overlay forbids through
// the mount such as user.overlay.* xattr writes in userxattr mode).
func shUpper(abs string) string {
	const prefix = "/ix/store/"

	if !strings.HasPrefix(abs, prefix) {
		ThrowFmt("shUpper: path %q does not start with %s", abs, prefix)
	}

	return `"$T"` + shQuote("/ovl/upper/"+strings.TrimPrefix(abs, prefix))
}


// shS3 emits `"molot/$S3_BUCKET"'<suffix>'` — a minio-client path using the
// `molot` alias that the wrap script sets via MC_HOST_molot.
func shS3(suffix string) string {
	return `"molot/$S3_BUCKET"` + shQuote(suffix)
}

func parseUIDFromStorePath(p string) string {
	base := path.Base(p)
	idx := strings.Index(base, "-")

	if idx <= 0 {
		ThrowFmt("cannot parse uid from store path %q", p)
	}

	return base[:idx]
}
