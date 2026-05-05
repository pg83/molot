package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sys/unix"
)

// InfraExitCode is the exit code molot exec uses when an infra phase
// (namespace setup, dep fetch, output push) fails. Paired with
// `gorn ignite --retry-error 100` so gorn classifies this exit as
// retriable, leaves the task on the queue, and skips writing the
// canonical result.json — next dispatch re-runs from scratch. Any
// other non-zero exit comes from the build script itself and stays
// non-retriable.
const InfraExitCode = 100

type ExecTask struct {
	UID     string    `json:"uid"`
	InDirs  []string  `json:"in_dirs"`
	OutDir  string    `json:"out_dir"`
	Cmds    []Cmd     `json:"cmds"`
	Net     bool      `json:"net,omitempty"`
	Predict []Predict `json:"predict,omitempty"`
}

func execMain(args []string) {
	if len(args) > 1 {
		ThrowFmt("usage: molot exec [task.json]   (or task JSON on stdin)")
	}

	task := readExecTask(args)
	cfg := loadS3Config()

	cwd := Throw2(os.Getwd())

	infraExc := Try(func() {
		setupNamespace(cwd, task)
		fetchDeps(cfg, cwd, task.InDirs)
	})

	if infraExc != nil {
		fmt.Fprintln(os.Stderr, "molot exec: infra (setup/fetch):", infraExc.Error())
		os.Exit(InfraExitCode)
	}

	Throw(os.Chdir("/"))

	scriptExit := runCmds(task)

	if scriptExit != 0 {
		fmt.Fprintln(os.Stderr, "molot exec: script exit", scriptExit)
		os.Exit(scriptExit)
	}

	verifyExc := Try(func() {
		verifyPredict(task)
	})

	if verifyExc != nil {
		fmt.Fprintln(os.Stderr, "molot exec: predict mismatch:", verifyExc.Error())
		os.Exit(1)
	}

	pushExc := Try(func() {
		pushOutput(cfg, cwd, task)
	})

	if pushExc != nil {
		fmt.Fprintln(os.Stderr, "molot exec: infra (push):", pushExc.Error())
		os.Exit(InfraExitCode)
	}
}

func readExecTask(args []string) ExecTask {
	if len(args) != 0 {
		ThrowFmt("usage: molot exec   (task JSON on stdin)")
	}

	raw := Throw2(io.ReadAll(os.Stdin))

	var t ExecTask
	Throw(json.Unmarshal(raw, &t))

	if t.UID == "" {
		ThrowFmt("exec: task.uid is empty")
	}

	if t.OutDir == "" {
		ThrowFmt("exec: task.out_dir is empty")
	}

	return t
}

// parseUIDFromStorePath strips the `/ix/store/<uid>-<name>` prefix
// down to <uid>. ExecTask.InDirs are these full paths; the S3 result.zstd
// object key is keyed by the bare uid.
func parseUIDFromStorePath(p string) string {
	base := filepath.Base(p)
	idx := strings.Index(base, "-")

	if idx <= 0 {
		ThrowFmt("cannot parse uid from store path %q", p)
	}

	return base[:idx]
}

// setupNamespace ports the mount choreography that wrap.sh.tmpl used to
// do via shell. Runs inside the user+mount namespace gorn-wrap already
// unshared (CAP_SYS_ADMIN granted there), which lets us mount tmpfs +
// overlayfs + rbinds without escalation. Order matters — opaque xattrs
// must be set on upper-paths BEFORE the overlay is mounted, since
// userxattr mode rejects user.overlay.* writes routed through the
// overlay itself.
func setupNamespace(cwd string, t ExecTask) {
	ovl := cwd + "/ovl"
	Throw(os.MkdirAll(ovl, 0755))
	Throw(unix.Mount("tmpfs", ovl, "tmpfs", 0, ""))

	upper := ovl + "/upper"
	work := ovl + "/work"
	Throw(os.MkdirAll(upper, 0755))
	Throw(os.MkdirAll(work, 0755))

	for _, in := range t.InDirs {
		markOpaque(filepath.Join(upper, filepath.Base(in)))
	}

	markOpaque(filepath.Join(upper, filepath.Base(t.OutDir)))

	overlayOpts := fmt.Sprintf("lowerdir=/ix/store,upperdir=%s,workdir=%s,userxattr", upper, work)
	Throw(unix.Mount("overlay", "/ix/store", "overlay", 0, overlayOpts))

	Throw(unix.Mount("tmpfs", "/ix/build", "tmpfs", 0, ""))

	Throw(unix.Mount("/proc", "/proc", "", uintptr(unix.MS_BIND|unix.MS_REC), ""))
	Throw(unix.Mount("/sys", "/sys", "", uintptr(unix.MS_BIND|unix.MS_REC), ""))
	Throw(unix.Mount("/dev", "/dev", "", uintptr(unix.MS_BIND|unix.MS_REC), ""))

	Throw(os.MkdirAll("/dev/shm", 0755))
	Throw(unix.Mount("tmpfs", "/dev/shm", "tmpfs", 0, ""))
}

func markOpaque(p string) {
	Throw(os.MkdirAll(p, 0755))
	Throw(unix.Setxattr(p, "user.overlay.opaque", []byte("y"), 0))
}

func fetchDeps(cfg *Config, cwd string, ins []string) {
	for i, in := range ins {
		uid := parseUIDFromStorePath(in)
		arch := fmt.Sprintf("%s/dep.%d.tar.zst", cwd, i)

		fmt.Fprintf(os.Stderr, "molot exec: fetch %s -> %s\n", uid, in)

		f := Throw2(os.Create(arch))

		resp := Throw2(cfg.S3Cli.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(cfg.ResultObjectKey(uid)),
		}))

		Throw2(io.Copy(f, resp.Body))
		Throw(resp.Body.Close())
		Throw(f.Close())

		untar := exec.Command("tar", "--use-compress-program=unzstd", "-xf", arch, "-C", in)
		untar.Stdout = os.Stdout
		untar.Stderr = os.Stderr
		Throw(untar.Run())

		Throw(os.Remove(arch))
	}
}

// runCmds runs the build cmds in sequence, returning the script exit
// code. 0 = all succeeded. Non-zero = a cmd's own exit code (build
// failure, propagated as-is). Throws on infra-style failures (cmd
// binary missing, fork-exec error) so the caller's Try classifies it
// as infra; ExitError shapes are treated as script exits.
func runCmds(t ExecTask) int {
	makeThrs := os.Getenv("MOLOT_CPUS")

	for i, c := range t.Cmds {
		if len(c.Args) == 0 {
			ThrowFmt("cmd %d: empty args", i)
		}

		fmt.Fprintf(os.Stderr, "molot exec: cmd %d/%d %v\n", i+1, len(t.Cmds), c.Args)

		env := []string{
			"IX_RANDOM=" + ixRandom(),
			"make_thrs=" + makeThrs,
		}

		for k, v := range c.Env {
			env = append(env, k+"="+v)
		}

		argv := c.Args

		if !t.Net {
			argv = append([]string{"/bin/unshare", "-r", "-n"}, argv...)
		}

		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Env = env
		cmd.Stdin = strings.NewReader(c.Stdin)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err := cmd.Run()

		if err == nil {
			continue
		}

		var ee *exec.ExitError

		if errors.As(err, &ee) {
			return ee.ExitCode()
		}

		Throw(err)
	}

	return 0
}

func ixRandom() string {
	var b [4]byte
	Throw2(rand.Read(b[:]))

	return strconv.FormatUint(uint64(binary.LittleEndian.Uint32(b[:])), 10)
}

func sha256File(path string) string {
	f := Throw2(os.Open(path))
	defer f.Close()

	h := sha256.New()
	Throw2(io.Copy(h, f))

	return hex.EncodeToString(h.Sum(nil))
}

func verifyPredict(t ExecTask) {
	for _, p := range t.Predict {
		actual := sha256File(p.Path)

		if actual != p.Sum {
			ThrowFmt("predict mismatch: %s expected=%s actual=%s", p.Path, p.Sum, actual)
		}
	}
}

func pushOutput(cfg *Config, cwd string, t ExecTask) {
	out := cwd + "/out.tar.zst"

	tar := exec.Command("tar", "--use-compress-program=zstd", "-cf", out, "-C", t.OutDir, ".")
	tar.Stdout = os.Stdout
	tar.Stderr = os.Stderr
	Throw(tar.Run())

	f := Throw2(os.Open(out))
	defer f.Close()

	Throw2(cfg.S3Cli.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(cfg.ResultObjectKey(t.UID)),
		Body:   f,
	}))

	fmt.Fprintf(os.Stderr, "molot exec: pushed %s\n", cfg.ResultObjectKey(t.UID))
}
