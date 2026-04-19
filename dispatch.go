package main

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"text/template"
)

//go:embed wrap.sh.tmpl
var wrapTmplSrc string

var wrapTmpl = template.Must(template.New("wrap").Funcs(template.FuncMap{
	"shT":      shT,
	"archT":    func(i int) string { return shT(fmt.Sprintf("/dep.%d.tar.zst", i)) },
	"outT":     func() string { return shT("/out.tar.zst") },
	"depS3":    func(in string) string { return shS3(fmt.Sprintf("/gorn/%s/result.zstd", parseUIDFromStorePath(in))) },
	"selfS3":   func(uid string) string { return shS3(fmt.Sprintf("/gorn/%s/result.zstd", uid)) },
	"stdinB64": func(c Cmd) string { return shQuote(base64.StdEncoding.EncodeToString([]byte(c.Stdin))) },
	"cmdLine":  cmdLine,
}).Parse(wrapTmplSrc))

type wrapCtx struct {
	UID    string
	InDirs []string
	Out    string
	Cmds   []Cmd
}

func dispatchNode(ex *Executor, n *Node) {
	script := buildWrapScript(n)

	if os.Getenv("MOLOT_DUMP") != "" {
		fmt.Fprintf(os.Stderr, "---- molot wrap script for %s ----\n%s---- end ----\n", n.UID, script)
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(script))
	payload := fmt.Sprintf("echo %s | base64 -d | sh", encoded)

	args := []string{
		"ignite",
		"--wait",
		"--guid", n.UID,
		"--api", ex.cfg.GornAPI,
		"--env", "AWS_ACCESS_KEY_ID=" + ex.cfg.AWSKey,
		"--env", "AWS_SECRET_ACCESS_KEY=" + ex.cfg.AWSSecret,
		"--env", "AWS_REGION=" + ex.cfg.AWSRegion,
		"--env", "S3_ENDPOINT=" + ex.cfg.S3Endpt,
		"--env", "S3_BUCKET=" + ex.cfg.S3Bucket,
		"--",
		"sh", "-c", payload,
	}

	cmd := exec.Command(ex.cfg.GornBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()

	if err != nil {
		ThrowFmt("node %s (out=%s) failed via gorn ignite: %v", n.UID, n.OutDirs[0], err)
	}
}

func buildWrapScript(n *Node) string {
	ctx := wrapCtx{
		UID:    n.UID,
		InDirs: n.InDirs,
		Out:    n.OutDirs[0],
		Cmds:   n.Cmds,
	}

	var buf strings.Builder
	Throw(wrapTmpl.Execute(&buf, ctx))

	return buf.String()
}

func cmdLine(c Cmd) string {
	if len(c.Args) == 0 {
		ThrowFmt("cmd with empty args")
	}

	parts := []string{"env", "-i"}

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
