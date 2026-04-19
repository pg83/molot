package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
)

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
	var b strings.Builder

	b.WriteString("#!/bin/sh\nset -eu\n\n")
	b.WriteString(`: "${AWS_ACCESS_KEY_ID:?}"` + "\n")
	b.WriteString(`: "${AWS_SECRET_ACCESS_KEY:?}"` + "\n")
	b.WriteString(`: "${S3_ENDPOINT:?}"` + "\n")
	b.WriteString(`: "${S3_BUCKET:?}"` + "\n\n")

	b.WriteString(`T=$(mktemp -d -t molot.XXXXXXXX)` + "\n")
	b.WriteString(`trap 'rm -rf "$T"' EXIT` + "\n")
	b.WriteString(`export T` + "\n\n")

	writeDepFetches(&b, n)
	writeOutDirScaffold(&b, n)
	writeInnerScript(&b, n)
	writeUnshareInvocation(&b)
	writeResultUpload(&b, n)

	return b.String()
}

func writeOutDirScaffold(b *strings.Builder, n *Node) {
	fmt.Fprintf(b, "mkdir -p %s\n\n", shT(n.OutDirs[0]))
}

func writeDepFetches(b *strings.Builder, n *Node) {
	for i, in := range n.InDirs {
		depUID := parseUIDFromStorePath(in)
		archive := shT(fmt.Sprintf("/dep.%d.tar.zst", i))
		s3key := shS3(fmt.Sprintf("/gorn/%s/result.zstd", depUID))

		fmt.Fprintf(b, "mkdir -p %s\n", shT(in))
		fmt.Fprintf(b, "aws s3 cp --endpoint-url \"$S3_ENDPOINT\" %s %s\n", s3key, archive)
		fmt.Fprintf(b, "tar --use-compress-program=unzstd -xf %s -C %s\n", archive, shT(in))
		fmt.Fprintf(b, "rm -f %s\n", archive)
	}

	b.WriteString("\n")
}

func writeInnerScript(b *strings.Builder, n *Node) {
	b.WriteString(`cat > "$T/inner.sh" <<'MOLOT_INNER_EOF'` + "\n")
	b.WriteString("#!/bin/sh\nset -eu\n")
	b.WriteString(`mount --bind "$T/ix" /ix` + "\n")
	b.WriteString("cd /\n\n")

	for i, c := range n.Cmds {
		fmt.Fprintf(b, "# cmd %d\n", i)
		writeOneCmd(b, &c)
	}

	b.WriteString("MOLOT_INNER_EOF\n")
	b.WriteString(`chmod +x "$T/inner.sh"` + "\n\n")
}

func writeOneCmd(b *strings.Builder, c *Cmd) {
	if len(c.Args) == 0 {
		ThrowFmt("cmd with empty args")
	}

	stdinB64 := base64.StdEncoding.EncodeToString([]byte(c.Stdin))

	envParts := []string{"env", "-i"}

	for k, v := range c.Env {
		envParts = append(envParts, shQuote(k+"="+v))
	}

	for _, a := range c.Args {
		envParts = append(envParts, shQuote(a))
	}

	fmt.Fprintf(b, "printf %%s %s | base64 -d | %s\n",
		shQuote(stdinB64),
		strings.Join(envParts, " "),
	)
}

func writeUnshareInvocation(b *strings.Builder) {
	b.WriteString(`unshare -r -U -m "$T/inner.sh"` + "\n\n")
}

func writeResultUpload(b *strings.Builder, n *Node) {
	out := n.OutDirs[0]
	archive := shT("/out.tar.zst")
	s3key := shS3(fmt.Sprintf("/gorn/%s/result.zstd", n.UID))

	fmt.Fprintf(b, "tar --use-compress-program=zstd -cf %s -C %s .\n", archive, shT(out))
	fmt.Fprintf(b, "aws s3 cp --endpoint-url \"$S3_ENDPOINT\" %s %s\n", archive, s3key)
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

// shS3 emits `"s3://$S3_BUCKET"'<suffix>'`.
func shS3(suffix string) string {
	return `"s3://$S3_BUCKET"` + shQuote(suffix)
}

func parseUIDFromStorePath(p string) string {
	base := path.Base(p)
	idx := strings.Index(base, "-")

	if idx <= 0 {
		ThrowFmt("cannot parse uid from store path %q", p)
	}

	return base[:idx]
}
