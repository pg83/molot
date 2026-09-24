package main

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func makeTree(t *testing.T) string {
	t.Helper()

	src := t.TempDir()
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

	if err := os.MkdirAll(filepath.Join(src, "bin"), 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Join(src, "share", "empty"), 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(src, "bin", "tool"), []byte("#!/bin/sh\necho hi\n"), 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(src, "env"), []byte("export X=1\n"), 0444); err != nil {
		t.Fatal(err)
	}

	if err := os.Link(filepath.Join(src, "bin", "tool"), filepath.Join(src, "bin", "tool-alias")); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink("tool", filepath.Join(src, "bin", "link")); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink("/ix/store/abc-bin-zstd/bin/zstd", filepath.Join(src, "bin", "zstd")); err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{"bin/tool", "env", "share/empty", "share", "bin"} {
		if err := os.Chtimes(filepath.Join(src, p), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}

	return src
}

func lstat(t *testing.T, path string) os.FileInfo {
	t.Helper()

	info, err := os.Lstat(path)

	if err != nil {
		t.Fatal(err)
	}

	return info
}

func checkTree(t *testing.T, src, dst string) {
	t.Helper()

	for _, rel := range []string{"bin", "bin/tool", "bin/tool-alias", "env", "share", "share/empty"} {
		want := lstat(t, filepath.Join(src, rel))
		got := lstat(t, filepath.Join(dst, rel))

		if got.Mode() != want.Mode() {
			t.Fatalf("%s: mode %v, want %v", rel, got.Mode(), want.Mode())
		}

		if !got.ModTime().Equal(want.ModTime()) {
			t.Fatalf("%s: mtime %v, want %v", rel, got.ModTime(), want.ModTime())
		}

		if got.Size() != want.Size() {
			t.Fatalf("%s: size %d, want %d", rel, got.Size(), want.Size())
		}
	}

	for _, rel := range []string{"bin/link", "bin/zstd"} {
		want, _ := os.Readlink(filepath.Join(src, rel))
		got, err := os.Readlink(filepath.Join(dst, rel))

		if err != nil || got != want {
			t.Fatalf("%s: symlink %q (%v), want %q", rel, got, err, want)
		}
	}

	data, err := os.ReadFile(filepath.Join(dst, "bin", "tool"))

	if err != nil || string(data) != "#!/bin/sh\necho hi\n" {
		t.Fatalf("bin/tool content %q (%v)", data, err)
	}

	tool := lstat(t, filepath.Join(dst, "bin", "tool")).Sys().(*syscall.Stat_t)
	alias := lstat(t, filepath.Join(dst, "bin", "tool-alias")).Sys().(*syscall.Stat_t)

	if tool.Ino != alias.Ino {
		t.Fatalf("bin/tool-alias is not a hard link of bin/tool")
	}
}

func TestArchiveRoundTrip(t *testing.T) {
	src := makeTree(t)
	var buf bytes.Buffer

	createArchive(&buf, src)

	dst := t.TempDir()
	extractArchive(&buf, dst)
	checkTree(t, src, dst)
}

func TestExtractArchiveReadsGnuTar(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("no tar on PATH")
	}

	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("no zstd on PATH")
	}

	src := makeTree(t)
	arch := filepath.Join(t.TempDir(), "dep.tar.zst")
	cmd := exec.Command("tar", "--use-compress-program=zstd", "-cf", arch, "-C", src, ".")

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tar: %v\n%s", err, out)
	}

	f, err := os.Open(arch)

	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	dst := t.TempDir()
	extractArchive(f, dst)
	checkTree(t, src, dst)
}

func TestCreateArchiveReadableByGnuTar(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("no tar on PATH")
	}

	if _, err := exec.LookPath("unzstd"); err != nil {
		t.Skip("no unzstd on PATH")
	}

	src := makeTree(t)
	arch := filepath.Join(t.TempDir(), "out.tar.zst")
	f, err := os.Create(arch)

	if err != nil {
		t.Fatal(err)
	}

	createArchive(f, src)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	cmd := exec.Command("tar", "--use-compress-program=unzstd", "-xf", arch, "-C", dst)

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tar: %v\n%s", err, out)
	}

	checkTree(t, src, dst)
}

func TestExtractArchiveRejectsEscape(t *testing.T) {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf)

	if err != nil {
		t.Fatal(err)
	}

	writer := tar.NewWriter(encoder)
	body := []byte("pwned\n")
	header := &tar.Header{Name: "../evil", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg}

	if err := writer.WriteHeader(header); err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}

	parent := t.TempDir()
	dst := filepath.Join(parent, "out")

	if err := os.Mkdir(dst, 0755); err != nil {
		t.Fatal(err)
	}

	exc := Try(func() {
		extractArchive(&buf, dst)
	})

	if exc == nil || !strings.Contains(exc.Error(), "escapes output") {
		t.Fatalf("escape not rejected: %v", exc)
	}

	if _, err := os.Stat(filepath.Join(parent, "evil")); !os.IsNotExist(err) {
		t.Fatalf("file written outside output dir: %v", err)
	}
}
