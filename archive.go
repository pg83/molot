package main

import (
	"archive/tar"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Dependency blobs and node outputs travel as tar+zstd. Both directions
// are done in-process: exec runs inside a namespace where the overlay
// hides every in_dir until it is fetched, and the host's tar/unzstd are
// symlinks into /ix/store — if a node depends on the very zstd or tar
// build the host realm uses, they vanish before the first fetch. Mirrors
// extractArchive in assemble's package_cache.go.

type dirMetadata struct {
	path    string
	mode    os.FileMode
	modTime time.Time
}

func tarMode(mode int64) os.FileMode {
	result := os.FileMode(mode & 0777)

	if mode&04000 != 0 {
		result |= os.ModeSetuid
	}

	if mode&02000 != 0 {
		result |= os.ModeSetgid
	}

	if mode&01000 != 0 {
		result |= os.ModeSticky
	}

	return result
}

func archivePath(root, name string) string {
	rel := filepath.Clean(filepath.FromSlash(name))

	if rel == "." {
		return root
	}

	if filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		ThrowFmt("archive path escapes output: %q", name)
	}

	current := root

	for _, part := range strings.Split(filepath.Dir(rel), string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}

		current = filepath.Join(current, part)
		info, err := os.Lstat(current)

		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			ThrowFmt("archive path traverses symlink: %q", name)
		}

		if err != nil && !os.IsNotExist(err) {
			Throw(err)
		}
	}

	return filepath.Join(root, rel)
}

func ensureParent(path string) {
	Throw(os.MkdirAll(filepath.Dir(path), 0755))
}

func rejectSymlink(path string) {
	info, err := os.Lstat(path)

	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		ThrowFmt("archive entry replaces symlink: %q", path)
	}

	if err != nil && !os.IsNotExist(err) {
		Throw(err)
	}
}

// extractArchive unpacks a tar+zstd stream into outDir, which must exist.
func extractArchive(input io.Reader, outDir string) {
	decoder := Throw2(zstd.NewReader(input))
	defer decoder.Close()

	reader := tar.NewReader(decoder)
	var dirs []dirMetadata

	for {
		header, err := reader.Next()

		if errors.Is(err, io.EOF) {
			break
		}

		Throw(err)

		target := archivePath(outDir, header.Name)
		mode := tarMode(header.Mode)

		switch header.Typeflag {
		case tar.TypeDir:
			Throw(os.MkdirAll(target, 0755))
			dirs = append(dirs, dirMetadata{path: target, mode: mode, modTime: header.ModTime})
		case tar.TypeReg, tar.TypeRegA:
			ensureParent(target)
			rejectSymlink(target)
			file := Throw2(os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode))
			_, copyErr := io.CopyN(file, reader, header.Size)
			closeErr := file.Close()
			Throw(copyErr)
			Throw(closeErr)
			Throw(os.Chmod(target, mode))
			Throw(os.Chtimes(target, header.ModTime, header.ModTime))
		case tar.TypeSymlink:
			ensureParent(target)
			Throw(os.Symlink(header.Linkname, target))
		case tar.TypeLink:
			ensureParent(target)
			linkTarget := archivePath(outDir, header.Linkname)
			Throw(os.Link(linkTarget, target))
		case tar.TypeFifo:
			ensureParent(target)
			Throw(syscall.Mkfifo(target, uint32(header.Mode&0777)))
		case tar.TypeXHeader, tar.TypeXGlobalHeader, tar.TypeGNULongName, tar.TypeGNULongLink:
			continue
		default:
			ThrowFmt("archive: unsupported tar entry type %d for %q", header.Typeflag, header.Name)
		}
	}

	for i := len(dirs) - 1; i >= 0; i-- {
		Throw(os.Chmod(dirs[i].path, dirs[i].mode))
		Throw(os.Chtimes(dirs[i].path, dirs[i].modTime, dirs[i].modTime))
	}

	_, err := io.Copy(io.Discard, decoder)
	Throw(err)
}

type inode struct {
	dev uint64
	ino uint64
}

// createArchive writes srcDir as a tar+zstd stream, entries named
// relative to srcDir with a leading "./" the way `tar -C dir .` does.
// Regular files with several names inside srcDir are stored once and
// then as hard links, so the layout survives the round trip.
func createArchive(output io.Writer, srcDir string) {
	encoder := Throw2(zstd.NewWriter(output))
	writer := tar.NewWriter(encoder)
	seen := map[inode]string{}

	Throw(filepath.WalkDir(srcDir, func(path string, entry fs.DirEntry, err error) error {
		Throw(err)

		info := Throw2(entry.Info())
		rel := Throw2(filepath.Rel(srcDir, path))
		name := "./" + filepath.ToSlash(rel)

		if rel == "." {
			name = "."
		}

		link := ""

		if info.Mode()&os.ModeSymlink != 0 {
			link = Throw2(os.Readlink(path))
		}

		header := Throw2(tar.FileInfoHeader(info, link))
		header.Name = name
		header.Uname = ""
		header.Gname = ""
		header.Uid = 0
		header.Gid = 0

		if info.IsDir() {
			header.Name += "/"
		}

		if info.Mode().IsRegular() {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink > 1 {
				key := inode{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}

				if first, ok := seen[key]; ok {
					header.Typeflag = tar.TypeLink
					header.Linkname = first
					header.Size = 0
					Throw(writer.WriteHeader(header))

					return nil
				}

				seen[key] = name
			}
		}

		Throw(writer.WriteHeader(header))

		if info.Mode().IsRegular() {
			file := Throw2(os.Open(path))
			_, copyErr := io.Copy(writer, file)
			closeErr := file.Close()
			Throw(copyErr)
			Throw(closeErr)
		}

		return nil
	}))

	Throw(writer.Close())
	Throw(encoder.Close())
}
