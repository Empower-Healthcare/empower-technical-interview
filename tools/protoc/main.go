// Command protoc downloads the pinned protoc release for this machine into a
// repository-local directory and verifies its SHA-256 before unpacking.
//
//	go run ./tools/protoc -version 36.1 -dest bin
//
// Result: <dest>/protoc-<version>/{bin/protoc,include/...} and a <dest>/protoc
// symlink. Nothing outside <dest> is touched, so it works the same whether Go
// came from go.dev, Homebrew, asdf, or mise. The include directory is what
// `make generate` passes to protoc for google/protobuf/*.proto.
package main

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// checksums are the SHA-256 digests of the official release zips, keyed by
// version then asset, taken from
// https://github.com/protocolbuffers/protobuf/releases/tag/v<version>.
// The Makefile (PROTOC_VERSION) decides which version is used; bumping it
// means adding that version's digests here and regenerating gen/.
var checksums = map[string]map[string]string{
	"36.1": {
		"linux-x86_64":   "c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4",
		"linux-aarch_64": "237a68856edf1bd28b6204bddd0596c1cf46d298bc29c620012540b2e44c73e7",
		"osx-x86_64":     "ee2c5496e4af0aa6a224894bc0f7025145260e004d890487d510725ce8b473eb",
		"osx-aarch_64":   "de56d57afe30c5d191b11d24ff93dd4025728d7fb43b773886b2d3613e0bdbb2",
	},
}

func main() {
	version := flag.String("version", "", "protoc release to install (required, e.g. 36.1)")
	dest := flag.String("dest", "bin", "directory to install into")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := run(ctx, *version, *dest); err != nil {
		fmt.Fprintln(os.Stderr, "protoc:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, version, dest string) error {
	if version == "" {
		return errors.New("-version is required")
	}
	digests, ok := checksums[version]
	if !ok {
		return fmt.Errorf("no checksums recorded for protoc %s; add them to tools/protoc/main.go", version)
	}
	asset, err := assetName(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return err
	}
	versioned := filepath.Join(dest, "protoc-"+version)
	binary := filepath.Join(versioned, "bin", "protoc")
	if installed(versioned) {
		return link(dest, binary)
	}
	if _, err := os.Stat(versioned); err == nil {
		// Left over from an interrupted install: never trust it, rebuild it.
		fmt.Printf("removing incomplete %s\n", versioned)
		if err := os.RemoveAll(versioned); err != nil {
			return err
		}
	}

	url := fmt.Sprintf("https://github.com/protocolbuffers/protobuf/releases/download/v%s/protoc-%s-%s.zip", version, version, asset)
	fmt.Printf("downloading %s\n", url)
	archive, err := download(ctx, url, digests[asset])
	if err != nil {
		return err
	}
	if err := install(archive, dest, versioned); err != nil {
		return err
	}
	fmt.Printf("installed protoc %s into %s\n", version, versioned)
	return link(dest, binary)
}

// completeMarker is written last inside a versioned directory; its presence is
// the only thing that makes an existing directory count as installed.
const completeMarker = ".complete"

func installed(versioned string) bool {
	_, err := os.Stat(filepath.Join(versioned, completeMarker))
	return err == nil
}

// install unpacks the archive into a temporary sibling of versioned and renames
// it into place only once everything, including the marker, is written. An
// interruption leaves a protoc-<version>.tmp-* directory that the next run
// ignores and this run removes on failure; it never leaves a half-written
// directory that later runs would accept.
func install(archive []byte, dest, versioned string) (err error) {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(dest, filepath.Base(versioned)+".tmp-")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := unzip(archive, tmp); err != nil {
		return fmt.Errorf("unpack: %w", err)
	}
	if err := os.Chmod(filepath.Join(tmp, "bin", "protoc"), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, completeMarker), nil, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, versioned)
}

// assetName maps Go's OS/arch names to the release asset suffixes.
func assetName(goos, goarch string) (string, error) {
	osName := map[string]string{"linux": "linux", "darwin": "osx"}[goos]
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch_64"}[goarch]
	if osName == "" || arch == "" {
		return "", fmt.Errorf("no pinned protoc build for %s/%s (Windows: use WSL2)", goos, goarch)
	}
	return osName + "-" + arch, nil
}

// download fetches url into memory and returns it only if its SHA-256 matches.
func download(ctx context.Context, url, wantSHA string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != wantSHA {
		return nil, fmt.Errorf("checksum mismatch for %s: got %s, want %s", url, got, wantSHA)
	}
	return body, nil
}

// unzip extracts bin/ and include/ from the archive into dir, refusing paths
// that would escape it.
func unzip(archive []byte, dir string) error {
	r, err := zip.NewReader(strings.NewReader(string(archive)), int64(len(archive)))
	if err != nil {
		return err
	}
	for _, f := range r.File {
		name := filepath.Clean(f.Name)
		if !strings.HasPrefix(name, "bin"+string(filepath.Separator)) && !strings.HasPrefix(name, "include"+string(filepath.Separator)) {
			continue
		}
		target := filepath.Join(dir, name)
		if !strings.HasPrefix(target, filepath.Clean(dir)+string(filepath.Separator)) {
			return fmt.Errorf("archive entry escapes destination: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := extractFile(f, target); err != nil {
			return err
		}
	}
	return nil
}

func extractFile(f *zip.File, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	src, err := f.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, f.Mode().Perm()|0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, src)
	return errors.Join(copyErr, dst.Close())
}

// link points <dest>/protoc at the versioned binary.
func link(dest, binary string) error {
	rel, err := filepath.Rel(dest, binary)
	if err != nil {
		return err
	}
	target := filepath.Join(dest, "protoc")
	_ = os.Remove(target)
	return os.Symlink(rel, target)
}
