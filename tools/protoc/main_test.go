package main

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// fakeArchive builds a protoc-shaped zip: bin/protoc plus one include file.
func fakeArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, body := range map[string]string{
		"bin/protoc": "#!/bin/sh\necho fake\n",
		"include/google/protobuf/timestamp.proto": "syntax = \"proto3\";\n",
		"readme.txt": "ignored\n",
	} {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestInstallIsAtomicAndMarksCompletion(t *testing.T) {
	dest := t.TempDir()
	versioned := filepath.Join(dest, "protoc-1.0")

	if err := install(fakeArchive(t), dest, versioned); err != nil {
		t.Fatal(err)
	}
	if !installed(versioned) {
		t.Fatal("a finished install must carry the completion marker")
	}
	for _, rel := range []string{"bin/protoc", "include/google/protobuf/timestamp.proto"} {
		if _, err := os.Stat(filepath.Join(versioned, rel)); err != nil {
			t.Errorf("%s missing after install: %v", rel, err)
		}
	}
	if _, err := os.Stat(filepath.Join(versioned, "readme.txt")); err == nil {
		t.Error("entries outside bin/ and include/ must not be extracted")
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("dest should contain only the versioned directory, got %d entries", len(entries))
	}
}

func TestInstallLeavesNothingBehindOnBadArchive(t *testing.T) {
	dest := t.TempDir()
	versioned := filepath.Join(dest, "protoc-1.0")

	if err := install([]byte("not a zip"), dest, versioned); err == nil {
		t.Fatal("expected an error for a corrupt archive")
	}
	if installed(versioned) {
		t.Fatal("a failed install must not be marked complete")
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("failed install left %d entries in dest", len(entries))
	}
}

func TestInstalledRequiresMarker(t *testing.T) {
	// A directory with a binary but no marker is what an interrupted install
	// from an older version of this tool could leave behind.
	versioned := filepath.Join(t.TempDir(), "protoc-1.0")
	if err := os.MkdirAll(filepath.Join(versioned, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(versioned, "bin", "protoc"), []byte("truncated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if installed(versioned) {
		t.Fatal("a directory without the completion marker must not count as installed")
	}
}
