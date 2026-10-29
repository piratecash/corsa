package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/piratecash/corsa/internal/release/admission"
)

type interimReader interface {
	interimSnapshot(context.Context, string) (string, []byte, admission.CommitHash, error)
}

var errNoInterimApproval = errors.New("no interim approval")

type interimEntry struct {
	path, mode string
	body       []byte
}

// The approval is the only excluded blob: including its own digest would
// require a hash fixed point. Its committed bytes and clean state are still
// checked. Every other tracked path, mode and byte participates.
func hashInterimEntries(entries []interimEntry) (string, error) {
	if len(entries) == 0 {
		return "", errors.New("empty interim tree")
	}
	entries = append([]interimEntry(nil), entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].path < entries[j].path })
	h := sha256.New()
	_, _ = h.Write([]byte("corsa-interim-v2.5.71-tree-v1\x00"))
	previous := ""
	for _, entry := range entries {
		if entry.path == "" || entry.path == "." || entry.path == previous || strings.ContainsRune(entry.path, '\x00') ||
			filepath.IsAbs(entry.path) || filepath.ToSlash(filepath.Clean(entry.path)) != entry.path ||
			entry.path == ".." || strings.HasPrefix(entry.path, "../") {
			return "", errors.New("invalid or repeated interim tree path")
		}
		if entry.mode != "100644" && entry.mode != "100755" {
			return "", fmt.Errorf("unsupported interim tree mode %s at %q", entry.mode, entry.path)
		}
		previous = entry.path
		if entry.path == admission.InterimApprovalPath {
			continue
		}
		for _, value := range [][]byte{[]byte(entry.path), []byte(entry.mode), entry.body} {
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(value)))
			_, _ = h.Write(size[:])
			_, _ = h.Write(value)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (g gitTree) interimSnapshot(ctx context.Context, tag string) (string, []byte, admission.CommitHash, error) {
	if tag != "v2.5.71" {
		return "", nil, "", errors.New("unsupported interim tag")
	}
	// Absence leaves the original S6 gate in force, including Android builds.
	if _, err := os.Lstat(filepath.Join(g.root, admission.InterimApprovalPath)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, "", errNoInterimApproval
		}
		return "", nil, "", err
	}
	commit, err := g.interimHead(ctx, tag)
	if err != nil {
		return "", nil, "", err
	}
	if err := g.interimClean(ctx, commit); err != nil {
		return "", nil, "", err
	}
	listing, err := g.git(ctx, "ls-tree", "-r", "-z", "--full-tree", string(commit))
	if err != nil {
		return "", nil, "", err
	}
	var entries []interimEntry
	var ids []string
	for _, record := range splitNul(listing) {
		meta, path, found := strings.Cut(record, "\t")
		fields := strings.Fields(meta)
		if !found || len(fields) != 3 || fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
			return "", nil, "", fmt.Errorf("unsupported tracked entry %q", record)
		}
		entries = append(entries, interimEntry{path: path, mode: fields[0]})
		ids = append(ids, fields[2])
	}
	blobs, err := g.interimBlobs(ctx, ids)
	if err != nil {
		return "", nil, "", err
	}
	var approval []byte
	notesPresent := false
	for i := range entries {
		entries[i].body = blobs[i]
		if err := g.verifyInterimFile(entries[i]); err != nil {
			return "", nil, "", err
		}
		if entries[i].path == admission.InterimApprovalPath {
			approval = entries[i].body
		}
		if entries[i].path == "docs/release-notes/RELEASE-v2.5.71.md" && len(entries[i].body) != 0 {
			notesPresent = true
		}
	}
	if approval == nil {
		return "", nil, "", errors.New("interim approval must be committed")
	}
	if !notesPresent {
		return "", nil, "", errors.New("interim release notes must be committed and nonempty")
	}
	digest, err := hashInterimEntries(entries)
	if err != nil {
		return "", nil, "", err
	}
	if err := g.interimClean(ctx, commit); err != nil {
		return "", nil, "", err
	}
	end, err := g.interimHead(ctx, tag)
	if err != nil || end != commit {
		return "", nil, "", errors.Join(errors.New("interim refs changed during validation"), err)
	}
	return digest, approval, commit, nil
}

func (g gitTree) interimHead(ctx context.Context, tag string) (admission.CommitHash, error) {
	head, err := g.git(ctx, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	ref, err := g.git(ctx, "rev-parse", "--verify", "refs/tags/"+tag+"^{commit}")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(head))
	if len(value) != 40 || value != strings.TrimSpace(string(ref)) {
		return "", errors.New("interim tag must point at checked-out HEAD")
	}
	if event := os.Getenv("GITHUB_SHA"); event != "" {
		decoded, decodeErr := hex.DecodeString(event)
		if decodeErr != nil || len(decoded) != 20 || event != strings.ToLower(event) {
			return "", errors.New("invalid GitHub event SHA")
		}
		peeled, peelErr := g.git(ctx, "rev-parse", "--verify", event+"^{commit}")
		if peelErr != nil || strings.TrimSpace(string(peeled)) != value {
			return "", errors.New("interim HEAD differs from GitHub event SHA")
		}
	}
	if event := os.Getenv("GITHUB_REF"); event != "" && event != "refs/tags/"+tag {
		return "", errors.New("interim ref differs from GitHub event ref")
	}
	return admission.CommitHash(value), nil
}

func (g gitTree) interimClean(ctx context.Context, commit admission.CommitHash) error {
	changed, err := g.FilesChangedSince(ctx, commit)
	if err != nil {
		return err
	}
	staged, err := g.git(ctx, "diff", "--cached", "--no-ext-diff", "--no-textconv", "--name-only", "-z", string(commit), "--")
	if err != nil {
		return err
	}
	if len(changed) != 0 || len(staged) != 0 {
		return fmt.Errorf("interim release requires clean tracked, staged and untracked inputs: %q %q", changed, splitNul(staged))
	}
	return nil
}

func (g gitTree) verifyInterimFile(entry interimEntry) error {
	path := filepath.Join(g.root, filepath.FromSlash(entry.path))
	// Check every component: a directory symlink must not redirect an input.
	for current := path; current != filepath.Clean(g.root); current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("interim input is a symlink: %s", entry.path)
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(fmt.Errorf("interim input is not regular: %s", entry.path), err)
	}
	if (info.Mode()&0o100 != 0) != (entry.mode == "100755") {
		return fmt.Errorf("interim input executable mode changed: %s", entry.path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(body, entry.body) {
		return fmt.Errorf("interim input bytes differ from HEAD: %s", entry.path)
	}
	return nil
}

func (g gitTree) interimBlobs(ctx context.Context, ids []string) ([][]byte, error) {
	cmd := exec.CommandContext(ctx, "git", "cat-file", "--batch")
	cmd.Dir = g.root
	cmd.Env = gitIsolatedEnv()
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read interim blobs: %w: %s", err, &stderr)
	}
	reader := bufio.NewReader(bytes.NewReader(out))
	blobs := make([][]byte, 0, len(ids))
	for _, id := range ids {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(header)
		if len(fields) != 3 || fields[0] != id || fields[1] != "blob" {
			return nil, errors.New("invalid interim blob response")
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 || size > int64(len(out)) {
			return nil, errors.New("invalid interim blob size")
		}
		body := make([]byte, int(size))
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		if separator, err := reader.ReadByte(); err != nil || separator != '\n' {
			return nil, errors.New("invalid interim blob separator")
		}
		blobs = append(blobs, body)
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing interim blob output")
	}
	return blobs, nil
}

func runInterim(ctx context.Context, req admission.Request, out io.Writer, manifestPath string, reader interimReader) (bool, error) {
	digest, raw, commit, err := reader.interimSnapshot(ctx, "v2.5.71")
	if errors.Is(err, errNoInterimApproval) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	approval, err := admission.DecodeInterimApproval(bytes.NewReader(raw))
	if err != nil {
		return true, err
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return true, err
	}
	m, err := admission.DecodeManifest(bytes.NewReader(manifest))
	if err != nil {
		return true, err
	}
	verdict, err := admission.EvaluateInterim(m, req, *approval, digest)
	if err != nil {
		return true, err
	}
	if _, err := fmt.Fprintf(out, "INTERIM release admitted: 2.5.71; commit %s; source SHA-256 %s\nS6 admitted: false; D1/G2 activation not granted. Publication still requires tests, CGO, lint and all five platform builds.\n", commit, digest); err != nil {
		return true, err
	}
	for _, id := range verdict.Outstanding {
		if _, err := fmt.Fprintf(out, "  unverified S6 condition: %s\n", id); err != nil {
			return true, err
		}
	}
	return true, reportDeferred(out, verdict)
}
