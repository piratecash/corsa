package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

const maxCaptureFileBytes int64 = 16 << 20

var errCaptureFileLimit = errors.New("capture file size limit reached")

// Writer is the interface for the destination of capture events.
// Production code writes to an *os.File; tests inject an in-memory buffer.
type Writer interface {
	io.Writer
	io.Closer
	// Sync flushes data to durable storage. No-op for in-memory writers.
	Sync() error
}

// WriterFactory creates a Writer for the given filesystem path.
// Production code opens a file; tests inject an in-memory buffer factory.
type WriterFactory func(path string) (Writer, error)

// DefaultWriterFactory creates a new protected file for writing.
func DefaultWriterFactory(path string) (Writer, error) {
	w, err := openCaptureFile(path)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func openCaptureFile(path string) (*fileWriter, error) {
	dir, err := secretfile.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	f, err := dir.CreateExclusive(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	return &fileWriter{file: f, dir: filepath.Dir(path), remaining: maxCaptureFileBytes}, nil
}

type fileWriter struct {
	file      *os.File
	dir       string
	mu        sync.Mutex
	remaining int64
	storage   *captureStorage
	closed    bool
}

func (fw *fileWriter) Sync() error { return fw.file.Sync() }

func (fw *fileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return os.ErrClosed
	}
	fw.closed = true
	err := fw.file.Close()
	if fw.storage != nil {
		fw.storage.writerClosed()
	}
	return err
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return 0, os.ErrClosed
	}
	size := int64(len(p))
	if size > fw.remaining {
		return 0, errCaptureFileLimit
	}
	if fw.storage != nil {
		if err := fw.storage.reserve(fw.dir, size); err != nil {
			return 0, err
		}
	}
	n, err := fw.file.Write(p)
	fw.remaining -= int64(n)
	if fw.storage != nil {
		fw.storage.finish(size, int64(n))
	}
	return n, err
}

// ---------------------------------------------------------------------------
// Rendering — compact and pretty format (plan §6.3)
// ---------------------------------------------------------------------------

// renderCompactLine writes a single compact-format capture line:
//
//	2026-04-14T12:44:03.412Z out: {"type":"ping"}
//	2026-04-14T12:44:03.412Z out [write_failed]: {"type":"ping"}
func renderCompactLine(w io.Writer, ev Event) error {
	ts := ev.Ts.UTC().Format("2006-01-02T15:04:05.000Z")

	var header string
	if ev.WireDir == domain.WireDirectionOut && ev.Outcome.IsFailed() {
		header = fmt.Sprintf("%s %s [%s]:", ts, ev.WireDir, ev.Outcome)
	} else {
		header = fmt.Sprintf("%s %s:", ts, ev.WireDir)
	}

	payload := ev.Raw
	if ev.Kind == domain.PayloadKindJSON {
		var compact bytes.Buffer
		if json.Compact(&compact, []byte(ev.Raw)) == nil {
			payload = compact.String()
		}
	}

	_, err := fmt.Fprintf(w, "%s %s\n", header, payload)
	return err
}

// renderPrettyLine writes a pretty-format capture event:
//
//	2026-04-14T12:44:03.412Z out:
//	{
//	  "type": "ping"
//	}
//
// Adjacent events are separated by an empty line.
func renderPrettyLine(w io.Writer, ev Event, needSeparator bool) error {
	separator := ""
	if needSeparator {
		separator = "\n"
	}

	ts := ev.Ts.UTC().Format("2006-01-02T15:04:05.000Z")

	var header string
	if ev.WireDir == domain.WireDirectionOut && ev.Outcome.IsFailed() {
		header = fmt.Sprintf("%s %s [%s]:", ts, ev.WireDir, ev.Outcome)
	} else {
		header = fmt.Sprintf("%s %s:", ts, ev.WireDir)
	}

	payload := ev.Raw
	if ev.Kind == domain.PayloadKindJSON {
		var pretty bytes.Buffer
		if json.Indent(&pretty, []byte(ev.Raw), "", "  ") == nil {
			payload = pretty.String()
		}
	}

	// Commit the header and payload together so quota refusal leaves no
	// partial event, including the preceding separator.
	_, err := fmt.Fprintf(w, "%s%s\n%s\n", separator, header, payload)
	return err
}
