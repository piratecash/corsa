package capture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/piratecash/corsa/internal/core/domain"
)

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

// DefaultWriterFactory opens a regular file for writing.
func DefaultWriterFactory(path string) (Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &fileWriter{File: f}, nil
}

type fileWriter struct{ *os.File }

func (fw *fileWriter) Sync() error { return fw.File.Sync() }

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
	if needSeparator {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}

	ts := ev.Ts.UTC().Format("2006-01-02T15:04:05.000Z")

	var header string
	if ev.WireDir == domain.WireDirectionOut && ev.Outcome.IsFailed() {
		header = fmt.Sprintf("%s %s [%s]:", ts, ev.WireDir, ev.Outcome)
	} else {
		header = fmt.Sprintf("%s %s:", ts, ev.WireDir)
	}

	if _, err := fmt.Fprintln(w, header); err != nil {
		return err
	}

	if ev.Kind == domain.PayloadKindJSON {
		var pretty bytes.Buffer
		if json.Indent(&pretty, []byte(ev.Raw), "", "  ") == nil {
			_, err := fmt.Fprintln(w, pretty.String())
			return err
		}
	}

	// Fallback: raw payload (non-JSON, invalid JSON, malformed).
	_, err := fmt.Fprintln(w, ev.Raw)
	return err
}
