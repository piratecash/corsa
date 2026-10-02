package runjournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// recordFormat tags a record file. It changes when the layout or the checksum
// rule changes, and a reader that does not know the tag refuses the file
// rather than guessing what its lines mean.
const recordFormat = "runjournal-record/v1"

// checksumPrefix starts the third line of a record.
const checksumPrefix = "sha256="

// checksumDomain separates this hash from every other sha256 in the project.
const checksumDomain = "corsa/testutil/runjournal/record/v1\x00"

// recordHeader is everything a record says about itself besides the body.
//
// ConfigID is written although it is derivable, because a reader compares it
// with the identifier re-derived from Params: a header whose parameters were
// edited still states the old identifier, and that disagreement is the proof
// of the edit.
type recordHeader struct {
	ConfigID    ConfigID      `json:"config_id"`
	Measurement Measurement   `json:"measurement"`
	Label       Label         `json:"label"`
	Params      []Param       `json:"params"`
	Sources     []SourceStamp `json:"sources"`
	Outcome     Outcome       `json:"outcome"`
	Detail      string        `json:"detail"`
	RecordedAt  time.Time     `json:"recorded_at"`
	BodyBytes   int           `json:"body_bytes"`
	// Run is the sweep run that wrote the record, absent for a record written
	// outside Sweep (RecordCompleted). A completed result with a Run is done
	// only once that run's verified mark exists.
	Run sweepRun `json:"run,omitempty"`
}

func (h recordHeader) key() ConfigKey {
	return ConfigKey{Measurement: h.Measurement, Label: h.Label, Params: h.Params}
}

// encodeRecord renders a record as
//
//	runjournal-record/v1
//	<header as one line of JSON>
//	sha256=<hex over the format, the header line and the body>
//	<body bytes, exactly body_bytes of them>
//
// JSON escapes every control character inside a string, so the header is one
// line whatever a label, a parameter or a failure reason contains, and the
// body is opaque bytes after a counted boundary. Nothing a driver passes can
// make the file mean something else.
func encodeRecord(header recordHeader, body []byte) ([]byte, error) {
	headerLine, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encoding the record header of %s: %w", header.ConfigID, err)
	}
	var out bytes.Buffer
	out.WriteString(recordFormat)
	out.WriteByte('\n')
	out.Write(headerLine)
	out.WriteByte('\n')
	out.WriteString(checksumPrefix)
	out.WriteString(recordChecksum(headerLine, body))
	out.WriteByte('\n')
	out.Write(body)
	return out.Bytes(), nil
}

// recordChecksum covers the raw header bytes and the body together. Raw bytes
// rather than a re-encoding, so verification never depends on JSON encoding
// the decoded value back to the same text; header and body together, so a
// header edited to claim other parameters does not pass on a valid body.
func recordChecksum(headerLine, body []byte) string {
	digest := sha256.New()
	digest.Write([]byte(checksumDomain + recordFormat + "\n"))
	digest.Write(headerLine)
	digest.Write([]byte{'\n'})
	digest.Write(body)
	return hex.EncodeToString(digest.Sum(nil))
}

// decodeRecord parses a record and verifies it against ITSELF: format,
// checksum, body length, outcome, and the stated identifier against the one
// its own parameters derive. It does not compare against what was asked for;
// that is matchRecord's job, so a directory can be read by someone who does
// not know what was expected.
func decodeRecord(raw []byte) (recordHeader, []byte, error) {
	formatLine, rest, ok := bytes.Cut(raw, []byte{'\n'})
	if !ok || string(formatLine) != recordFormat {
		return recordHeader{}, nil, fmt.Errorf("%w: not a %s file", ErrCorruptRecord, recordFormat)
	}
	headerLine, rest, ok := bytes.Cut(rest, []byte{'\n'})
	if !ok {
		return recordHeader{}, nil, fmt.Errorf("%w: the header is cut short", ErrCorruptRecord)
	}
	checksumLine, body, ok := bytes.Cut(rest, []byte{'\n'})
	if !ok || !bytes.HasPrefix(checksumLine, []byte(checksumPrefix)) {
		return recordHeader{}, nil, fmt.Errorf("%w: no checksum line", ErrCorruptRecord)
	}
	if string(checksumLine[len(checksumPrefix):]) != recordChecksum(headerLine, body) {
		return recordHeader{}, nil, fmt.Errorf("%w: the checksum does not match the contents", ErrCorruptRecord)
	}

	header, err := decodeHeader(headerLine)
	if err != nil {
		return recordHeader{}, nil, err
	}
	if err := checkHeaderAgainstItself(header, len(body)); err != nil {
		return recordHeader{}, nil, err
	}
	return header, body, nil
}

func decodeHeader(headerLine []byte) (recordHeader, error) {
	decoder := json.NewDecoder(bytes.NewReader(headerLine))
	// An unknown field is refused: the format tag is versioned, so a field this
	// reader does not know means a writer this reader does not know.
	decoder.DisallowUnknownFields()
	var header recordHeader
	if err := decoder.Decode(&header); err != nil {
		return recordHeader{}, fmt.Errorf("%w: the header does not decode: %w", ErrCorruptRecord, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return recordHeader{}, fmt.Errorf("%w: data after the header", ErrCorruptRecord)
	}
	return header, nil
}

var knownOutcomes = map[Outcome]struct{}{
	OutcomeCompleted:   {},
	OutcomeFailed:      {},
	OutcomeQuarantined: {},
	OutcomeReleased:    {},
	OutcomeVerified:    {},
}

func checkHeaderAgainstItself(header recordHeader, bodyBytes int) error {
	if _, ok := knownOutcomes[header.Outcome]; !ok {
		return fmt.Errorf("%w: outcome %q is not one this format knows", ErrCorruptRecord, header.Outcome)
	}
	for _, stamp := range header.Sources {
		if err := stamp.validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrCorruptRecord, err)
		}
	}
	if header.Run != "" && !validSweepRun(header.Run) {
		return fmt.Errorf("%w: run %q is not a run token", ErrCorruptRecord, header.Run)
	}
	if header.BodyBytes != bodyBytes {
		return fmt.Errorf("%w: the header promises %d body bytes and the file holds %d",
			ErrCorruptRecord, header.BodyBytes, bodyBytes)
	}
	if err := header.key().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrCorruptRecord, err)
	}
	if derived := header.key().ID(); derived != header.ConfigID {
		return fmt.Errorf("%w: the header states configuration %s and its parameters derive %s: it was edited",
			ErrCorruptRecord, header.ConfigID, derived)
	}
	return nil
}

// validSweepRun accepts exactly what newSweepRun produces: lowercase hex of
// attemptTokenHexLen characters, which is also what a file name may carry.
func validSweepRun(run sweepRun) bool {
	if len(run) != attemptTokenHexLen {
		return false
	}
	for _, c := range run {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
