package node

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"time"
)

// This bounds only the optional encoded JSON value. loadPeerState still
// reads and validates the existing whole file before this decoder runs.
const overlayDegreeHintMaxBytes = 256

// Retained statistics only: no raw JSON, decoder state, proof or callback.
// A saved hint is not a live degree cut and cannot grant admission authority.
type overlayDegreeStatistic struct {
	value uint8
	at    time.Time
	valid bool
}

// The DTO field is a value so repeated JSON null cannot replace a pointer
// and erase duplicate detection. Only stat is copied into Service.
type overlayDegreeHint struct {
	stat overlayDegreeStatistic
	seen bool
}

func (h *overlayDegreeHint) UnmarshalJSON(raw []byte) error {
	alreadySeen := h.seen
	*h = overlayDegreeHint{seen: true}
	if alreadySeen || len(raw) > overlayDegreeHintMaxBytes {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil
	}
	var value uint8
	var at time.Time
	valueSeen, atSeen := false, false
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil
		}
		switch key {
		case "value":
			if valueSeen {
				return nil
			}
			valueSeen = true
			token, err := decoder.Token()
			number, ok := token.(json.Number)
			if err != nil || !ok {
				return nil
			}
			parsed, err := strconv.ParseUint(number.String(), 10, 64)
			if err != nil {
				return nil
			}
			// Clip while still uint64, before conversion or arithmetic.
			if parsed > 64 {
				parsed = 64
			}
			value = uint8(parsed)
		case "at":
			if atSeen {
				return nil
			}
			atSeen = true
			token, err := decoder.Token()
			stamp, ok := token.(string)
			if err != nil || !ok {
				return nil
			}
			at, err = time.Parse(time.RFC3339Nano, stamp)
			if err != nil || at.IsZero() {
				return nil
			}
		default:
			return nil
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !valueSeen || !atSeen {
		return nil
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil
	}
	h.stat = overlayDegreeStatistic{value: value, at: at.UTC(), valid: true}
	return nil
}

func (h overlayDegreeHint) IsZero() bool { return !h.stat.valid }

// Value receivers work when peerStateFile is passed by value to MarshalIndent.
func (h overlayDegreeHint) MarshalJSON() ([]byte, error) {
	if h.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(struct {
		Value uint8     `json:"value"`
		At    time.Time `json:"at"`
	}{Value: h.stat.value, At: h.stat.at})
}

// Only optional statistics use wall time. N-in and live degree clocks are
// separate. This neither refreshes the original at nor constructs a consumer.
func freshOverlayDegreeStatistic(s overlayDegreeStatistic, now time.Time) overlayDegreeStatistic {
	if !s.valid || s.value > 64 || s.at.IsZero() || now.IsZero() || s.at.After(now) || now.Sub(s.at) >= 24*time.Hour {
		return overlayDegreeStatistic{}
	}
	return s
}
