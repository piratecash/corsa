package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/netip"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

const ninHeaderBytes = 72
const ninRowBytes = 520
const ninMaxPacked = ninHeaderBytes + ninGroupCapacity*ninRowBytes
const ninMaxEncoded = 4 * ((ninMaxPacked + 2) / 3)

const ninChecksumDomain = "corsa/nin-journal/v1\x00"

func ninKeyOf(group domain.SourceGroup) (ninKey, error) {
	var key ninKey
	switch group {
	case domain.SourceGroupLoopback:
		return key, nil
	case domain.SourceGroupTor:
		key[0] = 3
		return key, nil
	case domain.SourceGroupI2P:
		key[0] = 4
		return key, nil
	case domain.SourceGroupUnresolvedName:
		key[0] = 5
		return key, nil
	}
	p, err := netip.ParsePrefix(string(group))
	if err != nil || p != p.Masked() || p.String() != string(group) {
		return key, errNinJournal
	}
	if p.Addr().Is4() && p.Bits() == 16 {
		a := p.Addr().As4()
		key[0] = 1
		copy(key[1:3], a[:2])
		return key, nil
	}
	if p.Addr().Is6() && p.Bits() == 32 {
		a := p.Addr().As16()
		key[0] = 2
		copy(key[1:], a[:4])
		return key, nil
	}
	return key, errNinJournal
}
func ninGroupOf(key ninKey) (domain.SourceGroup, error) {
	switch key[0] {
	case 0, 3, 4, 5:
		if key[1] != 0 || key[2] != 0 || key[3] != 0 || key[4] != 0 {
			return "", errNinJournal
		}
		return map[byte]domain.SourceGroup{0: domain.SourceGroupLoopback, 3: domain.SourceGroupTor, 4: domain.SourceGroupI2P, 5: domain.SourceGroupUnresolvedName}[key[0]], nil
	case 1:
		if key[3] != 0 || key[4] != 0 {
			return "", errNinJournal
		}
		a := netip.AddrFrom4([4]byte{key[1], key[2]})
		return domain.SourceGroup(netip.PrefixFrom(a, 16).String()), nil
	case 2:
		var a [16]byte
		copy(a[:4], key[1:])
		return domain.SourceGroup(netip.PrefixFrom(netip.AddrFrom16(a), 32).String()), nil
	}
	return "", errNinJournal
}
func validateNinImage(i *ninJournalImage) error {
	if i == nil || i.domain == ([16]byte{}) || i.generation == 0 || (i.phase != ninKnown && i.phase != ninUnknown) || len(i.rows) < 1 || len(i.rows) > ninGroupCapacity {
		return errNinJournal
	}
	if i.phase == ninUnknown && i.tick >= ninWindow {
		return errNinJournal
	}
	for n, row := range i.rows {
		if _, err := ninGroupOf(row.key); err != nil {
			return err
		}
		if row.count > ninEventsPerGroup || row.reserved != ([2]byte{}) || (n == 0 && row.key != (ninKey{})) || (n > 0 && (row.count == 0 || bytes.Compare(i.rows[n-1].key[:], row.key[:]) >= 0)) {
			return errNinJournal
		}
		for k, tick := range row.ticks {
			if k >= int(row.count) {
				if tick != 0 {
					return errNinJournal
				}
				continue
			}
			if i.phase == ninUnknown || tick > i.tick || (i.tick >= ninWindow && tick <= i.tick-ninWindow) || (k > 0 && tick < row.ticks[k-1]) {
				return errNinJournal
			}
		}
	}
	return nil
}
func ninChecksum(raw []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(ninChecksumDomain))
	_, _ = h.Write(raw[:40])
	_, _ = h.Write(raw[ninHeaderBytes:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func encodeNinImage(i *ninJournalImage) ([]byte, error) {
	if err := validateNinImage(i); err != nil {
		return nil, err
	}
	raw := make([]byte, ninHeaderBytes+len(i.rows)*ninRowBytes)
	copy(raw, "NQ")
	raw[2] = 1
	raw[3] = 0x10 | i.phase
	copy(raw[4:20], i.domain[:])
	binary.BigEndian.PutUint64(raw[20:28], i.generation)
	binary.BigEndian.PutUint64(raw[28:36], i.tick)
	binary.BigEndian.PutUint16(raw[36:38], ninGroupCapacity)
	binary.BigEndian.PutUint16(raw[38:40], uint16(len(i.rows)))
	for n, row := range i.rows {
		b := raw[ninHeaderBytes+n*ninRowBytes:]
		copy(b[:5], row.key[:])
		b[5] = row.count
		for k, tick := range row.ticks {
			binary.BigEndian.PutUint64(b[8+k*8:16+k*8], tick)
		}
	}
	sum := ninChecksum(raw)
	copy(raw[40:72], sum[:])
	return raw, nil
}
func decodeNinImage(raw []byte) (*ninJournalImage, error) {
	if len(raw) < ninHeaderBytes || len(raw) > ninMaxPacked || string(raw[:2]) != "NQ" || raw[2] != 1 || (raw[3] != 0x11 && raw[3] != 0x12) || binary.BigEndian.Uint16(raw[36:38]) != ninGroupCapacity {
		return nil, errNinJournal
	}
	count := int(binary.BigEndian.Uint16(raw[38:40]))
	if count < 1 || count > ninGroupCapacity || len(raw) != ninHeaderBytes+count*ninRowBytes {
		return nil, errNinJournal
	}
	sum := ninChecksum(raw)
	if !bytes.Equal(sum[:], raw[40:72]) {
		return nil, errNinJournal
	}
	i := &ninJournalImage{phase: raw[3] & 0x0f, generation: binary.BigEndian.Uint64(raw[20:28]), tick: binary.BigEndian.Uint64(raw[28:36]), rows: make([]ninRow, count)}
	copy(i.domain[:], raw[4:20])
	for n := range i.rows {
		b := raw[ninHeaderBytes+n*ninRowBytes:]
		row := &i.rows[n]
		copy(row.key[:], b[:5])
		row.count = b[5]
		copy(row.reserved[:], b[6:8])
		for k := range row.ticks {
			row.ticks[k] = binary.BigEndian.Uint64(b[8+k*8 : 16+k*8])
		}
	}
	if err := validateNinImage(i); err != nil {
		return nil, err
	}
	return i, nil
}
func (i ninJournalImage) MarshalJSON() ([]byte, error) {
	raw, err := encodeNinImage(&i)
	if err != nil {
		return nil, err
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(raw))
}
func (i *ninJournalImage) UnmarshalJSON(raw []byte) error {
	if len(raw) > ninMaxEncoded+2 || len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return errNinJournal
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return errNinJournal
	}
	canonical, _ := json.Marshal(encoded)
	if !bytes.Equal(canonical, raw) || len(encoded) > ninMaxEncoded {
		return errNinJournal
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return errNinJournal
	}
	if base64.StdEncoding.EncodeToString(decoded) != encoded {
		return errNinJournal
	}
	image, err := decodeNinImage(decoded)
	if err != nil {
		return err
	}
	*i = *image
	return nil
}

// The standard decoder has already validated syntax. This constant-memory
// top-level walk rejects duplicate format/journal keys without decoding a second
// copy of the large identity/endpoint maps. New-format aliases are not accepted.
func validateNinObjectKeys(raw []byte, strict bool) error {
	seen := make(map[string]bool, 2)
	i := 0
	space := func() {
		for i < len(raw) && (raw[i] == ' ' || raw[i] == '\n' || raw[i] == '\r' || raw[i] == '\t') {
			i++
		}
	}
	stringEnd := func(start int) int {
		for n := start + 1; n < len(raw); n++ {
			if raw[n] == '\\' {
				n++
				continue
			}
			if raw[n] == '"' {
				return n + 1
			}
		}
		return len(raw)
	}
	space()
	if i >= len(raw) || raw[i] != '{' {
		return errNinJournal
	}
	i++
	for {
		space()
		if i >= len(raw) {
			return errNinJournal
		}
		if raw[i] == '}' {
			return nil
		}
		start := i
		end := stringEnd(i)
		var key string
		if err := json.Unmarshal(raw[start:end], &key); err != nil {
			return errNinJournal
		}
		i = end
		space()
		if i >= len(raw) || raw[i] != ':' {
			return errNinJournal
		}
		i++
		lower := strings.ToLower(key)
		if lower == "inbound_pin_quota" && !strict {
			return errNinJournal
		}
		if lower == "format" || lower == "inbound_pin_quota" {
			if seen[lower] || (strict && (key != lower || string(raw[start:end]) != "\""+lower+"\"")) {
				return errNinJournal
			}
			seen[lower] = true
		}
		depth := 0
		for i < len(raw) {
			c := raw[i]
			if c == '"' {
				i = stringEnd(i)
				continue
			}
			if c == '{' || c == '[' {
				depth++
			}
			if c == '}' || c == ']' {
				if depth == 0 {
					return nil
				}
				depth--
			}
			if c == ',' && depth == 0 {
				i++
				break
			}
			i++
		}
	}
}
