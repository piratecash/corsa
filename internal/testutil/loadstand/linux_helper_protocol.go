package loadstand

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
)

var ErrLinuxOwnerUnsupported = errors.New("owned Linux fixture unavailable")
var ErrLinuxOwnerProtocol = errors.New("owned Linux fixture protocol defect")
var ErrLinuxOwnerBusy = errors.New("owned Linux fixture operation already active")
var ErrLinuxOwnerBound = errors.New("owned Linux fixture requires a finite deadline")
var ErrLinuxOwnerPoisoned = errors.New("owned Linux fixture permanently poisoned")

const linuxPacketLimit = 16 << 10
const linuxHeaderSize = 32
const linuxProtocolVersion = 1

type linuxPacketKind uint16

const (
	linuxInitPublic linuxPacketKind = iota + 1
	linuxInitReady
	linuxSample
	linuxSampleResult
	linuxStop
	linuxTargetExitKnown
	linuxHelperJoined
	linuxDefect
	linuxObserveRuntime
	linuxRuntimeResult
	linuxMemberBind
	linuxMemberBindResult
	linuxMemberReceipts
	linuxMemberReceiptsResult
)

type LinuxRequestID uint64
type LinuxLaunchOrdinal uint64
type LinuxStopDelivery uint8

const (
	LinuxStopNotAttempted LinuxStopDelivery = iota
	LinuxStopSent
	LinuxStopNoPacket
	LinuxStopUnknown
)

type linuxResultCode uint8

type linuxPacket struct {
	kind    linuxPacketKind
	id      LinuxRequestID
	ordinal LinuxLaunchOrdinal
	body    json.RawMessage
}

type linuxSampleRequest struct {
	Nonce       LaunchNonce
	BudgetNanos int64
}

// This closed helper statement is not an RPC-server PID attestation. The
// namespace is lifetime-single-claim; public facts assume the reviewed SDK.
type linuxRuntimeNamespace struct{ Device, Inode uint64 }
type linuxRuntimeFacts struct {
	Provenance LinuxProcessProvenance
	Namespace  linuxRuntimeNamespace
	Public     PublicIdentity
	StartedAt  time.Time
	BeganAt    time.Time
	EndedAt    time.Time
}
type linuxRuntimeReply struct {
	Nonce LaunchNonce
	Code  linuxResultCode
	Facts linuxRuntimeFacts
}
type linuxTerminal struct {
	MemberFinal *linuxMemberFinal `json:",omitempty"`
	Nonce       LaunchNonce
	Code        linuxResultCode
	Exit        ProcessExitResult
}

func encodeLinuxPacket(kind linuxPacketKind, id LinuxRequestID, ordinal LinuxLaunchOrdinal, body any) ([]byte, error) {
	if kind < linuxInitPublic || kind > linuxMemberReceiptsResult || ordinal == 0 {
		return nil, ErrLinuxOwnerProtocol
	}
	raw, err := json.Marshal(body)
	if err != nil || len(raw) > linuxPacketLimit-linuxHeaderSize {
		return nil, ErrLinuxOwnerProtocol
	}
	packet := make([]byte, linuxHeaderSize+len(raw))
	copy(packet, "CLH4")
	binary.BigEndian.PutUint16(packet[4:6], linuxProtocolVersion)
	binary.BigEndian.PutUint16(packet[6:8], uint16(kind))
	binary.BigEndian.PutUint32(packet[8:12], uint32(len(raw)))
	binary.BigEndian.PutUint64(packet[16:24], uint64(id))
	binary.BigEndian.PutUint64(packet[24:32], uint64(ordinal))
	copy(packet[linuxHeaderSize:], raw)
	return packet, nil
}
func decodeLinuxPacket(raw []byte) (linuxPacket, error) {
	if len(raw) < linuxHeaderSize || len(raw) > linuxPacketLimit || string(raw[:4]) != "CLH4" || binary.BigEndian.Uint16(raw[4:6]) != linuxProtocolVersion || binary.BigEndian.Uint32(raw[12:16]) != 0 || int(binary.BigEndian.Uint32(raw[8:12])) != len(raw)-linuxHeaderSize {
		return linuxPacket{}, ErrLinuxOwnerProtocol
	}
	kind := linuxPacketKind(binary.BigEndian.Uint16(raw[6:8]))
	ordinal := LinuxLaunchOrdinal(binary.BigEndian.Uint64(raw[24:32]))
	if kind < linuxInitPublic || kind > linuxMemberReceiptsResult || ordinal == 0 {
		return linuxPacket{}, ErrLinuxOwnerProtocol
	}
	return linuxPacket{kind: kind, id: LinuxRequestID(binary.BigEndian.Uint64(raw[16:24])), ordinal: ordinal, body: append(json.RawMessage(nil), raw[linuxHeaderSize:]...)}, nil
}
func decodeLinuxBody[T any](packet linuxPacket) (T, error) {
	var value T
	if !canonicalLinuxJSON(packet.body, reflect.TypeFor[T](), false) || json.Unmarshal(packet.body, &value) != nil {
		return value, ErrLinuxOwnerProtocol
	}
	return value, nil
}

// There are no arbitrary-label objects in this closed control schema. Require
// exact known names recursively before encoding/json can apply case folding.
func canonicalLinuxJSON(raw []byte, typ reflect.Type, optional bool) bool {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return optional
	}
	if typ.Kind() == reflect.Pointer {
		return canonicalLinuxJSON(raw, typ.Elem(), optional)
	}
	custom := reflect.PointerTo(typ)
	if custom.Implements(reflect.TypeFor[json.Unmarshaler]()) || custom.Implements(reflect.TypeFor[interface{ UnmarshalText([]byte) error }]()) {
		return json.Valid(raw)
	}
	switch typ.Kind() {
	case reflect.Struct:
		return canonicalLinuxObject(raw, typ)
	case reflect.Array, reflect.Slice:
		var rows []json.RawMessage
		if json.Unmarshal(raw, &rows) != nil {
			return false
		}
		for _, row := range rows {
			if !canonicalLinuxJSON(row, typ.Elem(), false) {
				return false
			}
		}
	}
	return json.Valid(raw)
}
func canonicalLinuxObject(raw []byte, typ reflect.Type) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return false
	}
	fields := make(map[string]reflect.StructField, typ.NumField())
	for index := range typ.NumField() {
		field := typ.Field(index)
		if !field.IsExported() || field.Tag.Get("json") == "-" {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			name = field.Name
		}
		fields[name] = field
	}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		key, err := decoder.Token()
		name, ok := key.(string)
		field, known := fields[name]
		if err != nil || !ok || !known || seen[name] {
			return false
		}
		var value json.RawMessage
		optional := strings.Contains(field.Tag.Get("json"), ",omitempty")
		if decoder.Decode(&value) != nil || !canonicalLinuxJSON(value, field.Type, optional) {
			return false
		}
		seen[name] = true
	}
	if last, err := decoder.Token(); err != nil || last != json.Delim('}') {
		return false
	}
	for name, field := range fields {
		if !seen[name] && !strings.Contains(field.Tag.Get("json"), ",omitempty") {
			return false
		}
	}
	_, err = decoder.Token()
	return errors.Is(err, io.EOF)
}

// The containing owner state mutex serializes this fixed-size correlation row.
// A local send never proves helper observation. Event zero remains spontaneous.
type linuxCorrelation struct {
	nonce        LaunchNonce
	ordinal      LinuxLaunchOrdinal
	lastID       LinuxRequestID
	stopID       LinuxRequestID
	stopDelivery LinuxStopDelivery
	stopObserved bool
	poisoned     bool
}

func (c *linuxCorrelation) allocate() (LinuxRequestID, error) {
	if c.poisoned || c.lastID == math.MaxUint64 {
		c.poisoned = true
		return 0, ErrLinuxOwnerPoisoned
	}
	c.lastID++
	return c.lastID, nil
}
func (c *linuxCorrelation) acceptTerminal(packet linuxPacket, terminal linuxTerminal) error {
	if packet.ordinal != c.ordinal || terminal.Nonce != c.nonce || (packet.kind != linuxTargetExitKnown && packet.kind != linuxHelperJoined && packet.kind != linuxDefect) || (packet.id != 0 && (c.stopID == 0 || packet.id != c.stopID)) {
		c.poisoned = true
		return ErrLinuxOwnerProtocol
	}
	if packet.id != 0 {
		c.stopObserved = true
	}
	return nil
}

type linuxMemberFinal struct {
	Receipt          StartReceipt
	Finality         Finality
	Traffic          domain.TransportTrafficStats
	Reason           FinalAccountingReason
	PeersKnown       bool
	PeerCount        int
	MembershipSHA256 string
}
