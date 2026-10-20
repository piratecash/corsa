package loadstand

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestLinuxHelperClosedPayload(t *testing.T) {
	nonce := LaunchNonce{1}
	valid, err := json.Marshal(linuxSampleRequest{Nonce: nonce, BudgetNanos: 1})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"unknown":       `{"Nonce":"` + nonce.String() + `","BudgetNanos":1,"Secret":"must-not-echo"}`,
		"case-alias":    `{"Nonce":"` + nonce.String() + `","BudgetNanos":1,"budgetnanos":0}`,
		"duplicate":     `{"Nonce":"` + nonce.String() + `","BudgetNanos":1,"BudgetNanos":2}`,
		"null-required": `{"Nonce":"` + nonce.String() + `","BudgetNanos":null}`,
		"missing":       `{"Nonce":"` + nonce.String() + `"}`,
		"trailing":      string(valid) + `{}`,
		"unicode-fold":  strings.Replace(string(valid), "BudgetNanos", "BudgetNanoſ", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeLinuxBody[linuxSampleRequest](linuxPacket{body: []byte(raw)}); err == nil {
				t.Fatal("noncanonical helper payload accepted")
			}
		})
	}
	if _, err := decodeLinuxBody[linuxSampleRequest](linuxPacket{body: valid}); err != nil {
		t.Fatal(err)
	}
}
func TestLinuxHelperHeaderBounds(t *testing.T) {
	valid, err := encodeLinuxPacket(linuxSample, 2, 1, linuxSampleRequest{Nonce: LaunchNonce{1}, BudgetNanos: 1})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"truncated":    func(raw []byte) []byte { return raw[:len(raw)-1] },
		"reserved":     func(raw []byte) []byte { raw[12] = 1; return raw },
		"kind":         func(raw []byte) []byte { binary.BigEndian.PutUint16(raw[6:8], 99); return raw },
		"zero-ordinal": func(raw []byte) []byte { clear(raw[24:32]); return raw },
		"trailing":     func(raw []byte) []byte { return append(raw, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeLinuxPacket(mutate(append([]byte(nil), valid...))); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
}
func TestLinuxHelperSpontaneousStatusDoesNotAcknowledgeSentStop(t *testing.T) {
	c := linuxCorrelation{nonce: LaunchNonce{1}, ordinal: 1, lastID: 4, stopID: 4, stopDelivery: LinuxStopSent, poisoned: true}
	p := linuxPacket{kind: linuxHelperJoined, id: 0, ordinal: 1}
	terminal := linuxTerminal{Nonce: c.nonce}
	if err := c.acceptTerminal(p, terminal); err != nil || c.stopObserved || !c.poisoned {
		t.Fatal("spontaneous status lost or forged Stop acknowledgment", err)
	}
	p.id = 4
	if err := c.acceptTerminal(p, terminal); err != nil || !c.stopObserved {
		t.Fatal("exact observed Stop not retained", err)
	}
	p.id = 3
	if err := c.acceptTerminal(p, terminal); err == nil {
		t.Fatal("foreign stop accepted")
	}
}
func TestLinuxHelperRequestOverflowPermanentlyPoisons(t *testing.T) {
	c := linuxCorrelation{lastID: math.MaxUint64}
	if _, err := c.allocate(); err == nil || !c.poisoned || c.lastID != math.MaxUint64 {
		t.Fatal("request ID wrapped")
	}
}

func TestLinuxRuntimeClosedObservationPayload(t *testing.T) {
	// Portable wire shape only, no Linux process/namespace authority is minted.
	value := linuxRuntimeReply{Nonce: LaunchNonce{7}, Code: linuxResultCode(1), Facts: linuxRuntimeFacts{Namespace: linuxRuntimeNamespace{Device: 3, Inode: 4}}}
	raw, err := encodeLinuxPacket(linuxRuntimeResult, 2, 1, value)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := decodeLinuxPacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeLinuxBody[linuxRuntimeReply](packet)
	if err != nil || decoded != value {
		t.Fatal("closed portable observation roundtrip", err)
	}
	request, err := encodeLinuxPacket(linuxObserveRuntime, 2, 1, linuxSampleRequest{Nonce: value.Nonce, BudgetNanos: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeLinuxPacket(request); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(string) string{
		"case-alias":   func(s string) string { return strings.Replace(s, `"Nonce":`, `"nonce":`, 1) },
		"nested-alias": func(s string) string { return strings.Replace(s, `"Inode":`, `"inode":`, 1) },
		"nested-null":  func(s string) string { return strings.Replace(s, `"Inode":4`, `"Inode":null`, 1) },
		"unicode-fold": func(s string) string { return strings.Replace(s, `"Facts":`, `"Factſ":`, 1) },
		"missing":      func(s string) string { return strings.Replace(s, `"Code":1,`, ``, 1) },
		"duplicate":    func(s string) string { return strings.Replace(s, `"Code":1,`, `"Code":1,"Code":1,`, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := mutate(string(packet.body))
			if changed == string(packet.body) {
				t.Fatal("fixture did not mutate payload")
			}
			if _, err := decodeLinuxBody[linuxRuntimeReply](linuxPacket{body: []byte(changed)}); err == nil {
				t.Fatal("noncanonical closed observation accepted")
			}
		})
	}
	value.Facts.Public.PublicKey = strings.Repeat("x", linuxPacketLimit)
	if _, err = encodeLinuxPacket(linuxRuntimeResult, 2, 1, value); err == nil {
		t.Fatal("oversize observation encoded")
	}
}
