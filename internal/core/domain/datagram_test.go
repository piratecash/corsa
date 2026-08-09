package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseDatagramMode(t *testing.T) {
	cases := map[string]struct {
		input   string
		want    DatagramMode
		wantErr error
	}{
		"routed":     {"routed", DatagramModeRouted, nil},
		"request":    {"request", DatagramModeRequest, nil},
		"response":   {"response", DatagramModeResponse, nil},
		"unknown":    {"broadcast", "", ErrInvalidDatagramMode},
		"empty":      {"", "", ErrInvalidDatagramMode},
		"upper case": {"ROUTED", "", ErrInvalidDatagramMode},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseDatagramMode(tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("mode = %q, want %q", got, tc.want)
			}
			if tc.wantErr == nil && got.String() != tc.input {
				t.Fatalf("String() = %q, want the wire form %q", got.String(), tc.input)
			}
		})
	}
}

func TestParseDatagramClass(t *testing.T) {
	cases := map[string]struct {
		input   string
		want    DatagramClass
		wantErr error
	}{
		"control": {"control", DatagramClassControl, nil},
		"bulk":    {"bulk", DatagramClassBulk, nil},
		// A third class would be a new wire format, not a new value (§2.3).
		"unknown": {"gossip", "", ErrInvalidDatagramClass},
		"empty":   {"", "", ErrInvalidDatagramClass},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseDatagramClass(tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("class = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseRoutePolicy(t *testing.T) {
	cases := map[string]struct {
		input   string
		want    RoutePolicy
		wantErr error
	}{
		"best":    {"best", RoutePolicyBest, nil},
		"explore": {"explore", RoutePolicyExplore, nil},
		"unknown": {"fastest", RoutePolicyNone, ErrInvalidRoutePolicy},
		// Absence is a separate state; a present-but-empty field is a reject.
		"empty": {"", RoutePolicyNone, ErrInvalidRoutePolicy},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseRoutePolicy(tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("policy = %q, want %q", got, tc.want)
			}
		})
	}
	if !RoutePolicyNone.IsNone() || RoutePolicyBest.IsNone() {
		t.Fatal("IsNone must distinguish an absent field from a real policy")
	}
}

// TestDatagramModeMatrix pins the closed contract of §2.1 row by row. It is
// stated as data here for the same reason the implementation is: a table
// makes an accidentally half-added mode visible.
func TestDatagramModeMatrix(t *testing.T) {
	cases := []struct {
		mode        DatagramMode
		control     bool
		bulk        bool
		auth        bool
		routePolicy bool
	}{
		{DatagramModeRouted, true, true, true, true},
		{DatagramModeRequest, true, false, false, true},
		{DatagramModeResponse, true, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.mode.String(), func(t *testing.T) {
			rule, ok := DatagramModeRuleFor(tc.mode)
			if !ok {
				t.Fatalf("mode %q has no rule", tc.mode)
			}
			if got := rule.AllowsClass(DatagramClassControl); got != tc.control {
				t.Fatalf("AllowsClass(control) = %v, want %v", got, tc.control)
			}
			if got := rule.AllowsClass(DatagramClassBulk); got != tc.bulk {
				t.Fatalf("AllowsClass(bulk) = %v, want %v", got, tc.bulk)
			}
			if rule.AuthRequired != tc.auth {
				t.Fatalf("AuthRequired = %v, want %v", rule.AuthRequired, tc.auth)
			}
			if rule.RoutePolicyRequired != tc.routePolicy {
				t.Fatalf("RoutePolicyRequired = %v, want %v", rule.RoutePolicyRequired, tc.routePolicy)
			}
			if !tc.mode.Valid() {
				t.Fatal("a mode with a rule must be Valid")
			}
		})
	}
	if _, ok := DatagramModeRuleFor("broadcast"); ok {
		t.Fatal("an unknown mode must have no rule")
	}
	if rule, _ := DatagramModeRuleFor(DatagramModeRouted); rule.AllowsClass("gossip") {
		t.Fatal("an unknown class must not be admitted by any mode")
	}
}

func TestParseVersionBytes(t *testing.T) {
	cases := map[string]struct {
		input   int64
		wantErr bool
	}{
		"one":      {1, false},
		"max":      {255, false},
		"zero":     {0, true},
		"negative": {-1, true},
		"over max": {256, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// v and av share one rule so two parsers cannot disagree on what
			// the single transcript byte was (§3.4).
			_, headerErr := ParseDatagramVersion(tc.input)
			_, authErr := ParseAuthVersion(tc.input)
			for label, err := range map[string]error{"v": headerErr, "av": authErr} {
				if (err != nil) != tc.wantErr {
					t.Fatalf("%s: error = %v, wantErr = %v", label, err, tc.wantErr)
				}
			}
			if !tc.wantErr {
				return
			}
			if !errors.Is(headerErr, ErrInvalidDatagramVersion) ||
				!errors.Is(authErr, ErrInvalidAuthVersion) {
				t.Fatal("each version field must report its own sentinel")
			}
		})
	}
	// v2 is the envelope without req_caps and ext: the field set and the
	// transcript both changed, so the header version had to move with them.
	if DatagramHeaderVersion != 2 {
		t.Fatalf("DatagramHeaderVersion = %d, want 2", DatagramHeaderVersion)
	}
	if AuthVersionBase != 1 {
		t.Fatalf("AuthVersionBase = %d, want 1", AuthVersionBase)
	}
}

func TestParseDType(t *testing.T) {
	cases := map[string]struct {
		input   string
		wantErr bool
	}{
		"lowercase":     {"delivery_receipt", false},
		"digits":        {"dm_v2", false},
		"at max length": {strings.Repeat("d", MaxDTypeLen), false},
		"empty":         {"", true},
		"uppercase":     {"Delivery_Receipt", true},
		"dash":          {"delivery-receipt", true},
		"dot":           {"delivery.receipt", true},
		"over max":      {strings.Repeat("d", MaxDTypeLen+1), true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseDType(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidDType) {
					t.Fatalf("error = %v, want ErrInvalidDType", err)
				}
				return
			}
			if got.String() != tc.input {
				t.Fatalf("String() = %q, want %q", got.String(), tc.input)
			}
		})
	}
}

func TestParseCapabilityName(t *testing.T) {
	cases := map[string]struct {
		input   string
		wantErr bool
	}{
		"profile name":  {"mesh_datagram_durable_v1", false},
		"at max length": {strings.Repeat("a", MaxCapabilityNameLen), false},
		"empty":         {"", true},
		"over max":      {strings.Repeat("a", MaxCapabilityNameLen+1), true},
		"dash":          {"mesh-datagram", true},
		"uppercase":     {"Mesh_Datagram", true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseCapabilityName(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidCapabilityName) {
					t.Fatalf("error = %v, want ErrInvalidCapabilityName", err)
				}
				return
			}
			if got.Capability() != Capability(tc.input) {
				t.Fatalf("Capability() = %q, want %q", got.Capability(), tc.input)
			}
		})
	}
}

// TestDatagramClassSizingConstants pins the wire-normative numbers of §2.3
// and §4.2. They are not local tuning knobs: a mismatch shows up as a frame
// one node accepts and its neighbour drops.
func TestDatagramClassSizingConstants(t *testing.T) {
	cases := map[DatagramClass]struct {
		payloadCap int
		residence  time.Duration
	}{
		DatagramClassControl: {4 * 1024, 5 * time.Second},
		DatagramClassBulk:    {64 * 1024, 30 * time.Second},
	}
	for class, want := range cases {
		t.Run(class.String(), func(t *testing.T) {
			cap, err := DatagramPayloadCap(class)
			if err != nil {
				t.Fatalf("DatagramPayloadCap: %v", err)
			}
			if cap != want.payloadCap {
				t.Fatalf("payload cap = %d, want %d", cap, want.payloadCap)
			}
			residence, err := QueueResidence(class)
			if err != nil {
				t.Fatalf("QueueResidence: %v", err)
			}
			if residence != want.residence {
				t.Fatalf("queue residence = %s, want %s", residence, want.residence)
			}
			grace, err := WriteGrace(class)
			if err != nil {
				t.Fatalf("WriteGrace: %v", err)
			}
			// §4.2 defines write grace as numerically equal to queue
			// residence; a second constant could only drift.
			if grace != residence {
				t.Fatalf("write grace = %s, want %s", grace, residence)
			}
		})
	}

	// An unknown class is an error, never a zero ceiling: zero would reject
	// every frame silently instead of surfacing the bug.
	for name, call := range map[string]func() error{
		"payload cap":     func() error { _, err := DatagramPayloadCap("gossip"); return err },
		"queue residence": func() error { _, err := QueueResidence("gossip"); return err },
		"write grace":     func() error { _, err := WriteGrace("gossip"); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalidDatagramClass) {
			t.Fatalf("%s of an unknown class: error = %v, want ErrInvalidDatagramClass", name, err)
		}
	}
}

// TestReverseStateTTLMatchesFormula recomputes §4.2 from its inputs: the
// entry must survive a full round trip of DatagramDefaultMaxHops hops each
// way, at queue residence plus write grace per hop, plus the target budget.
func TestReverseStateTTLMatchesFormula(t *testing.T) {
	residence, err := QueueResidence(DatagramClassControl)
	if err != nil {
		t.Fatalf("QueueResidence: %v", err)
	}
	grace, err := WriteGrace(DatagramClassControl)
	if err != nil {
		t.Fatalf("WriteGrace: %v", err)
	}
	const targetBudget = 10 * time.Second
	roundTrip := 2*time.Duration(DatagramDefaultMaxHops)*(residence+grace) + targetBudget
	if roundTrip != 210*time.Second {
		t.Fatalf("round trip = %s, want 210s per §4.2", roundTrip)
	}
	if ReverseStateTTL < roundTrip {
		t.Fatalf("ReverseStateTTL = %s, shorter than the round trip %s it must survive", ReverseStateTTL, roundTrip)
	}
	if ReverseStateTTL != 240*time.Second {
		t.Fatalf("ReverseStateTTL = %s, want the 240s of §4.2", ReverseStateTTL)
	}
}

// TestDatagramWireConstants pins the remaining numbers that two
// implementations must agree on byte for byte.
func TestDatagramWireConstants(t *testing.T) {
	checks := map[string]struct{ got, want int }{
		"DatagramDefaultMaxHops": {int(DatagramDefaultMaxHops), 10},
		"MaxCapabilityNameLen":   {MaxCapabilityNameLen, 40},
		"MaxRawCapabilityNames":  {MaxRawCapabilityNames, 64},
		"MaxDTypesPerNode":       {MaxDTypesPerNode, 64},
		"MaxDTypeLen":            {MaxDTypeLen, 64},
		"DatagramPubKeyBytes":    {DatagramPubKeyBytes, 32},
		"DatagramSaltBytes":      {DatagramSaltBytes, 16},
		"DatagramSigBytes":       {DatagramSigBytes, 64},
	}
	for name, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %d, want %d", name, check.got, check.want)
		}
	}
	if DatagramBaseReplayWindow != 5*time.Minute {
		t.Errorf("DatagramBaseReplayWindow = %s, want 5m", DatagramBaseReplayWindow)
	}
	if DatagramFreshnessWindow != 5*time.Minute {
		t.Errorf("DatagramFreshnessWindow = %s, want 5m", DatagramFreshnessWindow)
	}
}

func TestParseNetworkID(t *testing.T) {
	got, err := ParseNetworkID("gazeta-devnet")
	if err != nil {
		t.Fatalf("ParseNetworkID: %v", err)
	}
	if got.String() != "gazeta-devnet" {
		t.Fatalf("String() = %q, want %q", got.String(), "gazeta-devnet")
	}
	// §3.2 signs the network as UTF-8 without a BOM: an empty id makes every
	// network sign alike, a BOM makes one network sign two ways.
	for name, input := range map[string]string{
		"empty":        "",
		"bom":          "\ufeffgazeta-devnet",
		"invalid utf8": string([]byte{0xff, 0xfe}),
	} {
		if _, err := ParseNetworkID(input); !errors.Is(err, ErrInvalidNetworkID) {
			t.Fatalf("ParseNetworkID(%s): error = %v, want ErrInvalidNetworkID", name, err)
		}
	}
}

// TestParseRawCapabilityNames pins the all-or-nothing reaction: a breach of
// the bounds empties the whole raw set, because "drop one name" and "drop the
// set" would make two implementations disagree about which peer is a datagram
// candidate at all.
func TestParseRawCapabilityNames(t *testing.T) {
	known := []string{"mesh_datagram_v1", "mesh_datagram_transit_v1", "some_future_profile_v3"}
	got := ParseRawCapabilityNames(known)
	if len(got) != len(known) {
		t.Fatalf("raw set = %d names, want %d", len(got), len(known))
	}
	for i, name := range known {
		if got[i].String() != name {
			t.Fatalf("raw set[%d] = %q, want %q", i, got[i], name)
		}
	}
	if ParseRawCapabilityNames(nil) == nil && len(ParseRawCapabilityNames([]string{})) != 0 {
		t.Fatal("an empty advertised set must stay empty, not become invalid")
	}

	overLength := make([]string, MaxRawCapabilityNames+1)
	for i := range overLength {
		name := "cap_"
		for j := 0; j <= i; j++ {
			name += "a"
		}
		overLength[i] = name
	}
	for name, input := range map[string][]string{
		"too many names": overLength,
		"bad charset":    {"mesh_datagram_v1", "mesh-datagram-v2"},
		"empty name":     {"mesh_datagram_v1", ""},
		"oversized name": {strings.Repeat("a", MaxCapabilityNameLen+1)},
	} {
		if got := ParseRawCapabilityNames(input); got != nil {
			t.Fatalf("%s: raw set = %v, want the whole set dropped", name, got)
		}
	}
}

func TestReplayKey(t *testing.T) {
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = byte(i)
	}
	key, err := ReplayKeyFromBytes(digest)
	if err != nil {
		t.Fatalf("ReplayKeyFromBytes: %v", err)
	}
	if key.IsZero() {
		t.Fatal("a derived key must not read as zero")
	}
	parsed, err := ParseReplayKey(key.String())
	if err != nil {
		t.Fatalf("ParseReplayKey: %v", err)
	}
	if parsed != key {
		t.Fatal("hex round trip changed the key")
	}
	if (ReplayKey{}).String() != "" {
		t.Fatal("the zero key must render as an empty string, not 64 zeros")
	}
	for name, input := range map[string]string{
		"too short": key.String()[:63],
		"uppercase": strings.ToUpper(key.String()),
		"empty":     "",
	} {
		if _, err := ParseReplayKey(input); !errors.Is(err, ErrInvalidReplayKey) {
			t.Fatalf("ParseReplayKey(%s): error = %v, want ErrInvalidReplayKey", name, err)
		}
	}
	if _, err := ReplayKeyFromBytes(digest[:16]); !errors.Is(err, ErrInvalidReplayKey) {
		t.Fatalf("ReplayKeyFromBytes(16 bytes): error = %v, want ErrInvalidReplayKey", err)
	}
}

// TestParseDeclaredDTypes pins the closed wire contract of §6.1 as the domain
// enforces it, INCLUDING the third state the field carries: an absent field is
// the baseline set, an explicitly empty one is the empty set, duplicates
// collapse into a set, and any breach of the bounds drops the WHOLE field
// rather than the offending name — half-honouring a breached list is exactly
// the ambiguity a closed contract exists to remove.
func TestParseDeclaredDTypes(t *testing.T) {
	absent := ParseDeclaredDTypesField(nil)
	if absent.Declaration() != DTypeDeclarationAbsent {
		t.Fatalf("absent field = %s, want %s", absent.Declaration(), DTypeDeclarationAbsent)
	}
	if absent.Len() != 0 || absent.WireField() != nil {
		t.Fatalf("absent field carries %d names and renders as %v, want none and nil", absent.Len(), absent.WireField())
	}

	// An empty ARRAY is the statement "the envelope yes, handlers no". It must
	// never collapse into the absent form: that collapse is what left a node
	// with an empty type registry unable to say anything at all.
	for name, empty := range map[string]DeclaredDTypeSet{
		"parsed list": ParseDeclaredDTypes([]string{}),
		"wire field":  ParseDeclaredDTypesField(&[]string{}),
		"explicit":    ExplicitDTypes(nil),
	} {
		if empty.Declaration() != DTypeDeclarationExplicit {
			t.Fatalf("%s: empty field = %s, want %s — an empty array is a statement, not a missing field",
				name, empty.Declaration(), DTypeDeclarationExplicit)
		}
		if empty.Len() != 0 {
			t.Fatalf("%s: empty field carries %d names, want none", name, empty.Len())
		}
		field := empty.WireField()
		if field == nil {
			t.Fatalf("%s: the empty set rendered as an absent field", name)
		}
		if len(*field) != 0 {
			t.Fatalf("%s: the empty set rendered as %v", name, *field)
		}
	}

	got := ParseDeclaredDTypes([]string{"push_identity", "get_identity", "push_identity"})
	if got.Declaration() != DTypeDeclarationExplicit {
		t.Fatalf("a field that was sent = %s, want %s", got.Declaration(), DTypeDeclarationExplicit)
	}
	names := got.Types()
	if len(names) != 2 {
		t.Fatalf("declared set = %v, want 2 names after the duplicate collapsed", names)
	}
	if names[0] != DType("push_identity") || names[1] != DType("get_identity") {
		t.Fatalf("declared set = %v, want first-appearance order preserved", names)
	}
	// Types() hands out a copy: the set is fixed for the lifetime of the
	// session, so no reader may grow another reader's view of it.
	names[0] = "smuggled"
	if got.Types()[0] != DType("push_identity") {
		t.Fatal("mutating the returned slice changed the stored set")
	}

	overLength := make([]string, MaxDTypesPerNode+1)
	for i := range overLength {
		overLength[i] = "dtype_" + strings.Repeat("a", i%9+1)
	}
	for name, input := range map[string][]string{
		"too many names": overLength,
		"bad charset":    {"get_identity", "Get-Identity"},
		"empty name":     {"get_identity", ""},
		"oversized name": {strings.Repeat("a", MaxDTypeLen+1)},
	} {
		breached := ParseDeclaredDTypes(input)
		if breached.Declaration() != DTypeDeclarationAbsent {
			t.Fatalf("%s: declaration = %s, want the whole field dropped to %s (hence to the baseline)",
				name, breached.Declaration(), DTypeDeclarationAbsent)
		}
		if breached.Len() != 0 {
			t.Fatalf("%s: %d names survived a bounds breach", name, breached.Len())
		}
	}
}

// TestDeclaredDTypeSetCloneKeepsTheDeclaration pins that the state travels
// with the value into storage. A clone that lost it would turn "no handlers"
// back into "the baseline four" the moment a session mirrored the set.
func TestDeclaredDTypeSetCloneKeepsTheDeclaration(t *testing.T) {
	empty := ParseDeclaredDTypes(nil).Clone()
	if empty.Declaration() != DTypeDeclarationExplicit {
		t.Fatalf("cloned empty set = %s, want %s", empty.Declaration(), DTypeDeclarationExplicit)
	}
	absent := AbsentDTypes().Clone()
	if absent.Declaration() != DTypeDeclarationAbsent || absent.WireField() != nil {
		t.Fatalf("cloned absent set = %s rendering as %v, want absent and nil", absent.Declaration(), absent.WireField())
	}
	listed := ExplicitDTypes([]DType{"file_transfer"}).Clone()
	if listed.Len() != 1 || listed.Types()[0] != DType("file_transfer") {
		t.Fatalf("cloned set = %v, want the single name it carried", listed.Types())
	}
}
