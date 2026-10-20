package node

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func ninCodecImage() *ninJournalImage {
	return &ninJournalImage{domain: [16]byte{1}, generation: 1, phase: ninKnown, rows: []ninRow{{}}}
}
func TestInboundN0CodecCanonicalGroupsAndHostileImages(t *testing.T) {
	for _, group := range []domain.SourceGroup{domain.SourceGroupLoopback, domain.SourceGroupTor, domain.SourceGroupI2P, domain.SourceGroupUnresolvedName, "192.0.0.0/16", "2001:db8::/32"} {
		key, err := ninKeyOf(group)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ninGroupOf(key)
		if err != nil || got != group {
			t.Fatal(group, got, err)
		}
	}
	for _, bad := range []domain.SourceGroup{"192.0.1.0/16", "2001:0db8::/32", "127.0.0.1", "fe80::1%en0/32", "not-a-group"} {
		if _, err := ninKeyOf(bad); err == nil {
			t.Fatal("noncanonical group accepted", bad)
		}
	}
	good := ninCodecImage()
	good.tick = 10
	good.rows[0].count = 2
	good.rows[0].ticks[0] = 10
	good.rows[0].ticks[1] = 10
	raw, err := encodeNinImage(good)
	if err != nil {
		t.Fatal(err)
	}
	copyImage, err := decodeNinImage(raw)
	if err != nil || !reflect.DeepEqual(good, copyImage) {
		t.Fatal("round trip", err)
	}
	for _, mutate := range []func(*ninJournalImage){func(i *ninJournalImage) { i.domain = [16]byte{} }, func(i *ninJournalImage) { i.generation = 0 }, func(i *ninJournalImage) { i.phase = 99 }, func(i *ninJournalImage) { i.phase = ninUnknown }, func(i *ninJournalImage) { i.rows[0].count = 65 }, func(i *ninJournalImage) { i.rows[0].reserved[0] = 1 }, func(i *ninJournalImage) { i.rows[0].ticks[2] = 1 }, func(i *ninJournalImage) { i.tick = 9 }, func(i *ninJournalImage) { i.tick = ninWindow + 10 }, func(i *ninJournalImage) { i.rows = append(i.rows, i.rows[0]) }} {
		image, _ := decodeNinImage(raw)
		mutate(image)
		if _, err := encodeNinImage(image); err == nil {
			t.Fatal("invalid image accepted")
		}
	}
	for _, offset := range []int{0, 2, 3, 20, 36, 38, 40, 72, 73, 79} {
		bad := bytes.Clone(raw)
		bad[offset] ^= 255
		if _, err := decodeNinImage(bad); err == nil {
			t.Fatalf("corruption at%d accepted", offset)
		}
	}
	bad := bytes.Clone(raw)
	binary.BigEndian.PutUint16(bad[38:40], 4097)
	if _, err := decodeNinImage(bad); err == nil {
		t.Fatal("row overflow")
	}
	jsonRaw, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`null`, `" "`, `"` + strings.Repeat("A", ninMaxEncoded+4) + `"`, string(jsonRaw[:len(jsonRaw)-2]) + `\n"`, `"` + base64.RawStdEncoding.EncodeToString(raw) + `"`} {
		var image ninJournalImage
		if err := json.Unmarshal([]byte(bad), &image); err == nil {
			t.Fatal("noncanonical/oversize JSON accepted")
		}
	}
}
func TestInboundN0CodecMaximumBoundAndSnapshotNoAlias(t *testing.T) {
	i := ninCodecImage()
	i.rows = make([]ninRow, ninGroupCapacity)
	for n := range i.rows {
		if n > 0 {
			i.rows[n].key = ninKey{1, byte(n >> 8), byte(n)}
		}
		i.rows[n].count = 64
	}
	packed, err := encodeNinImage(i)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) != 2129992 {
		t.Fatal(len(packed))
	}
	encoded, err := json.Marshal(i)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded)+21 != 2840015 {
		t.Fatal("exact member bound", len(encoded)+21)
	}
	j := ninJournalFromImage(i)
	snapshot := j.image()
	snapshot.rows[1].ticks[0] = 100
	if j.row(1).ticks[0] != 0 || i.rows[1].ticks[0] != 0 {
		t.Fatal("snapshot mutable alias")
	}
	j.arena[2].ticks[0] = 99
	if snapshot.rows[2].ticks[0] != 0 || i.rows[2].ticks[0] != 0 {
		t.Fatal("owner aliases imported image")
	}
}
func TestInboundN0ReservedLoopbackAndHistoryDiagnostics(t *testing.T) {
	s, c, _ := ninUnknownFixture(t)
	c.advance(24 * time.Hour)
	s.mu.Lock()
	s.sampleNinQuotaLocked()
	j := s.ninJournal
	j.confirmed = j.stamp()
	j.arena = new([ninGroupCapacity]ninRow)
	j.rows = ninGroupCapacity
	for n := 1; n < ninGroupCapacity; n++ {
		j.arena[n] = ninRow{key: ninKey{1, byte(n >> 8), byte(n)}, count: 2}
		j.arena[n].ticks[0] = j.tick - uint64(time.Hour)
		j.arena[n].ticks[1] = j.tick
	}
	s.mu.Unlock()
	status := s.inboundQuotaStatus("2001:db8::/32")
	if status.Reason != domain.InboundQuotaHistoryFull || status.RemainingObserved == nil || *status.RemainingObserved != 24*time.Hour || status.Groups != 4096 {
		t.Fatal("history uses first instead of last event or wrong row budget", status)
	}
	if got := s.inboundQuotaStatus(domain.SourceGroupLoopback); got.Reason != domain.InboundQuotaReady {
		t.Fatal("loopback reservation lost", got)
	}
	c.advance(24 * time.Hour)
	if got := s.inboundQuotaStatus("2001:db8::/32"); got.Reason != domain.InboundQuotaReady || got.Groups != 1 || got.Events != 0 {
		t.Fatal("expired dynamic rows retained", got)
	}
}
func TestInboundN0NewMemberDuplicateAndAliasRejection(t *testing.T) {
	s, _, _ := ninUnknownFixture(t)
	file := ninReadFileFixture(t, s)
	raw, _ := json.Marshal(file)
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	journal := string(value["inbound_pin_quota"])
	for _, member := range []string{`"inbound_pin_quota":` + journal, `"Inbound_Pin_Quota":` + journal, `"inbound_pin_quot\u0061":` + journal, `"format":3`, `"FORMAT":3`, `"for\u006dat":3`} {
		bad := append(bytes.Clone(raw[:len(raw)-1]), []byte(","+member+"}")...)
		if _, err := decodeSecureSessionFile(bad); err == nil {
			t.Fatal("duplicate/alias escaped authority", member[:10])
		}
	}
}

// The generic inventory forbids installation, witness and admission roots.
// The caller normalizes only exact reviewed N1 leaves and the two N2 Unknown
// attachment sites; there is no file-wide/default-constructor allowance.
func ninClosedSource(src []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "candidate.go", src, 0)
	if err != nil {
		return err
	}
	var refused error
	ast.Inspect(file, func(n ast.Node) bool {
		if refused != nil {
			return false
		}
		switch n := n.(type) {
		case *ast.SelectorExpr:
			if n.Sel.Name == "stageInboundPin" {
				refused = fmt.Errorf("live admission reference")
			}
		case *ast.CompositeLit:
			if name, ok := n.Type.(*ast.Ident); ok && (name.Name == "matureInboundWitness" || name.Name == "ninJournal") {
				refused = fmt.Errorf("live witness/installation constructor")
			}
		case *ast.CallExpr:
			if name, ok := n.Fun.(*ast.Ident); ok && name.Name == "new" && len(n.Args) == 1 {
				if typ, ok := n.Args[0].(*ast.Ident); ok && typ.Name == "ninJournal" {
					refused = fmt.Errorf("live installation new constructor")
				}
			}
		case *ast.TypeSpec:
			if n.Assign.IsValid() {
				ast.Inspect(n.Type, func(v ast.Node) bool {
					if id, ok := v.(*ast.Ident); ok && id.Name == "ninJournal" {
						refused = fmt.Errorf("live journal type alias")
					}
					return true
				})
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "ninFormat3" {
					refused = fmt.Errorf("live installation assignment")
				}
			}
		}
		return true
	})
	return refused
}
func TestInboundN0ClosedProductionReferenceInventory(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		// Exactly one load attachment and one journal-from-image allocator are the
		// existing-file reader, not installation. Match their AST body separately.
		if name == "secure_session_store.go" || name == "identity_pin_inbound_journal.go" {
			literal, replacement, allowed := []byte("s.ninFormat3 = true"), []byte("s.loadedFormatSentinel = true"), "loadFile"
			if name == "identity_pin_inbound_journal.go" {
				literal = []byte("&ninJournal{")
				replacement = []byte("&loadedNinJournal{")
				allowed = "ninJournalFromImage"
			}
			if bytes.Count(raw, literal) != 1 {
				t.Fatal("changed load-only authority inventory", name)
			}
			start := bytes.Index(raw, literal)
			fs := token.NewFileSet()
			file, err := parser.ParseFile(fs, name, raw, 0)
			if err != nil {
				t.Fatal(err)
			}
			inside := false
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == allowed {
					inside = start >= fs.Position(fn.Body.Pos()).Offset && start < fs.Position(fn.Body.End()).Offset
				}
			}
			if !inside {
				t.Fatal("load-only authority moved to live function", name)
			}
			raw = bytes.Replace(raw, literal, replacement, 1)
		}
		// Exact N1 and N2 roots are normalized only at reviewed AST function/sites,
		// never by excluding an entire lifecycle/installation source file.
		for _, rule := range []struct{ file, function, literal, replacement string }{
			{"identity_pin_inbound_lifecycle.go", "attempt", ".stageInboundPin(", ".reviewedPairedInboundStage("},
			{"session_secure.go", "acceptSecureInbound", "&matureInboundWitness{", "&reviewedOriginalInboundLeaf{"},
			{"identity_pin_inbound_install.go", "installUnknownInboundPinJournal", "s.ninFormat3 = true", "s.reviewedUnknownFormatSentinel = true"},
			{"identity_pin_inbound_install.go", "installUnknownInboundPinJournal", "&ninJournal{", "&reviewedUnknownJournal{"},
		} {
			if name != rule.file {
				continue
			}
			old := []byte(rule.literal)
			if bytes.Count(raw, old) != 1 {
				t.Fatal("changed exact N1 authority inventory", name, rule.function)
			}
			offset := bytes.Index(raw, old)
			fs := token.NewFileSet()
			parsed, err := parser.ParseFile(fs, name, raw, 0)
			if err != nil {
				t.Fatal(err)
			}
			inside := false
			for _, decl := range parsed.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == rule.function && fn.Body != nil {
					inside = offset >= fs.Position(fn.Body.Pos()).Offset && offset < fs.Position(fn.Body.End()).Offset
				}
			}
			if !inside {
				t.Fatal("N1 root moved outside exact allowed function", name)
			}
			raw = bytes.Replace(raw, old, []byte(rule.replacement), 1)
		}
		if err := ninClosedSource(raw); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for _, body := range []string{`func bad(s *secureSessionStore){s.stageInboundPin(nil,nil,nil)}`, `var escaped=(*secureSessionStore).stageInboundPin`, `var forged=&matureInboundWitness{}`, `func bad(s *secureSessionStore){s.ninFormat3=true}`, `var forged=&ninJournal{}`, `func bad(){_=new(ninJournal)}`, `type Escape = ninJournal`, `type Escape = *ninJournal`} {
		if err := ninClosedSource([]byte("package node;" + body)); err == nil {
			t.Fatal("closed guard accepted production escape", body)
		}
	}
}

func ninReferenceErrors(sources map[string][]byte) []string {
	type site struct{ file, function string }
	expected := map[string]map[site]int{
		"stageInboundPin": {{"identity_pin_inbound_lifecycle.go", "attempt"}: 1}, "ninInstallFixture": {}, "ninInstallVirginFixture": {}, "ninInstallUnknownFixture": {}, "ninProofFixture": {}, "ninProofForFixture": {},
		"ninJournalFromImage":             {{"secure_session_store.go", "loadFile"}: 1},
		"installUnknownInboundPinJournal": {{"identity_pin_inbound_lifecycle.go", "newUnknownInboundPinCandidateService"}: 1},
		"matureInboundWitness":            {{"conn_registry.go", "selectInboundPinAfterLocked"}: 1, {"identity_pin_inbound.go", "<package>"}: 1, {"identity_pin_inbound_lifecycle.go", "attempt"}: 1, {"proof_transport_lifetime.go", "<package>"}: 1, {"session_secure.go", "acceptSecureInbound"}: 1},
	}
	actual := make(map[string]map[site]int)
	for name := range expected {
		actual[name] = map[site]int{}
	}
	var failures []string
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range file.Decls {
			where := site{path, "<package>"}
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				where.function = fn.Name.Name
				body = fn.Body
				if strings.HasPrefix(fn.Name.Name, "ninInstall") || strings.HasPrefix(fn.Name.Name, "ninProof") {
					failures = append(failures, "fixture-only root moved to production")
				}
			}
			if body == nil {
				continue
			}
			ast.Inspect(body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if refs, tracked := actual[id.Name]; tracked {
						refs[where]++
					}
				}
				return true
			})
		}
	}
	for name, want := range expected {
		if !reflect.DeepEqual(actual[name], want) {
			failures = append(failures, name+" reference multiset changed")
		}
	}
	return failures
}
func TestInboundN0ExactClosedAuthorityReferences(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = raw
	}
	if errors := ninReferenceErrors(sources); len(errors) != 0 {
		t.Fatal(errors)
	}
	for _, src := range []string{`func bad(s *secureSessionStore){s.stageInboundPin(nil,nil,nil)}`, `var escaped=(*secureSessionStore).stageInboundPin`, `func bad(){_ = ninJournalFromImage}`, `func ninInstallFixture(){}`, `func bad(){_=new(matureInboundWitness)}`, `var escaped=(*secureSessionStore).installUnknownInboundPinJournal`} {
		compileSource := "package fixture; type secureSessionStore struct{}; func (*secureSessionStore) stageInboundPin(any,any,any){}; func (*secureSessionStore) installUnknownInboundPinJournal(any,any){}; type matureInboundWitness struct{}; func ninJournalFromImage(){};" + src
		fs := token.NewFileSet()
		parsed, parseErr := parser.ParseFile(fs, "counterexample.go", compileSource, 0)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		cfg := types.Config{}
		if _, typeErr := cfg.Check("fixture", fs, []*ast.File{parsed}, nil); typeErr != nil {
			t.Fatal("counterexample must compile", typeErr)
		}
		sources["unexpected.go"] = []byte("package node;" + src)
		if len(ninReferenceErrors(sources)) == 0 {
			t.Fatal("authority function-value/caller escape accepted", src)
		}
	}
}

func TestInboundN0ActualAddressNormalizationUsesReservedLoopback(t *testing.T) {
	for _, literal := range []string{"127.200.1.2", "::1", "::ffff:127.2.1.1"} {
		key, err := ninKeyOf(sourceGroup(netip.MustParseAddr(literal)))
		if err != nil || key != (ninKey{}) {
			t.Fatal("loopback became dynamic group", literal, key, err)
		}
	}
	key, err := ninKeyOf(sourceGroup(netip.MustParseAddr("::ffff:192.0.2.10")))
	if err != nil || key != (ninKey{1, 192, 0}) {
		t.Fatal("mapped IPv4 misgrouped", key, err)
	}
}

func TestInboundN0LegacyCannotHideJournalPresenceAsNull(t *testing.T) {
	for _, format := range []int{1, 2} {
		raw := fmt.Sprintf(`{"format":%d,"identities":{},"identity_metadata":{},"endpoints":{},"inbound_pin_quota":null}`, format)
		if _, err := decodeSecureSessionFile([]byte(raw)); err == nil {
			t.Fatal("legacy accepted journal member null", format)
		}
	}
}
