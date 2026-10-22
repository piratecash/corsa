package findnodebridge

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func domainPrinted(node any) string {
	var b bytes.Buffer
	if err := format.Node(&b, token.NewFileSet(), node); err != nil {
		return "<format error>"
	}
	return strings.Join(strings.Fields(b.String()), " ")
}
func domainSourceFiles(t *testing.T) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	for _, name := range []string{"domain.go", "clock.go", "construction_point.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = raw
	}
	return files
}

// Match the literal which is actually returned, rather than any origin-shaped
// text elsewhere in the constructor. This is a closed SOURCE contract.
func domainRuntimeOriginOwned(fn *ast.FuncDecl) bool {
	ident := func(expr ast.Expr, want string) bool {
		name, ok := expr.(*ast.Ident)
		return ok && name.Name == want
	}
	if fn.Recv != nil || fn.Name.Name != "NewRuntimeDomain" || domainPrinted(fn.Type) != "func(done <-chan struct{}) (RuntimeDomain, error)" || fn.Body == nil || len(fn.Body.List) != 3 {
		return false
	}
	if domainPrinted(fn.Body.List[0]) != "if done == nil { return RuntimeDomain{}, errInvalid }" {
		return false
	}
	assignment, ok := fn.Body.List[1].(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || !ident(assignment.Lhs[0], "d") || len(assignment.Rhs) != 1 {
		return false
	}
	address, ok := assignment.Rhs[0].(*ast.UnaryExpr)
	if !ok || address.Op != token.AND {
		return false
	}
	literal, ok := address.X.(*ast.CompositeLit)
	if !ok || !ident(literal.Type, "domain") || len(literal.Elts) != 5 {
		return false
	}
	fields := make(map[string]ast.Expr)
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return false
		}
		key, ok := field.Key.(*ast.Ident)
		if !ok || fields[key.Name] != nil {
			return false
		}
		switch key.Name {
		case "kind", "done", "origin", "phase", "generation":
			fields[key.Name] = field.Value
		default:
			return false
		}
	}
	if !ident(fields["kind"], "runtimeKind") || !ident(fields["done"], "done") || !ident(fields["phase"], "constructing") {
		return false
	}
	generation, ok := fields["generation"].(*ast.BasicLit)
	if !ok || generation.Kind != token.INT || generation.Value != "1" {
		return false
	}
	origin, ok := fields["origin"].(*ast.CallExpr)
	if !ok || len(origin.Args) != 0 || origin.Ellipsis.IsValid() {
		return false
	}
	call, ok := origin.Fun.(*ast.SelectorExpr)
	if !ok || !ident(call.X, "time") || call.Sel.Name != "Now" {
		return false
	}
	returned, ok := fn.Body.List[2].(*ast.ReturnStmt)
	if !ok || len(returned.Results) != 2 || !ident(returned.Results[1], "nil") {
		return false
	}
	wrapper, ok := returned.Results[0].(*ast.CompositeLit)
	if !ok || !ident(wrapper.Type, "RuntimeDomain") || len(wrapper.Elts) != 1 {
		return false
	}
	bound, ok := wrapper.Elts[0].(*ast.KeyValueExpr)
	return ok && ident(bound.Key, "runtimeDomain") && ident(bound.Value, "d")
}
func domainSourceErrors(raw map[string][]byte) []string {
	expected := map[string]string{
		"NewRuntimeDomain":        "func(done <-chan struct{}) (RuntimeDomain, error)",
		"NewFixtureDomain":        "func(done <-chan struct{}, clock *atomic.Int64) (FixtureDomain, error)",
		"RuntimeDomain.Publisher": "func() Publisher", "RuntimeDomain.Observer": "func() Observer", "RuntimeDomain.Consumer": "func() Consumer",
		"FixtureDomain.Publisher": "func() Publisher", "FixtureDomain.Observer": "func() Observer", "FixtureDomain.Consumer": "func() Consumer",
		"Observer.BeginCapture": "func() (CaptureStamp, error)", "Observer.ReserveClock": "func() (*ObservationClock, error)",
		"Consumer.TryReadClock":         "func(stamp CaptureStamp) (*CommitReservation, error)",
		"Consumer.ReserveAcceptedClock": "func() (CaptureStamp, *CommitReservation, error)",
		"Publisher.BeginPublication":    "func() *Publication", "Publisher.BeginClockedPublication": "func() *ClockPublication", "Publisher.SealConstruction": "func() error",
		"ObservationClock.Sample": "func() (OwnedInstant, error)", "CommitReservation.Sample": "func() (OwnedInstant, error)", "ClockPublication.Sample": "func() (OwnedInstant, error)",
		"ClockPublication.SampleRuntimePair": "func() (OwnedInstant, time.Time, error)",
		"ObservationClock.Release":           "func()", "CommitReservation.Release": "func()", "Publication.Complete": "func()", "ClockPublication.Complete": "func()",
		"OwnedInstant.Elapsed": "func() time.Duration", "OwnedInstant.NotBefore": "func(other OwnedInstant) bool", "OwnedInstant.BoundaryAfter": "func(delta time.Duration) (OwnedBoundary, error)",
		"OwnedBoundary.Elapsed": "func() time.Duration", "OwnedBoundary.Contains": "func(at OwnedInstant) bool",
		"Consumer.IsRuntime": "func() bool", "Consumer.IsFixture": "func() bool", "Consumer.Matches": "func(stamp CaptureStamp, at OwnedInstant) bool",
		"Consumer.MatchesPoint": "func(at OwnedInstant) bool",
	}
	wrappers := map[string]string{
		"RuntimeDomain": "runtimeDomain *domain", "FixtureDomain": "fixtureDomain *domain",
		"Publisher": "publisherBinding *roleBinding", "Observer": "observerBinding *roleBinding", "Consumer": "consumerBinding *roleBinding",
		"ObservationClock": "observationLease *leaseCell", "CommitReservation": "commitLease *leaseCell", "Publication": "publicationLease *leaseCell", "ClockPublication": "clockPublicationLease *leaseCell",
		"CaptureStamp":  "captureDomain *domain; captureKind domainKind; generation uint64",
		"OwnedInstant":  "instantDomain *domain; instantKind domainKind; instantElapsed time.Duration",
		"OwnedBoundary": "boundaryDomain *domain; boundaryKind domainKind; boundaryElapsed time.Duration",
	}
	allowedImports := map[string]bool{"errors": true, "math": true, "sync": true, "sync/atomic": true, "time": true}
	bodies, seen, types := map[string]string{}, map[string]int{}, map[string]int{}
	rawCalls := map[string]int{}
	runtimeConstructors, runtimeOriginOwned := 0, false
	var failures []string
	for _, name := range []string{"domain.go", "clock.go", "construction_point.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw[name], 0)
		if err != nil {
			failures = append(failures, "source parse: "+name)
			continue
		}
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil || !allowedImports[path] || imp.Name != nil {
				failures = append(failures, "nonstandard or aliased leaf import")
			}
		}
		rawSites := map[*ast.SelectorExpr]string{}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.Name == "NewRuntimeDomain" && d.Recv == nil {
					runtimeConstructors++
					runtimeOriginOwned = name == "domain.go" && domainRuntimeOriginOwned(d)
				}
				key := d.Name.Name
				if d.Recv != nil {
					key = strings.TrimPrefix(domainPrinted(d.Recv.List[0].Type), "*") + "." + key
				}
				bodies[key] = domainPrinted(d.Body)
				if d.Body == nil {
					failures = append(failures, "missing leaf function body")
					continue
				}
				// Attribute references to exact declarations, excluding nested callbacks.
				// The later whole-file walk also sees package ValueSpec initializers,
				// method expressions and any reference outside these direct bodies.
				ast.Inspect(d, func(n ast.Node) bool {
					if _, nested := n.(*ast.FuncLit); nested {
						return false
					}
					if selector, ok := n.(*ast.SelectorExpr); ok && selector.Sel.Name == "sampleLocked" {
						rawSites[selector] = key
					}
					return true
				})
				if ast.IsExported(d.Name.Name) {
					seen[key]++
					if expected[key] != domainPrinted(d.Type) {
						failures = append(failures, "closed public API shape: "+key)
					}
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if !ast.IsExported(s.Name.Name) {
							continue
						}
						types[s.Name.Name]++
						if _, allowed := wrappers[s.Name.Name]; !allowed {
							failures = append(failures, "extra exported type: "+s.Name.Name)
						}
						shape, ok := s.Type.(*ast.StructType)
						if !ok {
							failures = append(failures, "exported alias or nonopaque type")
							continue
						}
						var fields []string
						for _, f := range shape.Fields.List {
							if len(f.Names) != 1 || ast.IsExported(f.Names[0].Name) {
								failures = append(failures, "exported or embedded authority field")
								continue
							}
							fields = append(fields, f.Names[0].Name+" "+domainPrinted(f.Type))
						}
						if wrappers[s.Name.Name] != strings.Join(fields, "; ") {
							failures = append(failures, "copyable wrapper ownership shape: "+s.Name.Name)
						}
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if ast.IsExported(n.Name) {
								failures = append(failures, "extra exported value")
							}
						}
					}
				}
			}
		}
		// Raw reference accounting is over the WHOLE file, never Body alone.
		callees := map[*ast.SelectorExpr]*ast.CallExpr{}
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					callees[selector] = call
				}
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			selector, ok := n.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "sampleLocked" {
				return true
			}
			site := rawSites[selector]
			if site == "" {
				site = "<package-or-literal>"
			}
			rawCalls[name+":"+site]++
			call := callees[selector]
			if call == nil {
				failures = append(failures, "raw sample method-value escape")
			} else {
				owner, ownerOK := selector.X.(*ast.SelectorExpr)
				var cell *ast.Ident
				if ownerOK {
					cell, _ = owner.X.(*ast.Ident)
				}
				if !ownerOK || owner.Sel.Name != "owner" || cell == nil || cell.Name != "cell" || len(call.Args) != 0 {
					failures = append(failures, "raw sample must use direct cell.owner call")
				}
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.GoStmt, *ast.ForStmt, *ast.RangeStmt:
				failures = append(failures, "worker or loop in leaf primitive")
			}
			return true
		})
	}
	for key := range expected {
		if seen[key] != 1 {
			failures = append(failures, "missing or duplicate public API: "+key)
		}
	}
	for key := range wrappers {
		if types[key] != 1 {
			failures = append(failures, "missing or duplicate opaque wrapper: "+key)
		}
	}
	require := func(key, fragment, label string) {
		if !strings.Contains(bodies[key], strings.Join(strings.Fields(fragment), " ")) {
			failures = append(failures, label)
		}
	}
	if runtimeConstructors != 1 || !runtimeOriginOwned {
		failures = append(failures, "runtime monotonic origin must be constructor-owned")
	}
	require("domain.sampleLocked", "if d.poisoned.Load() { return OwnedInstant{}, time.Time{}, errUnavailable } d.last = q", "post-raw poison before HWM and mint")
	sample := bodies["domain.sampleLocked"]
	rawQ, validity, post, last := strings.Index(sample, "var q time.Duration"), strings.Index(sample, "if q < 0"), strings.LastIndex(sample, "if d.poisoned.Load()"), strings.Index(sample, "d.last = q")
	if rawQ < 0 || validity <= rawQ || post <= validity || last <= post || strings.Count(sample, "d.poisoned.Load()") != 2 || strings.Contains(sample, "phase") || strings.Contains(sample, ".Lock(") {
		failures = append(failures, "uniform sample ordering or hidden lock/phase")
	}
	require("domain.sampleLocked", "if d.kind == runtimeKind { sample := time.Now() q = sample.Sub(d.origin) wall = sample.UTC() } else { q = time.Duration(d.fixture.Load()) }", "runtime pair must use one original read")
	if strings.Count(sample, "time.Now()") != 1 || strings.Contains(sample, "time.Since(") {
		failures = append(failures, "runtime pair has independent clock read")
	}
	require("domain.sampleLocked", "return OwnedInstant{instantDomain: d, instantKind: d.kind, instantElapsed: q}, wall, nil", "pair must return original wall metadata")
	if !reflect.DeepEqual(rawCalls, map[string]int{"clock.go:ObservationClock.Sample": 1, "clock.go:CommitReservation.Sample": 1, "clock.go:ClockPublication.Sample": 1, "clock.go:ClockPublication.SampleRuntimePair": 1}) {
		failures = append(failures, "shared raw sample caller set changed")
	}
	require("leaseCell.beginSample", "c.state.CompareAndSwap(leaseLive, leaseBusy)", "missing shared BUSY arbitration")
	require("leaseCell.terminate", "if !c.valid(role) || !c.state.CompareAndSwap(leaseLive, leaseClosing) { panic(errLease) }", "terminal must claim LIVE before effects")
	require("leaseCell.terminate", "case clockPublicationRole: d.advanceGenerationLocked() d.auth.Unlock() d.clock.Unlock()", "clocked generation and release order")
	require("leaseCell.terminate", "case commitRole: d.clock.Unlock() d.auth.RUnlock()", "commit release order")
	require("leaseCell.terminate", "c.state.Store(leaseClosed)", "terminal cannot reopen cell")
	require("Publisher.BeginClockedPublication", "b.owner.clock.Lock() b.owner.auth.Lock()", "clock must precede W")
	require("Publisher.BeginPublication", "if b == nil { return &Publication{publicationLease: newLease(nil, publicationRole, true)} }", "zero publisher cleanup cannot veto")
	require("Publisher.BeginClockedPublication", "if b == nil { return &ClockPublication{clockPublicationLease: newLease(nil, clockPublicationRole, true)} }", "zero clock publisher cleanup cannot veto")
	require("Publisher.BeginPublication", "guard := &Publication{publicationLease: newLease(b.owner, publicationRole, false)} b.owner.auth.Lock() return guard", "publication guard must be allocated before W")
	require("Publisher.BeginClockedPublication", "guard := &ClockPublication{clockPublicationLease: newLease(b.owner, clockPublicationRole, false)} b.owner.clock.Lock() b.owner.auth.Lock() return guard", "clocked guard must be allocated before clock and W")

	require("Consumer.ReserveAcceptedClock", "guard := &CommitReservation{commitLease: newLease(d, commitRole, false)} d.clock.Lock() d.auth.RLock()", "accepted original guard preallocation and clock-before-R")
	require("Consumer.ReserveAcceptedClock", "if !d.readableLocked() { d.auth.RUnlock() d.clock.Unlock() return CaptureStamp{}, nil, errUnavailable }", "accepted unavailable path releases original locks")
	require("Consumer.ReserveAcceptedClock", "stamp := CaptureStamp{captureDomain: d, captureKind: b.kind, generation: d.generation} return stamp, guard, nil", "accepted current stamp only after R")
	if strings.Contains(bodies["Consumer.ReserveAcceptedClock"], ".done") || strings.Contains(bodies["Consumer.ReserveAcceptedClock"], "TryLock") || strings.Contains(bodies["Consumer.ReserveAcceptedClock"], "TryRLock") {
		failures = append(failures, "accepted lease must not revoke custody through transient try/done shortcut")
	}
	commitBody := bodies["Consumer.TryReadClock"]
	preallocated := "d := b.owner guard := &CommitReservation{commitLease: newLease(d, commitRole, false)} if !d.auth.TryRLock()"
	if !strings.Contains(commitBody, preallocated) || strings.Count(commitBody, "newLease(") != 1 || strings.Count(commitBody, "&CommitReservation{") != 1 || !strings.HasSuffix(commitBody, "return guard, nil }") {
		failures = append(failures, "commit guard must allocate before authority R")
	}
	require("Consumer.TryReadClock", "if !d.clock.TryLock() { d.auth.RUnlock() return nil, errBusy }", "failed clock try must release R")
	if strings.Contains(bodies["Consumer.TryReadClock"], "d.auth.RLock()") || strings.Contains(bodies["Consumer.TryReadClock"], "d.clock.Lock()") {
		failures = append(failures, "consumer contains parking acquisition")
	}
	for key, role := range map[string]string{"ObservationClock.Sample": "observationRole", "CommitReservation.Sample": "commitRole", "ClockPublication.Sample": "clockPublicationRole", "ClockPublication.SampleRuntimePair": "clockPublicationRole"} {
		body := bodies[key]
		claim, raw := strings.Index(body, "cell.beginSample("+role+")"), strings.Index(body, "cell.owner.sampleLocked()")
		if claim < 0 || raw <= claim || !strings.Contains(body, "defer cell.finishSample()") {
			failures = append(failures, "Sample bypassed shared claim: "+key)
		}
	}
	done := bodies["CommitReservation.Sample"]
	if strings.Index(done, "select {") <= strings.Index(done, "cell.owner.sampleLocked()") || !strings.Contains(done, "case <-cell.owner.done:") {
		failures = append(failures, "Done must follow actual raw sample")
	}
	require("ClockPublication.Sample", "if cell.noop { return OwnedInstant{}, errNoClock }", "noop cannot mint observation")
	require("ClockPublication.SampleRuntimePair", "if cell.noop { return OwnedInstant{}, time.Time{}, errNoClock }", "noop pair cannot mint metadata")
	require("ClockPublication.SampleRuntimePair", "if cell.kind != runtimeKind { return OwnedInstant{}, time.Time{}, errInvalid }", "fixture pair must refuse before raw read")
	pair := bodies["ClockPublication.SampleRuntimePair"]
	if strings.Index(pair, "if cell.kind != runtimeKind") >= strings.Index(pair, "cell.owner.sampleLocked()") || strings.Contains(pair, ".done") || strings.Contains(pair, ".phase") || strings.Contains(pair, ".Lock(") {
		failures = append(failures, "runtime pair role, lifecycle or wait escape")
	}
	seal := bodies["Publisher.SealConstruction"]
	if strings.Contains(seal, "BeginPublication") || strings.Contains(seal, "Complete") || strings.Index(seal, "if d.phase == ready") >= strings.Index(seal, "if d.generation >= math.MaxUint64-1") {
		failures = append(failures, "seal must be dedicated with Ready priority")
	}
	require("Publisher.SealConstruction", "if d.generation >= math.MaxUint64-1 { d.poisoned.Store(true) return errUnavailable }", "first seal cannot enter exhausted sentinel")
	require("OwnedInstant.BoundaryAfter", "delta > time.Duration(math.MaxInt64)-at.instantElapsed", "exclusive MAX boundary remains representable")
	return failures
}

func TestDomainClosedSourceContract(t *testing.T) {
	if failures := domainSourceErrors(domainSourceFiles(t)); len(failures) != 0 {
		t.Fatal("closed domain source contract", failures)
	}
}
func TestDomainGuardRejectsCriticalOwnershipEscapes(t *testing.T) {
	original := domainSourceFiles(t)
	for name, raw := range original {
		normalized, err := format.Source(raw)
		if err != nil {
			t.Fatal(err)
		}
		original[name] = normalized
	}
	if failures := domainSourceErrors(original); len(failures) != 0 {
		t.Fatal("baseline source contract failed", failures)
	}
	cases := []struct {
		name, path, before, after string
		statement                 bool
	}{
		{"copied-cell", "clock.go", "observationLease *leaseCell", "observationLease leaseCell", false},
		{"old-parent-api", "domain.go", "type RuntimeDomain struct", "type ReadReservation struct{}\ntype RuntimeDomain struct", false},
		{"wall-origin", "domain.go", "origin: time.Now()", "origin: time.Now().Round(0)", false},
		{"origin-decoy", "domain.go", "d := &domain{kind: runtimeKind, done: done, origin: time.Now(), phase: constructing, generation: 1}", "d := &domain{kind: runtimeKind, done: done, origin: time.Now().Round(0), phase: constructing, generation: 1}\n_ = &domain{origin: time.Now()}", false},
		{"origin-wrong-initializer", "domain.go", "kind: runtimeKind, done: done, origin: time.Now()", "kind: fixtureKind, done: done, origin: time.Now()", false},
		{"origin-duplicate-field", "domain.go", "origin: time.Now()", "origin: time.Now(), origin: time.Now()", false},
		{"missing-busy", "clock.go", "CompareAndSwap(leaseLive, leaseBusy)", "CompareAndSwap(leaseLive, leaseLive)", false},
		{"terminal-busy", "clock.go", "CompareAndSwap(leaseLive, leaseClosing)", "CompareAndSwap(leaseBusy, leaseClosing)", false},
		{"postcheck", "clock.go", "if d.poisoned.Load() { return OwnedInstant{}, time.Time{}, errUnavailable }\nd.last = q", "d.last = q", true},
		{"parking-consumer", "domain.go", "if !d.clock.TryLock() { d.auth.RUnlock(); return nil, errBusy }", "d.clock.Lock()", true},
		{"top-level-raw-alias", "clock.go", "type OwnedInstant struct", "var runtimePairRawAlias = (*domain).sampleLocked\ntype OwnedInstant struct", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			altered := make(map[string][]byte)
			for name, raw := range original {
				altered[name] = bytes.Clone(raw)
			}
			before, after := tc.before, tc.after
			if tc.statement {
				before, after = domainStatementText(t, before), domainStatementText(t, after)
			}
			if strings.Count(string(altered[tc.path]), before) != 1 {
				t.Fatal("source control anchor not unique", tc.name)
			}
			altered[tc.path] = []byte(strings.Replace(string(altered[tc.path]), before, after, 1))
			failures := domainSourceErrors(altered)
			if len(failures) == 0 {
				t.Fatal("critical ownership escape accepted", tc.name)
			}
			if tc.name == "top-level-raw-alias" {
				found := false
				for _, failure := range failures {
					if failure == "raw sample method-value escape" {
						found = true
					}
				}
				if !found {
					t.Fatal("top-level raw alias refused for unrelated reason", tc.name, failures)
				}
			}
			if (tc.name == "wall-origin" || strings.HasPrefix(tc.name, "origin-")) && (len(failures) != 1 || failures[0] != "runtime monotonic origin must be constructor-owned") {
				t.Fatal("origin self-control refused for unrelated reason", tc.name, failures)
			}
		})
	}
}

// Canonicalize test-only statement fragments, so guard self-controls survive
// an independently authorized gofmt of the candidate without changing intent.
func domainStatementText(t *testing.T, body string) string {
	t.Helper()
	raw, err := format.Source([]byte("package p\nfunc f() {\n" + body + "\n}\n"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start, end := strings.Index(text, "{\n"), strings.LastIndex(text, "\n}")
	if start < 0 || end <= start {
		t.Fatal("test statement shape changed")
	}
	return text[start+2 : end]
}

func TestDomainHasNoExistingProductionImporter(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	wanted := "github.com/piratecash/corsa/internal/core/findnodebridge"
	approvedImporters := map[string]bool{
		"internal/core/node/find_node_ready_construction.go": true,
		"internal/core/findnode/owned_bridge_bootstrap.go":   true, "internal/core/findnode/owned_bridge_forward_answer.go": true, "internal/core/findnode/owned_bridge_round_dispatch.go": true,
		"internal/core/findnode/owned_bridge_receive.go":          true,
		"internal/core/node/find_node_owned_receive.go":           true,
		"internal/core/findnode/owned_bridge_commit.go":           true,
		"internal/core/node/find_node_bridge_capture.go":          true,
		"internal/core/node/find_node_degree_bridge.go":           true,
		"internal/core/findnode/owned_originals.go":               true,
		"internal/core/findnode/owned_bridge.go":                  true,
		"internal/core/findnode/owned_bridge_cut.go":              true,
		"internal/core/node/conn_registry.go":                     true,
		"internal/core/node/secure_session_store.go":              true,
		"internal/core/node/trust.go":                             true,
		"internal/core/node/find_node_degree.go":                  true,
		"internal/core/node/find_node_degree_terminal_bridge.go":  true,
		"internal/core/node/find_node_bridge_construction.go":     true,
		"internal/core/node/peer_state_degree_statistic.go":       true,
		"internal/core/node/peer_state_owned_degree_statistic.go": true,
		"internal/core/node/session_secure.go":                    true,
	}
	importers := map[string]int{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".gocache", ".gomodcache", ".golangci-lint-cache", ".idea", "refactoring":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			imported, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if imported == wanted {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				importers[rel]++
				if !approvedImporters[rel] || imp.Name != nil {
					t.Errorf("dormant leaf has unapproved production importer: %s", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(importers) != len(approvedImporters) {
		t.Fatalf("dormant leaf importer count changed: %v", importers)
	}
	for path := range approvedImporters {
		if importers[path] != 1 {
			t.Fatalf("dormant leaf requires exact named importer %s: %v", path, importers)
		}
	}
}

// The negative keeps a real declaration and all error/release paths; moving it
// after successful TryR is type-plausible and must fail only this SOURCE gate.
func TestDomainGuardRejectsLateCommitLeaseAllocation(t *testing.T) {
	raw := domainSourceFiles(t)
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "domain.go", raw["domain.go"], 0)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "TryReadClock" {
			continue
		}
		if fn.Body == nil || len(fn.Body.List) < 5 || domainPrinted(fn.Body.List[3]) != "guard := &CommitReservation{commitLease: newLease(d, commitRole, false)}" || domainPrinted(fn.Body.List[4]) != "if !d.auth.TryRLock() { return nil, errBusy }" {
			t.Fatal("commit preallocation selfcase exact anchor changed")
		}
		fn.Body.List[3], fn.Body.List[4] = fn.Body.List[4], fn.Body.List[3]
		matched++
	}
	if matched != 1 {
		t.Fatal("commit preallocation selfcase must match once")
	}
	var b bytes.Buffer
	if err := format.Node(&b, set, file); err != nil {
		t.Fatal(err)
	}
	raw["domain.go"] = b.Bytes()
	failures := domainSourceErrors(raw)
	if len(failures) != 1 || failures[0] != "commit guard must allocate before authority R" {
		t.Fatal("late commit allocation refused for wrong reason", failures)
	}
}
