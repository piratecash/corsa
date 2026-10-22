package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func ownedStatisticSourceErrors(sources map[string][]byte) []string {
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	var failures []string
	bodies := map[string]string{}
	refs := map[string]map[string]int{}
	want := map[string]map[string]int{
		"ownedDegreeStatisticProducer":      {"peer_state_owned_degree_statistic.go:<package>": 1, "find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "service.go:<package>": 2, "service.go:newServiceWithConstruction": 2, "peer_state_owned_degree_statistic.go:installOwnedDegreeStatistic": 1, "peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush": 1, "peer_state_degree_statistic.go:captureOverlayDWinForFlush": 1},
		"installOwnedDegreeStatistic":       {"service.go:newServiceWithConstruction": 1},
		"captureOwnedOverlayDWinForFlush":   {"peer_state_degree_statistic.go:captureOverlayDWinForFlush": 1},
		"projectOwnedDegreeStatisticLocked": {"peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush": 1},
		"projectOwnedDegreeBasisLocked":     {"peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked": 1, "find_node_degree_bridge.go:captureBridgeDegreeProjection": 1},
		"statistic":                         {"find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "service.go:newServiceWithConstruction": 2},
		"statisticInstalled":                {"find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "service.go:newServiceWithConstruction": 1},
	}
	// Exact private Ready constructor sites; all existing closures stay strict.
	want["ownedDegreeStatisticProducer"]["find_node_ready_construction.go:<package>"] = 1
	want["ownedDegreeStatisticProducer"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 1
	want["statistic"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 2
	want["ownedDegreeStatisticProducer"]["find_node_ready_construction.go:bindings"] = 1
	want["statistic"]["find_node_ready_construction.go:bindings"] = 5
	want["statistic"]["find_node_ready_construction.go:finalize"] = 1
	want["statisticInstalled"]["find_node_ready_construction.go:finalize"] = 1
	for name := range want {
		refs[name] = map[string]int{}
	}
	literals := map[string]int{}
	pairCalls := map[string]int{}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"owned statistic parse"}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if fn.Body != nil {
					bodies[site] = compact(pinPublicationPrinted(fn.Body))
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n != declaration {
						if set, ok := refs[n.Name]; ok {
							set[site]++
						}
					}
				case *ast.CompositeLit:
					if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "ownedDegreeStatisticProducer" {
						literals[site]++
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked" || site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked" {
							if _, ok := lhs.(*ast.StarExpr); ok {
								failures = append(failures, "owned pure projection writes through pointer")
							}
						}
						if sel, ok := lhs.(*ast.SelectorExpr); ok {
							if sel.Sel.Name == "ownedDegreeStatisticProducer" {
								failures = append(failures, "owned producer late Service setter")
							}
							if sel.Sel.Name == "statistic" && site != "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService" && site != "find_node_ready_construction.go:prepareFindNodeReadyConstruction" {
								failures = append(failures, "owned producer late construction setter")
							}
							if site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked" || site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked" {
								failures = append(failures, "owned pure projection writes authority")
							}
						}
					}
				case *ast.IncDecStmt:
					if site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked" || site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked" {
						if _, ok := n.X.(*ast.SelectorExpr); ok {
							failures = append(failures, "owned pure projection increments authority")
						}
					}
				case *ast.UnaryExpr:
					if (site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked" || site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked") && n.Op == token.AND {
						if _, ok := n.X.(*ast.SelectorExpr); ok {
							failures = append(failures, "owned pure projection exports mutable field alias")
						}
					}
				case *ast.CallExpr:
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SampleRuntimePair" {
						pairCalls[site]++
					}
					if site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked" || site == "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked" {
						name := ""
						if id, ok := n.Fun.(*ast.Ident); ok {
							name = id.Name
						}
						if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
							if pkg, ok := sel.X.(*ast.Ident); ok {
								name = pkg.Name + "." + sel.Sel.Name
							}
						}
						if name != "len" && name != "time.Duration" && name != "validDegreeStatisticKey" && name != "insertDegreeStatisticKey" && name != "projectOwnedDegreeBasisLocked" {
							failures = append(failures, "owned pure projection unknown call")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(refs, want) {
		failures = append(failures, "owned statistic exact reference set changed")
	}
	if !reflect.DeepEqual(literals, map[string]int{"find_node_ready_construction.go:prepareFindNodeReadyConstruction": 1, "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1}) {
		failures = append(failures, "owned statistic constructor set changed")
	}
	if !reflect.DeepEqual(pairCalls, map[string]int{"find_node_ready_construction.go:prepareFindNodeReadyConstruction": 1, "peer_state_owned_degree_statistic.go:installOwnedDegreeStatistic": 1, "peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush": 1}) {
		failures = append(failures, "owned pair caller set changed")
	}
	need := func(site, fragment, label string) {
		if !strings.Contains(bodies[site], compact(fragment)) {
			failures = append(failures, label)
		}
	}
	if strings.Count(compact(string(sources["peer_state_owned_degree_statistic.go"])), compact("type ownedDegreeStatisticProducer struct { owner *degreeOwner publisher findnodebridge.Publisher coverageStart findnodebridge.OwnedInstant installed bool }")) != 1 {
		failures = append(failures, "owned producer exact field shape changed")
	}
	need("find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService", "b.statistic = &ownedDegreeStatisticProducer{owner:b.owner,publisher:b.terminal.publisher} s := newServiceWithConstruction", "owned binding must precede builder callbacks")
	need("service.go:newServiceWithConstruction", "ownedStatistic = bridge.statistic", "exact prepared owner binding")
	need("service.go:newServiceWithConstruction", "svc := &Service{proofToken:proofToken,degree:degree,bridgeTerminal:terminal,ownedDegreeStatisticProducer:ownedStatistic,", "owned producer must be in FIRST Service literal")
	if !strings.HasSuffix(bodies["service.go:newServiceWithConstruction"], compact("svc.peerMu.Unlock() if construction.ready != nil { construction.ready.failure = construction.ready.checkBeforeInstall(svc) } if bridge != nil && bridge.statistic != nil && (construction.ready == nil || construction.ready.failure == nil) { bridge.statisticInstalled = svc.installOwnedDegreeStatistic() } return svc }")) {
		failures = append(failures, "coverage install must follow last constructor application wait")
	}
	need("peer_state_degree_statistic.go:captureOverlayDWinForFlush", "if s != nil && s.ownedDegreeStatisticProducer != nil { return s.captureOwnedOverlayDWinForFlush() }", "owned dispatch must not use legacy raw clock")
	wantInstall := `{ p:=s.ownedDegreeStatisticProducer if p==nil||p.owner==nil||p.owner!=s.degree{return false} o:=p.owner o.mu.Lock() defer o.mu.Unlock() if p.installed||o.poisoned||p.publisher==(findnodebridge.Publisher{})||p.publisher!=o.bridgePublisher{return false} publication:=p.publisher.BeginClockedPublication() point,_,err:=publication.SampleRuntimePair() valid:=err==nil&&point.Elapsed()>=o.last if valid{o.last=point.Elapsed() p.coverageStart=point p.installed=true}else{o.poisoned=true} publication.Complete() return valid }`
	if bodies["peer_state_owned_degree_statistic.go:installOwnedDegreeStatistic"] != compact(wantInstall) {
		failures = append(failures, "owned actual paired install point or memory window changed")
	}
	wantCapture := `{ p:=s.ownedDegreeStatisticProducer if p==nil||p.owner==nil||p.owner!=s.degree{return overlayDegreeStatistic{},errDegreeUnavailable} o:=p.owner o.mu.Lock() defer o.mu.Unlock() if !p.installed||o.poisoned||p.publisher==(findnodebridge.Publisher{})||p.publisher!=o.bridgePublisher{return overlayDegreeStatistic{},errDegreeUnavailable} publication:=p.publisher.BeginClockedPublication() point,wall,err:=publication.SampleRuntimePair() if err!=nil||point.Elapsed()<o.last{o.poisoned=true publication.Complete() return overlayDegreeStatistic{},errDegreeUnavailable} o.last=point.Elapsed() if !point.NotBefore(p.coverageStart){o.poisoned=true publication.Complete() return overlayDegreeStatistic{},errDegreeUnavailable} if !degreeStatisticCoverageReady(p.coverageStart.Elapsed(),point.Elapsed())||!validDegreeStatisticWall(wall){publication.Complete() return overlayDegreeStatistic{},errDegreeUnavailable} _,_,window,state:=projectOwnedDegreeStatisticLocked(o,point.Elapsed()) if state!=ownedDegreeComplete{if state==ownedDegreeFault{o.poisoned=true} publication.Complete() return overlayDegreeStatistic{},errDegreeUnavailable} result:=overlayDegreeStatistic{value:uint8(window),at:wall,valid:true} publication.Complete() return result,nil }`
	if bodies["peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush"] != compact(wantCapture) {
		failures = append(failures, "owned capture HWM coverage original-wall or W poison window changed")
	}
	need("peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked", "if o == nil || o.poisoned || o.unknown != 0 { return [degreeHistoryLimit]degreeHistoryRow{},0,ownedDegreeUnavailable }", "unknown owned coverage must remain unavailable")
	need("peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked", "rows,used,state:=projectOwnedDegreeBasisLocked(o,q) if state!=ownedDegreeComplete{return 0,0,0,state}", "statistics wrapper must consume actual shared basis")
	projection := bodies["peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked"]
	if !strings.Contains(projection, "w.origin!=row.life") || !strings.Contains(projection, "rows,used:=o.history,o.historyN") || strings.Contains(projection, "degreePoison(") || strings.Contains(projection, "BeginPublication(") || strings.Contains(projection, "degreeSnapshot(") {
		failures = append(failures, "owned pure projection provenance or legacy fallback")
	}
	return failures
}

func TestOwnedStatisticClosedSourceContract(t *testing.T) {
	if failures := ownedStatisticSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedStatisticGuardRejectsBackdateLateInstallAndIndependentWall(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"backdate", "peer_state_owned_degree_statistic.go", "p.coverageStart = point", "p.coverageStart = findnodebridge.OwnedInstant{}"},
		{"early-complete", "peer_state_owned_degree_statistic.go", "!degreeStatisticCoverageReady(p.coverageStart.Elapsed(), point.Elapsed())", "false && !degreeStatisticCoverageReady(p.coverageStart.Elapsed(), point.Elapsed())"},
		{"independent-wall", "peer_state_owned_degree_statistic.go", "at: wall", "at: time.Now().UTC()"},
		{"skip-unknown", "peer_state_owned_degree_statistic.go", " || o.unknown != 0", ""},
		{"origin", "peer_state_owned_degree_statistic.go", "w.origin != row.life || ", ""},
		{"projection-write", "peer_state_owned_degree_statistic.go", "rows, used := o.history, o.historyN", "rows, used := o.history, o.historyN\n\to.poisoned = true"},
		{"retime-install", "peer_state_owned_degree_statistic.go", "if p.installed || o.poisoned", "if false || o.poisoned"},
		{"late-setter", "peer_state_degree_statistic.go", "\nfunc validDegreeStatisticKey", "\nfunc lateOwnedStatistic(s *Service,p *ownedDegreeStatisticProducer){s.ownedDegreeStatisticProducer=p}\nfunc validDegreeStatisticKey"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("owned statistic selfcase exact anchor changed", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedStatisticSourceErrors(sources)) == 0 {
				t.Fatal("owned statistic SOURCE escape accepted", tc.name)
			}
		})
	}
}
