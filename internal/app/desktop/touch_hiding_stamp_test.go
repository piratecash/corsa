package desktop

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// The round this file belongs to closed a gap that only opened once the service
// thread started pumping its waits. A pane Hiding is now DELIVERED in the middle
// of a show — which is the point of pumping — but a callback may not touch
// shared state, so all it does is enqueue a command, and that command waits
// behind the very show that needs to know about it. For the length of that gap
// the process-wide "a hide has started" deadline still reads empty.
//
// The fix is a second deadline, stamped in the callback itself, and a predicate
// that consults both. The second deadline is held PER REGISTRATION, on the
// handler the callback resolved, and NOT in a word shared by the process. That
// distinction is the whole subject of half this file: the ordering that lets a
// Showing retire a Hiding — they are RAISED in the order they were raised —
// holds only WITHIN one registration, and the main and console windows advise
// separately. Raised, not executed: the handler is an in-proc MTA object and COM
// serializes no call on one, so the two can be running at once and it is
// tkHandlerMu that keeps their effects from interleaving. A shared word could be
// zeroed by a late Showing belonging to a window that never saw the hide, which
// is exactly the misfire the record was introduced to prevent.
//
// Holding the record on the registration is not by itself enough, because the
// registration can be taken away underneath the callback that is writing to it.
// Unadvise stops FUTURE notifications and promises nothing about a callback
// already in flight, so a resolution that releases tkHandlerMu before the store
// leaves room for the service thread to read the slot, fold nothing, and delete
// the handler — after which the store lands in an object no fold will visit
// again, and the command the callback enqueues is dropped along with the
// keyboard it names. The resolution and the store are therefore one critical
// section, and a hide whose registration is already gone is kept in a word that
// is only ever armed, never cleared.
//
// What follows tests the predicate directly and guards the source-level facts it
// rests on: that the stamp is armed as part of resolving the handler, under one
// hold of the lock, on the handler and nowhere shared; that it is retired the
// same way for that same handler and nowhere else; that a hide arriving after
// its registration is gone is still recorded; that the process-wide question is
// answered by folding over the live registrations and that record together; that
// unadvising a window hands its live stamp on rather than dropping it; and that
// the show's adoption path reads the result.

func TestHideStillClosingReadsBothRecordsOfAStartedHide(t *testing.T) {
	const (
		now  = 1_000
		live = 1_500
		past = 900
	)
	cases := []struct {
		name            string
		handled, raised int64
		want            bool
		why             string
	}{
		{
			"nothing has started", 0, 0, false,
			"no hide anywhere: a tap onto a stable keyboard must not arm a re-show poll",
		},
		{
			"the service thread has processed a hide", live, 0, true,
			"the ordinary case: our own TryHide, or a Hiding whose command has already run",
		},
		{
			"only the callback has seen it", 0, live, true,
			"this is the round's whole case — the Hiding was delivered mid-command and its command has not run yet",
		},
		{"both agree", live, live, true, "one physical hide, recorded twice"},
		{
			"both have expired", past, past, false,
			"a hide that started long enough ago is finished; a stale record must not make a stable pane look closing",
		},
		{
			"the handled record expired, the fresh one has not", past, live, true,
			"a second hide started while the first record was aging out",
		},
		{
			"the callback record expired, the handled one has not", live, past, true,
			"our own TryHide is recorded only by the service thread, so its record must be enough on its own",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tkHideStillClosing(now, c.handled, c.raised); got != c.want {
				t.Errorf("tkHideStillClosing(%d, %d, %d) = %v, want %v: %s",
					now, c.handled, c.raised, got, c.want, c.why)
			}
		})
	}
}

func TestAHideRecordExpiresExactlyAtItsDeadline(t *testing.T) {
	if tkHideStillClosing(1_000, 1_000, 0) {
		t.Error("a deadline equal to now still reads live: the window is [start, deadline), " +
			"and a record that outlives its own bound is the stale-value misfire the deadline exists to prevent")
	}
	if tkHideStillClosing(1_000, 0, 1_000) {
		t.Error("the callback's record outlives its own deadline; see above — both records must expire the same way")
	}
}

func TestAnUnarmedRecordIsNeverLive(t *testing.T) {
	// Zero means "never armed", not "armed at time zero". The process clock
	// starts at zero, so on a machine that hides its keyboard in the first
	// nanosecond of the process the two would otherwise be the same value.
	if tkHideStillClosing(0, 0, 0) {
		t.Error("an unarmed pair reads as a hide in progress at the start of the process clock")
	}
}

// ---- source-level guards -------------------------------------------------
//
// touch_keyboard_windows.go is //go:build windows and this suite runs on Linux,
// so none of the code below is ever compiled, let alone executed, by CI. These
// read it instead. Blunt, and chosen deliberately: the alternative is no
// coverage at all of the placement that the entire fix consists of.

// tkVtblCallback returns the function literal registered as the named entry of
// the shared handler vtable — Showing or Hiding.
func tkVtblCallback(t *testing.T, f *ast.File, name string) *ast.FuncLit {
	t.Helper()
	var lit *ast.FuncLit
	ast.Inspect(f, func(n ast.Node) bool {
		d, ok := n.(*ast.FuncDecl)
		if !ok || d.Name.Name != "tkInitHandlerVtbl" {
			return true
		}
		ast.Inspect(d.Body, func(m ast.Node) bool {
			kv, ok := m.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != name {
				return true
			}
			ast.Inspect(kv.Value, func(k ast.Node) bool {
				if fl, ok := k.(*ast.FuncLit); ok && lit == nil {
					lit = fl
				}
				return true
			})
			return false
		})
		return false
	})
	if lit == nil {
		t.Fatalf("no %s callback in tkInitHandlerVtbl — the pane events this fix rests on are no longer handled", name)
	}
	return lit
}

// tkStoresTo collects the `name.Store(...)` calls inside n, where name is a bare
// identifier — a package-level variable, in the file this reads.
func tkStoresTo(n ast.Node, name string) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Store" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == name {
			out = append(out, call)
		}
		return true
	})
	return out
}

// tkStoresToField collects the `recv.field.Store(...)` calls inside n — a store
// into per-instance state rather than into a shared package-level word.
func tkStoresToField(n ast.Node, recv, field string) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Store" {
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok || inner.Sel.Name != field {
			return true
		}
		if id, ok := inner.X.(*ast.Ident); ok && id.Name == recv {
			out = append(out, call)
		}
		return true
	})
	return out
}

// tkCallsTo collects the calls to a plain named function inside n.
func tkCallsTo(n ast.Node, name string) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
			out = append(out, call)
		}
		return true
	})
	return out
}

// tkCallsToSel collects the `recv.method(...)` calls inside n.
func tkCallsToSel(n ast.Node, recv, method string) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != method {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv {
			out = append(out, call)
		}
		return true
	})
	return out
}

// tkEnclosing reports whether some node on the path from root down to target
// satisfies want. Used to ask what a statement is nested inside.
func tkEnclosing(root ast.Node, target ast.Node, want func(ast.Node) bool) bool {
	var stack []ast.Node
	found := false
	ast.Inspect(root, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		if n != target {
			return true
		}
		for _, up := range stack {
			if want(up) {
				found = true
			}
		}
		return true
	})
	return found
}

// tkMentions reports whether n contains an identifier with this name, in any
// position — a call, a selector's field, a bare reference.
func tkMentions(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if id, ok := m.(*ast.Ident); ok && id.Name == name {
			found = true
		}
		return !found
	})
	return found
}

// tkPackageVars returns the names declared by the file's top-level `var` blocks.
func tkPackageVars(f *ast.File) map[string]bool {
	out := map[string]bool{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, s := range gd.Specs {
			vs, ok := s.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, n := range vs.Names {
				out[n.Name] = true
			}
		}
	}
	return out
}

// tkFuncDecl finds a top-level function by name.
func tkFuncDecl(t *testing.T, f *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name && fd.Recv == nil {
			return fd
		}
	}
	t.Fatalf("no %s in touch_keyboard_windows.go", name)
	return nil
}

func TestTheRegistrationCarriesItsOwnHideRecord(t *testing.T) {
	f := tkWindowsAST(t)

	var st *ast.StructType
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "tkPaneHandler" {
			return true
		}
		if s, ok := ts.Type.(*ast.StructType); ok {
			st = s
		}
		return false
	})
	if st == nil {
		t.Fatal("no tkPaneHandler struct — the object every pane callback resolves is gone")
	}

	var found *ast.Field
	for _, fl := range st.Fields.List {
		for _, n := range fl.Names {
			if n.Name == "hide" {
				found = fl
			}
		}
	}
	if found == nil {
		t.Fatal("tkPaneHandler has no hide field: the callback's record of a started hide has to live on the " +
			"REGISTRATION. Held in one process-wide word instead, it can be zeroed by a Showing belonging to a " +
			"window that never saw the hide — the main and console windows advise separately and their callbacks " +
			"are ordered only against their own")
	}
	sel, ok := found.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Int64" {
		t.Error("tkPaneHandler.hide is not an atomic.Int64: it is written from arbitrary RPC threads and read " +
			"from the service thread, so a plain field is a data race")
	}
	if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "atomic" {
		t.Error("tkPaneHandler.hide is not from sync/atomic; see above")
	}
}

func TestTheHidingCallbackStampsAsItResolvesTheHandler(t *testing.T) {
	f := tkWindowsAST(t)
	hiding := tkVtblCallback(t, f, "Hiding")

	stamps := tkCallsTo(hiding, "tkStampHiding")
	if len(stamps) != 1 {
		t.Fatalf("the Hiding callback calls tkStampHiding %d time(s), want exactly 1: the stamp has to come WITH "+
			"the resolution of `this` and under the same hold of the lock. A resolution that has already let go "+
			"can be followed by the unadvise that reads an empty slot, folds nothing and deletes the handler, "+
			"and the store then goes into an object nothing will ever read again", len(stamps))
	}
	if n := len(tkStoresToField(hiding, "h", "hide")); n != 0 {
		t.Errorf("the Hiding callback stores h.hide itself (%d time(s)): a store written here is outside "+
			"tkHandlerMu however the handler was resolved, and that gap is the whole defect — the lock must be "+
			"held across the lookup and the store together", n)
	}
	if n := len(tkCallsTo(hiding, "tkRetireHiding")); n != 0 {
		t.Errorf("the Hiding callback resolves through tkRetireHiding (%d time(s)): that clears the record of "+
			"the very hide this callback exists to report", n)
	}

	// Order still matters: everything else in this callback ends up on the
	// service thread's queue, which is the thing being outrun.
	enq := tkCallsTo(hiding, "tkEnqueue")
	if len(enq) == 0 {
		t.Fatal("the Hiding callback enqueues nothing — the marshaled half of the event handling is gone")
	}
	for _, call := range enq {
		if call.Pos() < stamps[0].Pos() {
			t.Errorf("the Hiding callback enqueues its command (offset %d) before it stamps the hide: the stamp "+
				"is what the currently running command reads, so anything ahead of it is time in which the hide "+
				"is invisible", call.Pos())
		}
	}
}

func TestTheStampIsArmedForTheAnimationWindow(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkStampHiding")

	// Armed for the animation window, not for an instant. A stamp of the bare
	// clock expires the moment it is written and is a fix in name only.
	for _, name := range []string{"tkNowNs", "tkHideAnimWindow"} {
		if !tkMentions(fn.Body, name) {
			t.Errorf("tkStampHiding does not use %s: the stamp is a deadline on the process clock, and one that "+
				"does not outlast the command queue records the hide for nobody", name)
		}
	}

	// Mentioning them is not using them. Follow the value that actually reaches
	// the slot: it has to be a name the function binds from the clock plus the
	// animation window. A helper called tkStampHiding that computes a deadline
	// and then stores a literal reads as correct at every other guard here.
	stores := tkStoresToField(fn.Body, "h", "hide")
	if len(stores) != 1 || len(stores[0].Args) != 1 {
		t.Fatalf("tkStampHiding writes the hide slot %d time(s), want exactly 1 with one argument", len(stores))
	}
	armed, ok := stores[0].Args[0].(*ast.Ident)
	if !ok {
		t.Fatalf("tkStampHiding stamps %T rather than the deadline it computed: the slot and the orphan record "+
			"have to carry the same value, or a hide reads as live in one place and settled in the other",
			stores[0].Args[0])
	}
	if !tkBoundFrom(fn.Body, armed.Name, "tkNowNs", "tkHideAnimWindow") {
		t.Errorf("tkStampHiding stamps %q, which it does not bind from tkNowNs plus tkHideAnimWindow: whatever "+
			"reaches the slot IS the record of the hide, and a zero or a bare clock there means the Hiding was "+
			"resolved, locked, stored — and still lost", armed.Name)
	}

	// And the orphaned hide is the same hide: one deadline, both records.
	orphans := tkStoresTo(fn.Body, "tkOrphanHidingNs")
	if len(orphans) != 1 || len(orphans[0].Args) != 1 {
		t.Fatalf("tkStampHiding writes tkOrphanHidingNs %d time(s), want exactly 1 with one argument", len(orphans))
	}
	if id, ok := orphans[0].Args[0].(*ast.Ident); !ok || id.Name != armed.Name {
		t.Errorf("tkStampHiding records the orphaned hide from something other than %q: the two paths differ in "+
			"where the hide is kept, not in when it ends", armed.Name)
	}
}

// tkBoundFrom reports whether name is bound, anywhere in n, by an assignment
// whose right-hand side mentions every one of from.
func tkBoundFrom(n ast.Node, name string, from ...string) bool {
	found := false
	ast.Inspect(n, func(node ast.Node) bool {
		as, ok := node.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); !ok || id.Name != name {
			return true
		}
		for _, want := range from {
			if !tkMentions(as.Rhs[0], want) {
				return true
			}
		}
		found = true
		return true
	})
	return found
}

func TestTheShowingCallbackRetiresOnlyItsOwnRegistrationsStamp(t *testing.T) {
	f := tkWindowsAST(t)
	showing := tkVtblCallback(t, f, "Showing")

	retires := tkCallsTo(showing, "tkRetireHiding")
	if len(retires) != 1 {
		t.Fatalf("the Showing callback calls tkRetireHiding %d time(s), want exactly 1: a pane that is up is "+
			"not a pane closing, and a stamp nobody retires makes a settled keyboard read as closing for a "+
			"whole animation window", len(retires))
	}
	if n := len(tkCallsTo(showing, "tkStampHiding")); n != 0 {
		t.Errorf("the Showing callback resolves through tkStampHiding (%d time(s)): that ARMS the record this "+
			"event is evidence against", n)
	}
	if n := len(tkStoresToField(showing, "h", "hide")); n != 0 {
		t.Errorf("the Showing callback stores h.hide itself (%d time(s)): outside tkHandlerMu the handler can "+
			"already be gone, and the clear writes to nothing", n)
	}

	stores := tkStoresToField(tkFuncDecl(t, f, "tkRetireHiding").Body, "h", "hide")
	if len(stores) != 1 {
		t.Fatalf("tkRetireHiding stores h.hide %d time(s), want exactly 1", len(stores))
	}
	lit, ok := stores[0].Args[0].(*ast.BasicLit)
	if !ok || lit.Value != "0" {
		t.Error("tkRetireHiding does not clear h.hide to zero: only zero means 'no hide has started', so any " +
			"other value re-arms the very record this is meant to retire")
	}
}

// The finding that produced this test: a Showing may retire the hide record of
// its OWN registration and of no other. Callbacks are delivered in the order
// they were raised only within a registration; across registrations there is no
// order at all, and they arrive on arbitrary RPC threads. So a Showing for the
// console — including one for a console whose Unadvise failed and whose handler
// therefore stayed pinned and live — must not be able to reach a record the main
// window's Hiding has just armed. In one shared word it can, and the show that
// then reads the word sees a settled pane, adopts a keyboard already closing,
// and arms no re-show poll.
func TestNoPaneCallbackWritesASharedWord(t *testing.T) {
	f := tkWindowsAST(t)
	shared := tkPackageVars(f)

	for _, name := range []string{"Showing", "Hiding"} {
		cb := tkVtblCallback(t, f, name)
		ast.Inspect(cb, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Store" {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || !shared[id.Name] {
				return true
			}
			t.Errorf("the %s callback stores into the package-level %s: a callback belongs to ONE registration "+
				"and its ordering guarantees reach no further, so a word shared by every window is one this "+
				"callback may not overwrite. Two windows advise independently and a late event from either would "+
				"erase the other's", name, id.Name)
			return true
		})
	}
}

func TestOnlyTheLockedHelpersWriteTheHidingStamp(t *testing.T) {
	f := tkWindowsAST(t)
	within := []*ast.FuncDecl{
		tkFuncDecl(t, f, "tkStampHiding"),
		tkFuncDecl(t, f, "tkRetireHiding"),
	}

	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Store" {
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok || inner.Sel.Name != "hide" {
			return true
		}
		for _, fn := range within {
			if call.Pos() >= fn.Pos() && call.End() <= fn.End() {
				return true
			}
		}
		t.Errorf("a hide stamp is written outside tkStampHiding/tkRetireHiding (offset %d): those two are the "+
			"only places that hold tkHandlerMu across the resolution AND the store, which is what stops an "+
			"unadvise from slipping between them. A write anywhere else — the callbacks above all — can land "+
			"in a handler the service thread has already dropped, and the hide is then held nowhere", call.Pos())
		return true
	})
}

// The finding that produced this test: the callback resolved its handler
// through a lookup that took tkHandlerMu and gave it back, and only then stored
// the stamp. Between the two, a successful Unadvise on the service thread reads
// the slot it is about to be handed, sees the zero still in it, folds nothing,
// and deletes the handler from the map. The store that follows writes into an
// object tkAnyPaneHiding will never visit again, and the command the callback
// enqueues behind it is dropped once the keyboard it names is released — so
// neither record ends up holding a hide that really began. A console closing
// while a field in the main window is tapped is exactly that sequence.
func TestResolvingAndStampingShareOneCriticalSection(t *testing.T) {
	f := tkWindowsAST(t)

	for _, name := range []string{"tkStampHiding", "tkRetireHiding"} {
		fn := tkFuncDecl(t, f, name)

		locks := tkCallsToSel(fn.Body, "tkHandlerMu", "Lock")
		if len(locks) != 1 {
			t.Fatalf("%s takes tkHandlerMu %d time(s), want exactly 1: it both resolves the handler and writes "+
				"to it, and the map it resolves through is mutated from the service thread", name, len(locks))
		}

		// Every release must be deferred. An explicit Unlock is a place where
		// the section can be ended early — which is the defect itself, written
		// out inside one function instead of across two.
		for _, un := range tkCallsToSel(fn.Body, "tkHandlerMu", "Unlock") {
			if !tkEnclosing(fn.Body, un, func(n ast.Node) bool { _, ok := n.(*ast.DeferStmt); return ok }) {
				t.Errorf("%s releases tkHandlerMu with a plain Unlock (offset %d): the lock has to be held "+
					"from the map read through the store, and an early release restores the very window in "+
					"which unadvise can delete the handler between them", name, un.Pos())
			}
		}

		var read *ast.IndexExpr
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			ix, ok := n.(*ast.IndexExpr)
			if !ok {
				return true
			}
			if id, ok := ix.X.(*ast.Ident); ok && id.Name == "tkHandlerByPtr" {
				read = ix
			}
			return true
		})
		if read == nil {
			t.Fatalf("%s never reads tkHandlerByPtr: it is meant to resolve the COM `this` pointer itself, "+
				"precisely so the resolution and the store cannot be pulled apart", name)
		}
		if read.Pos() < locks[0].Pos() {
			t.Errorf("%s reads tkHandlerByPtr before it takes tkHandlerMu: that is a concurrent map read "+
				"against the service thread's writes", name)
		}

		stores := tkStoresToField(fn.Body, "h", "hide")
		if len(stores) != 1 {
			t.Fatalf("%s writes a hide slot %d time(s), want exactly 1", name, len(stores))
		}
		if stores[0].Pos() < read.Pos() {
			t.Errorf("%s writes the hide slot before it has resolved one", name)
		}
	}
}

// A hide raised for a registration that is already gone is still a hide of the
// one shared pane. Unadvise ends future notifications; it does not wait for a
// callback that has begun, so this delivery is expected rather than exotic —
// and the command such a callback would enqueue is dropped anyway, because the
// keyboard it names is being released. If the stamp is dropped too, nothing
// anywhere holds the close, and the next window's show adopts a closing pane.
func TestAHideForARetiredRegistrationIsNotLost(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkStampHiding")

	stores := tkStoresTo(fn.Body, "tkOrphanHidingNs")
	if len(stores) != 1 {
		t.Fatalf("tkStampHiding writes tkOrphanHidingNs %d time(s), want exactly 1: a Hiding whose handler has "+
			"already left the map has nowhere else to be recorded — its slot is gone and its command will be "+
			"discarded with the keyboard being released", len(stores))
	}

	// On the absent-handler path only. Writing it on every hide would make the
	// shared word the primary record again, and it is the one record no
	// Showing is allowed to retire.
	if !tkEnclosing(fn.Body, stores[0], func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return false
		}
		be, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok || be.Op != token.EQL {
			return false
		}
		id, ok := be.Y.(*ast.Ident)
		return ok && id.Name == "nil"
	}) {
		t.Error("tkStampHiding writes tkOrphanHidingNs off the absent-handler path: a hide with a live slot " +
			"belongs in the slot, where its own Showing can retire it. In the shared word nothing may retire " +
			"it, so every ordinary hide would then read as closing for its full animation window")
	}

	// And it must take the later of the two, not overwrite. Two orphaned hides
	// can arrive out of order, and shortening a standing deadline retires a
	// close early — the failure this whole record exists to prevent.
	if !tkEnclosing(fn.Body, stores[0], func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return false
		}
		be, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok {
			return false
		}
		if be.Op == token.GTR && tkMentions(be.Y, "tkOrphanHidingNs") && !tkMentions(be.X, "tkOrphanHidingNs") {
			return true
		}
		return be.Op == token.LSS && tkMentions(be.X, "tkOrphanHidingNs") && !tkMentions(be.Y, "tkOrphanHidingNs")
	}) {
		t.Error("tkStampHiding does not keep the LATER of the new deadline and the one tkOrphanHidingNs " +
			"already holds: an unguarded write, or one guarded the wrong way round, lets a second orphaned " +
			"hide move a standing deadline backwards and end a close that is still animating")
	}
}

// The shared word is armed and never cleared, and that is deliberate. It is
// shared by every window, so the ordering that lets a Showing retire a Hiding —
// which holds only within one registration — does not reach it. A Showing
// allowed to zero it could erase a hide another window began, which is the
// defect the per-registration slot was introduced to fix. Expiry retires it
// instead, at the cost of at most one animation window of reading a settled
// pane as closing — an error that arms a re-show poll rather than dropping a
// keyboard.
func TestNothingRetiresTheOrphanRecord(t *testing.T) {
	f := tkWindowsAST(t)

	for _, call := range tkStoresTo(f, "tkOrphanHidingNs") {
		if len(call.Args) != 1 {
			continue
		}
		if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Value == "0" {
			t.Errorf("tkOrphanHidingNs is cleared (offset %d): no window may retire a hide it cannot prove is "+
				"its own, and across registrations there is no order in which to prove it. Only expiry retires "+
				"this record", call.Pos())
		}
	}
	if tkMentions(tkFuncDecl(t, f, "tkRetireHiding").Body, "tkOrphanHidingNs") {
		t.Error("tkRetireHiding touches tkOrphanHidingNs: a Showing outranks the events of its own " +
			"registration and no others, and what is in that word may belong to a window this one never saw")
	}
	if tkMentions(tkVtblCallback(t, f, "Showing"), "tkOrphanHidingNs") {
		t.Error("the Showing callback reaches the shared word directly; see above")
	}
}

func TestTheProcessWideAnswerIncludesAHideWhoseRegistrationIsGone(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkAnyPaneHiding")

	var rng *ast.RangeStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if r, ok := n.(*ast.RangeStmt); ok && tkMentions(r.X, "tkHandlerByPtr") {
			rng = r
		}
		return true
	})
	if rng == nil {
		t.Fatal("tkAnyPaneHiding no longer ranges over tkHandlerByPtr; see the fold test")
	}

	// Whatever the loop accumulates into has to START from the orphan record,
	// or a hide whose registration is gone is folded over nothing and the
	// answer is the same zero as before it was written.
	var acc string
	ast.Inspect(rng.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 || as.Tok != token.ASSIGN {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			acc = id.Name
		}
		return true
	})
	if acc == "" {
		t.Fatal("tkAnyPaneHiding's loop assigns to nothing: it is not accumulating an answer at all")
	}

	var seeded bool
	var seedPos token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Pos() >= rng.Pos() || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); !ok || id.Name != acc {
			return true
		}
		if tkMentions(as.Rhs[0], "tkOrphanHidingNs") {
			seeded = true
			seedPos = as.Pos()
		}
		return true
	})
	if !seeded {
		t.Errorf("tkAnyPaneHiding starts its fold from something other than tkOrphanHidingNs: the process-wide "+
			"question is whether ANY hide is live, and a hide whose registration was unadvised out from under "+
			"its own callback is held in that word and nowhere else. Folding only over %s leaves it invisible "+
			"to every show", "tkHandlerByPtr")
	}

	// And it is read INSIDE the section, not on the way in. The two records are
	// one answer: a hide that moves from a slot to the orphan word (or arrives
	// straight into it) does so under tkHandlerMu, and a seed taken before the
	// lock can miss it in the word and then miss it in the map as well, because
	// the delete that orphaned it landed in between.
	locks := tkCallsToSel(fn.Body, "tkHandlerMu", "Lock")
	if len(locks) != 1 {
		t.Fatalf("tkAnyPaneHiding takes tkHandlerMu %d time(s), want exactly 1", len(locks))
	}
	if seedPos < locks[0].Pos() {
		t.Error("tkAnyPaneHiding reads tkOrphanHidingNs before it takes tkHandlerMu: the word and the map are " +
			"read as one answer, and a stamp landing between them is then held by neither — the same hide-lost " +
			"shape this record was added to close, only moved to the reader")
	}
}

func TestTheProcessWideAnswerFoldsOverEveryLiveRegistration(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkAnyPaneHiding")

	// It must actually walk the registry. Reading one handler, or a cached
	// value, reintroduces the shared-word bug under a different name.
	var rng *ast.RangeStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if r, ok := n.(*ast.RangeStmt); ok && tkMentions(r.X, "tkHandlerByPtr") {
			rng = r
		}
		return true
	})
	if rng == nil {
		t.Fatal("tkAnyPaneHiding does not range over tkHandlerByPtr: the process-wide question is 'is ANY " +
			"registration mid-hide', and only the map knows which registrations there are")
	}

	// The fold must be a maximum, not a first hit. Returning from inside the
	// loop answers with whichever handler the map happened to yield first —
	// and Go's map order is deliberately random, so the bug would appear
	// intermittently on exactly the two-window setup that motivated this.
	ast.Inspect(rng.Body, func(n ast.Node) bool {
		if r, ok := n.(*ast.ReturnStmt); ok {
			t.Errorf("tkAnyPaneHiding returns from inside its loop (offset %d): that answers with an arbitrary "+
				"registration rather than the latest deadline any of them holds", r.Pos())
		}
		return true
	})
	// Whatever the loop binds the slot's value to has to end up on the GREATER
	// side of the comparison. A minimum reads as an equally tidy fold and is
	// the exact opposite answer: the running total starts at zero, so taking
	// the smaller of the two reports "nothing is hiding" forever.
	var loaded string
	ast.Inspect(rng.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if !tkMentions(as.Rhs[0], "hide") || !tkMentions(as.Rhs[0], "Load") {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			loaded = id.Name
		}
		return true
	})
	if loaded == "" {
		t.Fatal("tkAnyPaneHiding's loop never loads a hide slot: it is folding over something other than the " +
			"record it exists to answer for")
	}
	var takesMax bool
	ast.Inspect(rng.Body, func(n ast.Node) bool {
		be, ok := n.(*ast.BinaryExpr)
		if !ok {
			return true
		}
		if be.Op == token.GTR && tkMentions(be.X, loaded) && !tkMentions(be.Y, loaded) {
			takesMax = true
		}
		if be.Op == token.LSS && tkMentions(be.Y, loaded) && !tkMentions(be.X, loaded) {
			takesMax = true
		}
		return true
	})
	if !takesMax {
		t.Error("tkAnyPaneHiding does not keep the LARGER of the running answer and the slot it just read: a " +
			"stamp is live exactly while now is below it, so the answer for the process is the latest deadline " +
			"any registration holds. Keeping the smaller — or comparing nothing at all — reports an expired " +
			"stamp, or none, while a live one sits in the same map")
	}

	// The map is mutated from the service thread while callbacks read it.
	if !tkMentions(fn.Body, "tkHandlerMu") {
		t.Error("tkAnyPaneHiding walks tkHandlerByPtr without holding tkHandlerMu: advise and unadvise write " +
			"that map from the service thread while this runs, which is a concurrent map read and write")
	}
}

// One door out of the map. A registration that leaves tkHandlerByPtr with a
// live stamp in its slot destroys the only record of a hide whose command has
// not run — and for the window being unadvised that command never will run,
// because it is dropped the moment its keyboard reads as released. Handing the
// stamp on is therefore not an optimization; it is the difference between a
// close that is visible to the next tap and one that exists nowhere.
//
// It is handed to tkOrphanHidingNs and NOT to tkHideDeadlineNs. The latter is a
// record every window's events reach and a show zeroes, so parking a foreign
// registration's hide there lets an unrelated window cancel a close it never
// saw — the same shape, one indirection along, that moved this record off a
// shared word in the first place.
func TestUnadvisingAWindowHandsItsLiveStampOn(t *testing.T) {
	f := tkWindowsAST(t)

	// Exactly one delete, and it lives in tkForgetHandler. This is the guard
	// that makes all the others total: a second delete site elsewhere would be
	// a second lifetime rule, and the one that goes wrong is always the one
	// nobody wrote a test against.
	fn := tkFuncDecl(t, f, "tkForgetHandler")
	var dels []*ast.CallExpr
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "delete" || len(call.Args) != 2 {
			return true
		}
		if id, ok := call.Args[0].(*ast.Ident); ok && id.Name == "tkHandlerByPtr" {
			dels = append(dels, call)
		}
		return true
	})
	if len(dels) != 1 {
		t.Fatalf("tkHandlerByPtr is deleted from in %d places, want exactly 1 (tkForgetHandler): every removal "+
			"has to hand on the hide the slot may still hold, and a second delete site is a second place for "+
			"that rule to be forgotten", len(dels))
	}
	del := dels[0]
	if del.Pos() < fn.Pos() || del.End() > fn.End() {
		t.Fatalf("the one delete from tkHandlerByPtr is outside tkForgetHandler (offset %d): that helper exists "+
			"to be the only door out of the map", del.Pos())
	}

	// The stamp goes to the orphan word — armed only, never cleared — before
	// the slot holding it is dropped.
	stores := tkStoresTo(fn.Body, "tkOrphanHidingNs")
	if len(stores) != 1 || len(stores[0].Args) != 1 {
		t.Fatalf("tkForgetHandler writes tkOrphanHidingNs %d time(s), want exactly 1 with one argument: the "+
			"hide the departing registration is holding has nowhere else to go", len(stores))
	}
	if stores[0].Pos() > del.Pos() {
		t.Error("tkForgetHandler deletes the handler before folding its stamp: after the delete the slot is " +
			"unreachable and there is nothing left to read")
	}
	if tkMentions(fn.Body, "tkHideDeadlineNs") {
		t.Error("tkForgetHandler hands the stamp to tkHideDeadlineNs: that record is written by every window's " +
			"show and zeroed by one, so a hide parked there can be cancelled by a window that never saw it. " +
			"The orphan word is the one nothing but expiry retires, and it is already where a Hiding arriving " +
			"after this delete is put — both orders of that race have to end in the same place")
	}

	// What is folded is the departing slot's own deadline, not any old value.
	// A helper that resolves the handler, locks, and stores a literal passes
	// every structural guard here and records the hide for nobody.
	arg, ok := stores[0].Args[0].(*ast.Ident)
	if !ok {
		t.Fatalf("tkForgetHandler folds %T rather than a value it bound from the slot", stores[0].Args[0])
	}
	if !tkBoundFrom(fn.Body, arg.Name, "hide", "Load") {
		t.Errorf("tkForgetHandler folds %q, which it does not bind from the handler's hide slot: whatever "+
			"reaches the orphan word IS the hide being handed on", arg.Name)
	}

	// Later of the two, never an overwrite: two departing registrations, or a
	// Hiding orphaned by an earlier one, can leave a deadline already standing,
	// and shortening it ends a close that is still animating.
	if !tkEnclosing(fn.Body, stores[0], func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return false
		}
		be, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok {
			return false
		}
		if be.Op == token.GTR && tkMentions(be.Y, "tkOrphanHidingNs") && !tkMentions(be.X, "tkOrphanHidingNs") {
			return true
		}
		return be.Op == token.LSS && tkMentions(be.X, "tkOrphanHidingNs") && !tkMentions(be.Y, "tkOrphanHidingNs")
	}) {
		t.Error("tkForgetHandler does not keep the LATER of the stamp it is handing on and what tkOrphanHidingNs " +
			"already holds: an unguarded write moves a standing deadline backwards and retires a close early")
	}

	// One section, released only by defer, covering the read and the delete.
	// Split them and unadvise races the stamp again, which is the whole reason
	// this code is a helper rather than three lines at each call site.
	locks := tkCallsToSel(fn.Body, "tkHandlerMu", "Lock")
	if len(locks) != 1 {
		t.Fatalf("tkForgetHandler takes tkHandlerMu %d time(s), want exactly 1", len(locks))
	}
	if locks[0].Pos() > stores[0].Pos() {
		t.Error("tkForgetHandler reads and folds the slot before taking tkHandlerMu: that is a concurrent map " +
			"read against the callbacks that stamp it")
	}
	for _, un := range tkCallsToSel(fn.Body, "tkHandlerMu", "Unlock") {
		if !tkEnclosing(fn.Body, un, func(n ast.Node) bool { _, ok := n.(*ast.DeferStmt); return ok }) {
			t.Errorf("tkForgetHandler releases tkHandlerMu with a plain Unlock (offset %d): the lock has to "+
				"span the read and the delete, and an early release restores the window a Hiding can land in",
				un.Pos())
		}
	}

	// And nothing else happens in there. tkHandlerMu is not reentrant and this
	// runs on the service thread, so a COM call — anything that pumps — would
	// admit an incoming Hiding callback re-entering this same lock on this
	// same thread and deadlocking the apartment.
	allowed := map[string]bool{
		"tkHandlerMu.Lock": true, "tkHandlerMu.Unlock": true,
		"tkOrphanHidingNs.Load": true, "tkOrphanHidingNs.Store": true,
		"h.hide.Load": true, "delete": true,
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if !allowed[tkCallName(call.Fun)] {
			t.Errorf("tkForgetHandler calls %s (offset %d): this section holds a non-reentrant lock on the "+
				"service thread, so anything that can pump lets an incoming callback re-enter it here",
				tkCallName(call.Fun), call.Pos())
		}
		return true
	})
}

// tkCallName renders a call's function expression as a dotted name, or "" if it
// is not a plain chain of identifiers and selectors.
func tkCallName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		if inner := tkCallName(x.X); inner != "" {
			return inner + "." + x.Sel.Name
		}
	}
	return ""
}

// The service thread's record of a started hide is cleared by an act that
// thread performs, never by news it is handed. A pane event reaches it through
// the command queue with nothing on it to say how old the event is — the
// callback runs whenever the apartment happens to pump — and it speaks for ONE
// registration, while the record is process-wide and the two windows advised
// over the one shared pane each raise their own. Zeroing it from there erases
// a close that another window, or an earlier moment, had already begun: p86
// with a shared word, p87 with the handed-on stamp, and this.
func TestNoQueuedPaneEventClearsTheServiceRecord(t *testing.T) {
	f := tkWindowsAST(t)

	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Store" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "tkHideDeadlineNs" {
			return true
		}
		if lit, ok := call.Args[0].(*ast.BasicLit); !ok || lit.Value != "0" {
			return true
		}
		for i := len(stack) - 1; i >= 0; i-- {
			cc, ok := stack[i].(*ast.CaseClause)
			if !ok || !tkDispatchCase(cc) {
				continue
			}
			t.Errorf("tkHideDeadlineNs is cleared from inside a command-queue case (offset %d): a queued "+
				"pane event carries no evidence of its own age and speaks for one registration, so a "+
				"process-wide close zeroed from there is a close nobody is left holding. Clear it where "+
				"this thread commits a show itself", call.Pos())
		}
		return true
	})
}

// tkDispatchCase reports whether cc is a case of the command-queue switch, i.e.
// one selecting on a tkCmd* kind. An ordinary switch over a local result is not
// news from the queue and carries no staleness with it.
func tkDispatchCase(cc *ast.CaseClause) bool {
	for _, e := range cc.List {
		if id, ok := e.(*ast.Ident); ok && strings.HasPrefix(id.Name, "tkCmd") {
			return true
		}
	}
	return false
}

func TestTheClosingPredicateConsultsBothRecords(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "platformKeyboardClosing")

	for _, name := range []string{"tkHideDeadlineNs", "tkAnyPaneHiding", "tkHideStillClosing"} {
		if !tkMentions(fn.Body, name) {
			t.Errorf("platformKeyboardClosing does not consult %s: a predicate that reads one record of a "+
				"started hide answers 'no' for every hide the other one holds", name)
		}
	}
}

func TestAdoptingAVisiblePaneConsultsTheTapAndTheStampTogether(t *testing.T) {
	f := tkWindowsAST(t)

	// The one call that arms a re-show poll: tkEnqueue of a show with reshow set.
	var stack []ast.Node
	var cond ast.Expr
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "tkEnqueue" {
			return true
		}
		armsPoll := false
		ast.Inspect(call, func(m ast.Node) bool {
			kv, ok := m.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "reshow" {
				if v, ok := kv.Value.(*ast.Ident); ok && v.Name == "true" {
					armsPoll = true
				}
			}
			return true
		})
		if !armsPoll {
			return true
		}
		for i := len(stack) - 1; i >= 0; i-- {
			if ifs, ok := stack[i].(*ast.IfStmt); ok {
				cond = ifs.Cond
				break
			}
		}
		return true
	})
	if cond == nil {
		t.Fatal("nothing arms a re-show poll under a condition any more: a tap that meets a closing pane has " +
			"no way left to get its keyboard back")
	}

	// Both terms, joined by OR. Each covers a case the other cannot see: the tap
	// flag knows about a hide that started BEFORE the tap and may since have
	// expired; the live read knows about one that started while this command was
	// running. Under AND neither case is honored on its own.
	var joined bool
	ast.Inspect(cond, func(n ast.Node) bool {
		be, ok := n.(*ast.BinaryExpr)
		if !ok || be.Op != token.LOR {
			return true
		}
		if tkMentions(be, "closing") && tkMentions(be, "platformKeyboardClosing") {
			joined = true
		}
		return true
	})
	if !joined {
		t.Error("the re-show poll is not armed on (the tap's closing flag OR a live read of the started-hide " +
			"records): dropping the live read loses a hide that begins during the show's own settle wait, and " +
			"dropping the tap flag loses one whose record expired while the command sat in the queue")
	}
}

// The finding that produced these three tests: the resolving helpers handed the
// lock back before the callback assigned (or read) its pane-event generation and
// enqueued the command carrying it. COM serializes nothing for an in-proc MTA
// object — the Showing and the Hiding of ONE registration can be executing at
// the same time on two RPC threads — so a Hiding could take its generation from
// the gap between a Showing's retire and that Showing's own Add, and then land
// its command in the queue AHEAD of it. Processed in that order the Hiding is
// either dropped as stale against the newer shownGen, or honored and then undone
// by the Showing behind it, which republishes paneVisible for a keyboard that is
// already gone. The stamp, the generation and the command have to leave the
// callback as one step, so the work is handed INTO the helper as a closure.
func TestThePaneEventAndItsCommandLeaveTheCallbackTogether(t *testing.T) {
	f := tkWindowsAST(t)

	for _, c := range []struct{ cb, helper string }{
		{"Showing", "tkRetireHiding"},
		{"Hiding", "tkStampHiding"},
	} {
		cb := tkVtblCallback(t, f, c.cb)
		calls := tkCallsTo(cb, c.helper)
		if len(calls) != 1 {
			t.Fatalf("the %s callback calls %s %d time(s), want exactly 1", c.cb, c.helper, len(calls))
		}
		if len(calls[0].Args) != 2 {
			t.Fatalf("the %s callback passes %s %d argument(s), want 2: everything this callback does with the "+
				"handler has to be handed IN, so it runs inside the same hold of tkHandlerMu that resolved it",
				c.cb, c.helper, len(calls[0].Args))
		}
		body, ok := calls[0].Args[1].(*ast.FuncLit)
		if !ok {
			t.Fatalf("the %s callback hands %s a %T rather than a function literal", c.cb, c.helper, calls[0].Args[1])
		}
		inside := func(n ast.Node) bool { return n.Pos() >= body.Pos() && n.End() <= body.End() }

		for _, enq := range tkCallsTo(cb, "tkEnqueue") {
			if !inside(enq) {
				t.Errorf("the %s callback enqueues its command (offset %d) outside the section %s holds: the "+
					"generation the command carries and its position in the queue are one fact, and a callback "+
					"of the other kind slipping between them puts the two out of agreement — the service thread "+
					"then reads an order that never happened", c.cb, enq.Pos(), c.helper)
			}
		}

		var loose []token.Pos
		ast.Inspect(cb, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok || id.Name != "paneEventGen" || inside(id) {
				return true
			}
			loose = append(loose, id.Pos())
			return true
		})
		if len(loose) != 0 {
			t.Errorf("the %s callback touches paneEventGen outside the section %s holds (offsets %v): that "+
				"counter is the only thing ordering these events against each other, and a bump or a read taken "+
				"with the lock already given back is ordered against nothing", c.cb, c.helper, loose)
		}

		var asStmt bool
		ast.Inspect(cb, func(n ast.Node) bool {
			if es, ok := n.(*ast.ExprStmt); ok && es.X == calls[0] {
				asStmt = true
			}
			return true
		})
		if !asStmt {
			t.Errorf("the %s callback uses what %s evaluates to: there must be nothing to use. A handler still "+
				"in hand after the section has ended is an invitation to do exactly the work that may not happen "+
				"there", c.cb, c.helper)
		}
	}
}

func TestTheResolvingHelpersDoTheCallbacksWorkThemselves(t *testing.T) {
	f := tkWindowsAST(t)

	for _, name := range []string{"tkStampHiding", "tkRetireHiding"} {
		fn := tkFuncDecl(t, f, name)

		// Nothing to return is what makes the split impossible to write. While
		// these handed the handler back, the callback could always take its
		// generation and enqueue its command one line below the section.
		if fn.Type.Results != nil {
			t.Errorf("%s returns a value: handing the handler back out is precisely how the generation and the "+
				"command came to be taken after the lock was released", name)
		}
		if n := len(fn.Type.Params.List); n != 2 {
			t.Fatalf("%s takes %d parameter group(s), want 2: the pointer to resolve, and the work to run under "+
				"the lock", name, n)
		}
		p := fn.Type.Params.List[1]
		if _, ok := p.Type.(*ast.FuncType); !ok {
			t.Fatalf("%s's second parameter is a %T, not a function: the callback's work has to be something "+
				"this can call while it still holds tkHandlerMu", name, p.Type)
		}
		if len(p.Names) != 1 {
			t.Fatalf("%s's second parameter is unnamed, so nothing can call it", name)
		}

		calls := tkCallsTo(fn.Body, p.Names[0].Name)
		if len(calls) != 1 {
			t.Fatalf("%s calls %s %d time(s), want exactly 1: never, and the pane event is silently dropped; "+
				"twice, and its command is enqueued twice", name, p.Names[0].Name, len(calls))
		}
		stores := tkStoresToField(fn.Body, "h", "hide")
		if len(stores) == 1 && calls[0].Pos() < stores[0].Pos() {
			t.Errorf("%s runs the callback's work before it writes the hide slot: the command that work "+
				"enqueues is answered against a record that is not there yet", name)
		}
	}
}

func TestNothingInThePaneCallbacksCanPumpUnderTheLock(t *testing.T) {
	f := tkWindowsAST(t)

	// These bodies now run inside tkHandlerMu. sync.Mutex is not reentrant and
	// this is the thread COM delivers on, so a COM call here — anything that
	// dispatches — would admit the next callback into a lock this one still
	// holds. An allow-list rather than a search for known-bad names: the point
	// is that adding a call in here has to be a deliberate act. tkEnqueue is on
	// it because it dispatches nothing: a slice append under tkCmdMu, then a
	// SetEvent.
	allowed := map[string]bool{
		"tkRetireHiding":          true,
		"tkStampHiding":           true,
		"tkEnqueue":               true,
		"tkDropSynthShowMark":     true,
		"tkClaimSynthShow":        true,
		"tkClaimOwnPaneShow":      true,
		"tkPaneShowSeq.Add":       true,
		"h.kbd.paneEventGen.Add":  true,
		"h.kbd.paneEventGen.Load": true,
		"h.kbd.shownByUs.Load":    true,
	}
	for _, name := range []string{"Showing", "Hiding"} {
		cb := tkVtblCallback(t, f, name)
		ast.Inspect(cb, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fn := tkCallName(call.Fun); fn != "" && !allowed[fn] {
				t.Errorf("the %s callback calls %s (offset %d): its body runs under a non-reentrant lock on the "+
					"thread COM delivers on, so anything that can pump lets the next callback re-enter that lock "+
					"here and the apartment stops", name, fn, call.Pos())
			}
			return true
		})
	}
}
