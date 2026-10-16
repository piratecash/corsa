package service

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The service layer opens no sockets to peers. A peer connection has to go
// through the node's own dial — the SOCKS5 proxy for overlay addresses, the
// shared connection budget, the v2-first session with its pin and binding
// checks — and a dial written here would skip every one of them: the raw v1
// session this package once opened sent its signed hello straight to
// whatever answered the address. It talks to the embedded node in-process
// (LocalRequestFrameCtx) and nothing else. The whole tree is checked, the
// subpackages (filerouter, filetransfer) included.
func TestServiceLayerOpensNoPeerSockets(t *testing.T) {
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, finding := range peerDialFindings(fset, file) {
			t.Errorf("%s — the service layer must not dial peers; use the node's dial path", finding)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the service tree: %v", err)
	}
}

// forbiddenDialAPI is, per import path, the identifiers that open a socket
// or build something that does.
var forbiddenDialAPI = map[string]map[string]struct{}{
	"net": {
		"Dial": {}, "DialTimeout": {}, "DialTCP": {}, "DialUDP": {}, "DialIP": {}, "DialUnix": {}, "Dialer": {},
	},
	"crypto/tls": {
		"Dial": {}, "DialWithDialer": {}, "Dialer": {},
	},
	"net/http": {
		"Client": {}, "DefaultClient": {}, "Transport": {}, "DefaultTransport": {},
		"Get": {}, "Head": {}, "Post": {}, "PostForm": {},
	},
}

// peerDialFindings lists every use of forbiddenDialAPI in file. A package is
// recognised by its import path, not by the name it is used under, so an
// alias does not hide it; a dot import of such a package is itself a finding,
// since its identifiers can no longer be told apart.
func peerDialFindings(fset *token.FileSet, file *ast.File) []string {
	localNames := make(map[string]string)
	var findings []string
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		if _, watched := forbiddenDialAPI[path]; !watched {
			continue
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "." {
			findings = append(findings, fset.Position(spec.Pos()).String()+": dot import of "+path)
			continue
		}
		localNames[name] = path
	}
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Obj != nil {
			return true
		}
		path, imported := localNames[pkg.Name]
		if !imported {
			return true
		}
		if _, forbidden := forbiddenDialAPI[path][sel.Sel.Name]; forbidden {
			findings = append(findings, fset.Position(sel.Pos()).String()+": "+path+"."+sel.Sel.Name)
		}
		return true
	})
	return findings
}

// The guard sees through the ways a dial can be spelled.
func TestPeerDialFindingsSeeEverySpelling(t *testing.T) {
	cases := map[string]struct {
		source string
		found  int
	}{
		"net.Dial":         {`import "net"; func f() { net.Dial("tcp", "x") }`, 1},
		"aliased net":      {`import n "net"; func f() { n.DialTimeout("tcp", "x", 0) }`, 1},
		"net.Dialer":       {`import "net"; var d net.Dialer`, 1},
		"net.DialUDP":      {`import "net"; func f() { net.DialUDP("udp", nil, nil) }`, 1},
		"net.DialIP":       {`import "net"; func f() { net.DialIP("ip", nil, nil) }`, 1},
		"tls.Dial":         {`import "crypto/tls"; func f() { tls.Dial("tcp", "x", nil) }`, 1},
		"aliased tls":      {`import t "crypto/tls"; var d t.Dialer`, 1},
		"http client":      {`import "net/http"; var c = &http.Client{}`, 1},
		"http.Get":         {`import "net/http"; func f() { http.Get("x") }`, 1},
		"default client":   {`import h "net/http"; var c = h.DefaultClient`, 1},
		"dot import":       {`import . "net"; func f() { Dial("tcp", "x") }`, 1},
		"harmless net use": {`import "net"; func f() { net.ParseIP("1.2.3.4") }`, 0},
		"local shadow":     {`import "net"; func f() { net := struct{ Dial int }{}; _ = net.Dial }`, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "case.go", "package p\n"+tc.source, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := peerDialFindings(fset, file); len(got) != tc.found {
				t.Fatalf("findings %v, want %d", got, tc.found)
			}
		})
	}
}
