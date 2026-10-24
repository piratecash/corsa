//go:build linux

package loadstand

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/testutil/loadstand/fullwitness"
)

func TestPlannedFullPublicWitnessRegistration(t *testing.T) {
	own, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	public := fullwitness.PublicIdentity{Identity: domain.PeerIdentityFromWire(own.Address), PublicKey: identity.PublicKeyBase64(own.PublicKey), BoxPublicKey: identity.BoxPublicKeyBase64(own.BoxPublicKey), BoxSignature: identity.SignBoxKeyBinding(own)}
	t.Run("immutable-signed-noarg-local-witness", func(t *testing.T) {
		table := rpc.NewCommandTable()
		if err := fullwitness.RegisterPlannedFullPublicIdentity(table, public); err != nil {
			t.Fatal(err)
		}
		policy, known := table.TransportPolicyFor("loadstandFullPublicIdentity")
		if !known || policy != rpc.TransportLoopbackOnly {
			t.Fatal("fixture witness lost authenticated loopback policy", policy, known)
		}
		original := public
		public.BoxPublicKey = "changed-after-registration"
		defer func() { public = original }()
		first := table.Execute(rpc.CommandRequest{Name: "loadstandFullPublicIdentity", Ctx: t.Context()})
		var got fullwitness.PublicIdentity
		if first.Error != nil || json.Unmarshal(first.Data, &got) != nil || got != original || identity.VerifyBoxKeyBinding(got.Identity.String(), got.PublicKey, got.BoxPublicKey, got.BoxSignature) != nil {
			t.Fatal("registered witness did not retain original signed public tuple", first.Error)
		}
		first.Data[0] = 'x'
		second := table.Execute(rpc.CommandRequest{Name: "loadstandFullPublicIdentity", Ctx: t.Context()})
		if json.Unmarshal(second.Data, &got) != nil || got != original {
			t.Fatal("caller mutated retained public encoding")
		}
		legacy := PublicIdentity{Identity: original.Identity, PublicKey: original.PublicKey, BoxPublicKey: original.BoxPublicKey, BoxSignature: original.BoxSignature}
		legacyRaw, err := json.Marshal(legacy)
		if err != nil || !bytes.Equal(second.Data, legacyRaw) {
			t.Fatal("child and consumer public DTO wire differs", err)
		}

		for _, args := range []map[string]interface{}{{"identity": original.Identity.String()}, {"role": "full"}, {"key": original.PublicKey}} {
			r := table.Execute(rpc.CommandRequest{Name: "loadstandFullPublicIdentity", Args: args, Ctx: t.Context()})
			if r.Error == nil || r.ErrorKind != rpc.ErrValidation || len(r.Data) != 0 {
				t.Fatal("witness accepted caller-selected input", args)
			}
		}
	})
	t.Run("startup-collisions-preserve-existing-command", func(t *testing.T) {
		const command = "loadstandFullPublicIdentity"
		for _, collision := range []string{"exact", "case", "alias"} {
			table := rpc.NewCommandTable()
			name := command
			if collision == "case" {
				name = strings.ToUpper(command)
			}
			if collision == "alias" {
				name = "foreignFixtureCommand"
			}
			calls := 0
			table.Register(rpc.CommandInfo{Name: name, Transport: rpc.TransportInProcessOnly}, func(req rpc.CommandRequest) rpc.CommandResponse {
				calls++
				return rpc.CommandResponse{Data: json.RawMessage(`{"foreign":true}`)}
			})
			if collision == "alias" {
				table.RegisterAlias(command, name)
			}
			before := table.Execute(rpc.CommandRequest{Name: command, Ctx: t.Context()})
			policy, known := table.TransportPolicyFor(command)
			if before.Error != nil || !known || policy != rpc.TransportInProcessOnly || calls != 1 {
				t.Fatal("foreign collision fixture invalid", collision)
			}
			if err := fullwitness.RegisterPlannedFullPublicIdentity(table, public); err == nil {
				t.Fatal("startup command collision accepted", collision)
			}
			after := table.Execute(rpc.CommandRequest{Name: command, Ctx: t.Context()})
			afterPolicy, afterKnown := table.TransportPolicyFor(command)
			if after.Error != nil || !bytes.Equal(after.Data, before.Data) || calls != 2 || !afterKnown || afterPolicy != policy {
				t.Fatal("collision replaced foreign handler or transport policy", collision, calls, afterPolicy)
			}
		}
	})
	for _, tc := range []struct {
		name string
		auth bool
	}{{"http-auth-required", true}, {"http-auth-disabled-refused", false}} {
		t.Run(tc.name, func(t *testing.T) {
			table := rpc.NewCommandTable()
			if err := fullwitness.RegisterPlannedFullPublicIdentity(table, public); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			_, port, splitErr := net.SplitHostPort(listener.Addr().String())
			closeErr := listener.Close()
			if splitErr != nil || closeErr != nil {
				t.Fatal(splitErr, closeErr)
			}
			cfg := config.RPC{Host: "127.0.0.1", Port: port}
			if tc.auth {
				cfg.Username, cfg.Password = "fixture-user", "fixture-password"
			}
			server, err := rpc.NewServer(cfg, table)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.ShutdownWithTimeout(3 * time.Second); err != nil {
					t.Error(err)
				}
			})
			if err := server.StartAsync(); err != nil {
				t.Fatal(err)
			}
			transport := &http.Transport{}
			client := &http.Client{Transport: transport, Timeout: time.Second}
			t.Cleanup(transport.CloseIdleConnections)
			for _, authenticated := range []bool{false, true} {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:"+port+"/rpc/v1/exec", strings.NewReader(`{"command":"loadstandFullPublicIdentity"}`))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				if authenticated {
					req.SetBasicAuth("fixture-user", "fixture-password")
				}
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				raw, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil {
					t.Fatal(readErr, closeErr)
				}
				want := http.StatusForbidden
				if tc.auth {
					if authenticated {
						want = http.StatusOK
					} else {
						want = http.StatusUnauthorized
					}
				}
				if response.StatusCode != want {
					t.Fatal("actual HTTP witness transport/auth gate changed", response.StatusCode, want)
				}
				if want == http.StatusOK {
					var got fullwitness.PublicIdentity
					if json.Unmarshal(raw, &got) != nil || got != public {
						t.Fatal("HTTP witness body changed")
					}
				} else if bytes.Contains(raw, []byte(public.BoxSignature)) {
					t.Fatal("refused request returned witness")
				}
			}
		})
	}
}

// This carrier pins the actual measured entry's same-own binding and the
// registration-before-server-start edge; it does not attest an executed ELF.
func TestPlannedFullPublicWitnessSourceWiring(t *testing.T) {
	raw, err := os.ReadFile("fullnode/main.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fullnode/main.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, decl := range file.Decls {
		if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == "run" {
			run = f
		}
	}
	if run == nil {
		t.Fatal("original Full entry run missing")
	}
	want := []string{
		`cfg.DisableDirectMessages = true`,
		`own, err := identity.LoadOrCreate(cfg.IdentityPath)`,
		`service, unstartedCleanup := node.NewServiceForRuntime(cfg, own, ebus.New())`,
		`public := fullwitness.PublicIdentity{Identity: domain.PeerIdentityFromWire(own.Address), PublicKey: identity.PublicKeyBase64(own.PublicKey), BoxPublicKey: identity.BoxPublicKeyBase64(own.BoxPublicKey), BoxSignature: identity.SignBoxKeyBinding(own)}`,
		`if err = fullwitness.RegisterPlannedFullPublicIdentity(table, public); err != nil { return err }`,
		`server, err := rpc.NewServer(rpcConfig, table, service)`,
	}
	next := 0
	for _, stmt := range run.Body.List {
		text, err := linuxRuntimeAuthorityText(fset, stmt)
		if err != nil {
			t.Fatal(err)
		}
		if next < len(want) && text == want[next] {
			next++
		}
	}
	if next != len(want) {
		t.Fatal("Full witness is not bound to original same-own Service before authenticated server", next)
	}
}

// Prevent the direct production edge that made node's internal tests cycle.
// The unchanged combined four-root go-list check is still required to validate
// the complete (including transitive) actual Go test graph.
func TestPlannedFullPublicWitnessParentLayering(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		raw, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), raw, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "github.com/piratecash/corsa/internal/core/rpc" || path == "github.com/piratecash/corsa/internal/testutil/loadstand/fullwitness" {
				t.Fatal("parent production reintroduced RPC/child import cycle edge", entry.Name(), path)
			}
		}
	}
}
