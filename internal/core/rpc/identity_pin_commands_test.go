package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/rpc"
)

func TestIdentityPinCommandsKeepTheirTransportWhenUnavailable(t *testing.T) {
	t.Parallel()
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)
	for command, want := range map[string]rpc.TransportPolicy{
		"previewIdentityPinRemoval":   rpc.TransportLoopbackOnly,
		"confirmIdentityPinRemoval":   rpc.TransportLoopbackOnly,
		"previewIdentityPinRemovalUI": rpc.TransportInProcessOnly,
		"confirmIdentityPinRemovalUI": rpc.TransportInProcessOnly,
	} {
		got, found := table.TransportPolicyFor(command)
		if !found || got != want {
			t.Errorf("%s: registered=%v policy=%v want=%v", command, found, got, want)
		}
	}
}

var _ rpc.IdentityPinProvider = (*node.Service)(nil)

func TestIdentityPinCommandsParseOnlyExplicitSingleIdentityConfirmation(t *testing.T) {
	t.Parallel()
	provider := rpcmocks.NewMockIdentityPinProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterIdentityPinCommands(table, provider)
	id := domain.PeerIdentity{9}
	token := domain.IdentityPinConfirmationToken(strings.Repeat("ab", 32))
	preview := domain.IdentityPinRemovalPreview{Identity: id, PinnedAt: time.Unix(1780000000, 0), Consequence: domain.IdentityPinRemovalConsequence, ConsequenceVersion: domain.CurrentIdentityPinConsequenceVersion, Token: token, RemovalAvailable: false}
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, id).Return(preview, nil).Twice()
	provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, id, token).Return(domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinRemovalUnavailable).Twice()
	for _, suffix := range []string{"", "UI"} {
		response := table.Execute(rpc.CommandRequest{Name: "previewIdentityPinRemoval" + suffix, Args: map[string]interface{}{"identity": id.String()}, Ctx: context.Background()})
		if response.Error != nil {
			t.Fatal(response.Error)
		}
		var got rpc.IdentityPinRemovalPreviewDTO
		if err := json.Unmarshal(response.Data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Identity != id || got.ConfirmToken != token || got.Consequence != preview.Consequence || got.RemovalAvailable {
			t.Fatalf("wrong preview: %+v", got)
		}
		response = table.Execute(rpc.CommandRequest{Name: "confirmIdentityPinRemoval" + suffix, Args: map[string]interface{}{"identity": id.String(), "confirm": string(token)}})
		if response.ErrorKind != rpc.ErrUnavailable || !errors.Is(response.Error, domain.ErrIdentityPinRemovalUnavailable) {
			t.Fatalf("unfenced confirm: %+v", response)
		}
	}
	for _, args := range []map[string]interface{}{
		{}, {"identity": ""}, {"identity": strings.Repeat("0", 40)}, {"identity": "all"}, {"identity": []string{id.String()}}, {"identity": id.String()}, {"identity": id.String(), "confirm": true}, {"identity": id.String(), "confirm": "wrong"}, {"identity": id.String(), "confirm": strings.Repeat("z", 64)},
	} {
		response := table.Execute(rpc.CommandRequest{Name: "confirmIdentityPinRemovalUI", Args: args})
		if response.Error == nil || response.ErrorKind != rpc.ErrValidation {
			t.Fatalf("invalid confirmation reached provider: %+v", response)
		}
	}
}

func TestIdentityPinCommandsRejectHTTPBeforeProviderDispatch(t *testing.T) {
	t.Parallel()
	for _, auth := range []bool{false, true} {
		table := rpc.NewCommandTable()
		rpc.RegisterIdentityPinCommands(table, rpcmocks.NewMockIdentityPinProvider(t))
		cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
		if auth {
			cfg.Username, cfg.Password = "u", "p"
		}
		server, err := rpc.NewServer(cfg, table)
		if err != nil {
			t.Fatal(err)
		}
		for _, command := range []string{"previewIdentityPinRemoval", "confirmIdentityPinRemoval", "previewIdentityPinRemovalUI", "confirmIdentityPinRemovalUI"} {
			request := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec", strings.NewReader(fmt.Sprintf(`{"command":%q,"args":{"identity":"0900000000000000000000000000000000000000","confirm":"%s"}}`, command, strings.Repeat("ab", 32))))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Forwarded-For", "127.0.0.1")
			if auth {
				request.SetBasicAuth("u", "p")
			}
			response, err := server.Test(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusForbidden {
				t.Fatalf("%s auth=%v: %d %s", command, auth, response.StatusCode, body)
			}
		}
	}
}

func TestIdentityPinUICommandsRefuseEvenAuthenticatedLoopbackHTTP(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterIdentityPinCommands(table, rpcmocks.NewMockIdentityPinProvider(t))
	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
	startServer(t, cfg, table)
	for _, command := range []string{"previewIdentityPinRemovalUI", "confirmIdentityPinRemovalUI"} {
		request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+cfg.Port+"/rpc/v1/exec", strings.NewReader(fmt.Sprintf(`{"command":%q}`, command)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.SetBasicAuth("u", "p")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: status=%d", command, response.StatusCode)
		}
	}
}

func TestIdentityPinRPCAcceptsOnlyAuthenticatedLoopbackAndKeepsConfirmUnavailable(t *testing.T) {
	for _, authenticatedListener := range []bool{false, true} {
		t.Run(fmt.Sprintf("auth_%v", authenticatedListener), func(t *testing.T) {
			provider := rpcmocks.NewMockIdentityPinProvider(t)
			table := rpc.NewCommandTable()
			rpc.RegisterIdentityPinCommands(table, provider)
			cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t)}
			if authenticatedListener {
				cfg.Username, cfg.Password = "u", "p"
			}
			startServer(t, cfg, table)
			id := domain.PeerIdentity{9}
			token := domain.IdentityPinConfirmationToken(strings.Repeat("ab", 32))
			if authenticatedListener {
				provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, id).Return(domain.IdentityPinRemovalPreview{Identity: id, Token: token, Consequence: domain.IdentityPinRemovalConsequence}, nil).Once()
				provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, id, token).Return(domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinRemovalUnavailable).Once()
			}
			for _, credentials := range []bool{false, true} {
				for _, command := range []string{"previewIdentityPinRemoval", "confirmIdentityPinRemoval"} {
					body := fmt.Sprintf(`{"command":%q,"args":{"identity":%q,"confirm":%q}}`, command, id.String(), string(token))
					request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+cfg.Port+"/rpc/v1/exec", strings.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					request.Header.Set("Content-Type", "application/json")
					if credentials {
						request.SetBasicAuth("u", "p")
					}
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					if err := response.Body.Close(); err != nil {
						t.Fatal(err)
					}
					want := http.StatusForbidden
					if authenticatedListener {
						want = http.StatusUnauthorized
						if credentials {
							want = http.StatusOK
							if command == "confirmIdentityPinRemoval" {
								want = http.StatusServiceUnavailable
							}
						}
					}
					if response.StatusCode != want {
						t.Fatalf("%s credentials=%v: status=%d want=%d", command, credentials, response.StatusCode, want)
					}
				}
			}
		})
	}
}
