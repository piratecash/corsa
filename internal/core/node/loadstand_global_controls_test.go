package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

type globalObserverProbe struct {
	loadstand.Member
	message domain.GlobalMessage
	calls   int
}

func (p *globalObserverProbe) GlobalReplica(context.Context, domain.MessageID) (domain.GlobalMessage, error) {
	p.calls++
	return p.message, nil
}
func TestLoadStandGlobalObservationCorrelation(t *testing.T) {
	for _, kind := range []string{"valid", "wrong-id", "wrong-topic", "wrong-sender", "wrong-body", "stale-execution", "stale-incarnation", "wrong-observer", "duplicate-poll"} {
		t.Run(kind, func(t *testing.T) {
			id := domain.MessageID("11111111-1111-4111-8111-111111111111")
			sender := idPeerB
			message := domain.GlobalMessage{ID: id, Topic: domain.GlobalMessageTopic, ClaimedSender: sender, Body: "execution nonce body", CreatedAt: time.Now().UTC()}
			incarnation := loadstand.Incarnation{Ordinal: 1, StartedAt: time.Now().UTC()}
			start := loadstand.StartReceipt{Incarnation: incarnation, Public: loadstand.PublicIdentity{Identity: idNodeA}}
			digest := sha256.Sum256([]byte(message.Body))
			probe := &globalObserverProbe{message: message}
			e := &loadStandExecutor{starts: map[loadStandNodeID]loadstand.StartReceipt{"full": start}, members: map[loadStandNodeID]loadstand.Member{"full": probe}, globals: []loadStandExpectedGlobal{{Message: message, BodySHA256: hex.EncodeToString(digest[:]), Observer: "full", ObserverIdentity: idNodeA, Incarnation: incarnation}}}
			switch kind {
			case "wrong-id":
				probe.message.ID = "22222222-2222-4222-8222-222222222222"
			case "wrong-topic":
				probe.message.Topic = "dm"
			case "wrong-sender":
				probe.message.ClaimedSender = idPeerC
			case "wrong-body", "stale-execution":
				probe.message.Body = "other execution"
			case "stale-incarnation":
				e.globals[0].Incarnation.Ordinal++
			case "wrong-observer":
				e.globals[0].ObserverIdentity = idPeerC
			}
			err := e.observeGlobals(t.Context())
			if kind == "valid" || kind == "duplicate-poll" {
				if err != nil || len(e.report.GlobalObservations) != 1 {
					t.Fatal(err)
				}
				if kind == "duplicate-poll" {
					if err := e.observeGlobals(t.Context()); err != nil || len(e.report.GlobalObservations) != 1 || probe.calls != 1 {
						t.Fatal("duplicate observation counted")
					}
				}
			} else if err == nil {
				t.Fatal("forged/stale observation accepted")
			}
		})
	}
}
