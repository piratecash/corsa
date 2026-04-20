package main

import (
	"context"
	"log"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/piratecash/corsa/sdk"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := sdk.DefaultConfig()
	cfg.App.Profile = "sic-parvis-magna-bot"
	cfg.Node.ListenAddress = ":64648"
	cfg.Node.AdvertiseAddress = "127.0.0.1:64648"
	cfg.Node.ChatLogDir = ".corsa-bot"
	cfg.Node.IdentityPath = filepath.Join(".corsa-bot", "identity-64648.json")
	cfg.Node.TrustStorePath = filepath.Join(".corsa-bot", "trust-64648.json")
	cfg.Node.QueueStatePath = filepath.Join(".corsa-bot", "queue-64648.json")
	cfg.Node.PeersStatePath = filepath.Join(".corsa-bot", "peers-64648.json")

	// SDK does not auto-generate identity — ensure the file exists for this example.
	// Production bots should supply NodeConfig.PrivateKey instead.
	if err := sdk.EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		log.Fatalf("ensure identity: %v", err)
	}

	runtime, err := sdk.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Start(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("bot address: %s", runtime.Address())
	log.Printf("listen address: %s", runtime.ListenAddress())

	messages := runtime.SubscribeDirectMessages(ctx)
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- runtime.Wait()
	}()

	for {
		select {
		case err := <-waitCh:
			if err != nil {
				log.Fatal(err)
			}
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if _, err := runtime.Execute("send_dm", map[string]interface{}{
				"to":   msg.Sender,
				"body": "Sic Parvis Magna",
			}); err != nil {
				log.Printf("reply to %s failed: %v", msg.Sender, err)
				continue
			}
			log.Printf("replied to %s for message %s", msg.Sender, msg.ID)
		}
	}
}
