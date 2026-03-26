package desktop

import (
	"context"

	"github.com/rs/zerolog/log"

	"corsa/internal/core/config"
	"corsa/internal/core/identity"
	"corsa/internal/core/node"
	"corsa/internal/core/service"
)

func Run() error {
	cfg := config.Default()
	ctx := context.Background()

	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		return err
	}

	log.Info().Str("address", id.Address).Str("path", cfg.Node.IdentityPath).Msg("desktop identity loaded")

	prefs, err := LoadPreferences(preferencePathForIdentity(cfg.Node.IdentityPath))
	if err != nil {
		return err
	}
	if prefs.Language != "" {
		cfg.App.Language = prefs.Language
	}

	nodeService := node.NewService(cfg.Node, id)
	runtime := NewNodeRuntime(nodeService)
	runtime.Start(ctx)

	client := service.NewDesktopClient(cfg.App, cfg.Node, id, nodeService)
	defer func() {
		if err := client.Close(); err != nil {
			log.Error().Err(err).Msg("chatlog close failed")
		} else {
			log.Info().Msg("chatlog closed")
		}
	}()
	window := NewWindow(client, runtime, prefs)
	return window.Run()
}
