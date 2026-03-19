package desktop

import (
	"context"
	"log"

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

	log.Printf("desktop identity address=%s path=%s", id.Address, cfg.Node.IdentityPath)

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
	window := NewWindow(client, runtime, prefs)
	return window.Run()
}
