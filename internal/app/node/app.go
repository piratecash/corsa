package node

import (
	"context"
	"log"

	"corsa/internal/core/config"
	"corsa/internal/core/identity"
	"corsa/internal/core/node"
)

type App struct {
	service *node.Service
}

func New() *App {
	cfg := config.Default()
	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		panic(err)
	}
	return &App{
		service: node.NewService(cfg.Node, id),
	}
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting node on %s", a.service.ListenAddress())
	return a.service.Run(ctx)
}
