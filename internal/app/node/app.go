package node

import (
	"context"

	"github.com/rs/zerolog/log"

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
	log.Info().
		Str("version", config.CorsaVersion).
		Str("wire", config.CorsaWireVersion).
		Str("listen", a.service.ListenAddress()).
		Msg("starting node")
	return a.service.Run(ctx)
}
