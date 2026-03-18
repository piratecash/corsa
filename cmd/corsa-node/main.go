package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"corsa/internal/app/node"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app := node.New()
	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
