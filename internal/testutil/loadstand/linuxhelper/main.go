package main

import (
	"context"
	"os"

	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

func main() {
	if loadstand.RunLinuxHelper(context.Background()) != nil {
		os.Exit(1)
	}
}
