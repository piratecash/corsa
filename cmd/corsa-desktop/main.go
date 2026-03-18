package main

import (
	"log"

	"corsa/internal/app/desktop"
)

func main() {
	if err := desktop.Run(); err != nil {
		log.Fatal(err)
	}
}
