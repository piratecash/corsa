package transport

import "context"

type Network interface {
	Run(context.Context) error
	Peers() []Peer
}
