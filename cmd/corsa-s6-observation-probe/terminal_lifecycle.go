//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

package main

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
)

type terminalBoundary = observed[corenode.TerminalObservationSnapshot, corenode.TerminalObservationKey]

type terminalBoundaryFrame struct {
	Version     int              `json:"v"`
	Nonce       string           `json:"nonce"`
	Sequence    int              `json:"seq"`
	Kind        string           `json:"kind"`
	Provisional bool             `json:"provisional"`
	Terminal    terminalBoundary `json:"terminal"`
}

func (c *channel) readTerminalCommand(ctx context.Context, sequence int, kind string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(readIdle)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	raw, err := c.line(requestLimit, deadline)
	if err != nil {
		return err
	}
	var command readFrame
	if canonical(raw, &command) != nil || command != (readFrame{2, c.nonce, sequence, kind}) {
		return entryError("K10-command-binding")
	}
	return ctx.Err()
}

func terminalBoundaryComplete(cut terminalBoundary) bool {
	s := cut.Snapshot
	return !domain.PeerIdentityFromWire(cut.Identity).IsZero() && s.Available && s.SchemaVersion == 1 &&
		s.Population == "RegisteredTransportInstance" && s.Status == "COMPLETE" && s.Instance != ([16]byte{}) &&
		s.Active == 0 && s.Pending == 0 && s.Missing == 0 && s.Registered == s.Removed &&
		s.Removed == s.Classified && s.Removed == s.TerminalSerial && s.Registered == s.RegistrationSerial
}

// Version 2 is a separate, finite terminal-only exchange. START is the
// parent's permission to admit traffic after keeping the initial frame. The
// final frame is provisional until the parent has saved it and reaped us.
func (c *channel) serveTerminalLifecycle(ctx context.Context, read func() terminalBoundary, runAndDrain func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	initial := read()
	if !terminalBoundaryComplete(initial) || initial.Snapshot.Registered != 0 {
		return entryError("K10-initial-incomplete")
	}
	// Only this exchange owns its writes. The outer context survives our
	// own STOP, unlike the context used for running the application.
	var writeFence atomic.Bool
	if err := c.sendBound(ctx, &writeFence, terminalBoundaryFrame{2, c.nonce, -1, "TERMINAL_INITIAL", true, initial}); err != nil {
		return err
	}
	if err := c.readTerminalCommand(ctx, 0, "START"); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan error, 1)
	go func() {
		defer cancel()
		err := c.readTerminalCommand(ctx, 1, "STOP")
		if err == nil {
			end := time.Now().Add(writeBound)
			if outer, ok := ctx.Deadline(); ok && outer.Before(end) {
				end = outer
			}
			if _, tail := c.line(requestLimit, end); !errors.Is(tail, io.EOF) {
				err = entryError("K10-extra-or-unclosed-STOP")
			}
		}
		stop <- err
	}()
	runErr := runAndDrain(runCtx)
	select {
	case err := <-stop:
		if err != nil || runErr != nil || ctx.Err() != nil || runCtx.Err() == nil {
			return entryError("K10-drain-incomplete")
		}
	default:
		// Run ended independently of the STOP exchange. Unblock the sole
		// reader, join it, and refuse rather than waiting for a late STOP.
		c.pipes.close()
		<-stop
		return entryError("K10-run-ended-before-STOP")
	}
	final := read()
	if !terminalBoundaryComplete(final) || final.Key != initial.Key || final.Identity != initial.Identity ||
		final.Snapshot.Instance != initial.Snapshot.Instance {
		return entryError("K10-final-incomplete")
	}
	return c.sendBound(ctx, &writeFence, terminalBoundaryFrame{2, c.nonce, 1, "TERMINAL_FINAL", true, final})
}
