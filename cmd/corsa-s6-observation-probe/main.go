//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

// Separate diagnostic command; ordinary daemon/Desktop/SDK roots are unchanged.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	appnode "github.com/piratecash/corsa/internal/app/node"
	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/crashlog"
)

type entryError string

func (e entryError) Error() string { return string(e) }

type entryOptions struct {
	nonce         string
	input, output int
	version       bool
}

func entryArgs(args []string) (entryOptions, error) {
	if len(args) == 1 && args[0] == "--version" {
		return entryOptions{version: true}, nil
	}
	bad := func() (entryOptions, error) { return entryOptions{}, entryError("entry-arguments") }
	if len(args) != 3 || !strings.HasPrefix(args[0], "--nonce=") || !strings.HasPrefix(args[1], "--request-fd=") || !strings.HasPrefix(args[2], "--response-fd=") {
		return bad()
	}
	nonce := strings.TrimPrefix(args[0], "--nonce=")
	if len(nonce) != 32 || strings.Trim(nonce, "0123456789abcdef") != "" {
		return bad()
	}
	fd := func(raw string) (int, error) {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 10 || n > 1<<20 || strconv.Itoa(n) != raw {
			return 0, entryError("entry-fd-number")
		}
		return n, nil
	}
	in, err := fd(strings.TrimPrefix(args[1], "--request-fd="))
	if err != nil {
		return bad()
	}
	out, err := fd(strings.TrimPrefix(args[2], "--response-fd="))
	if err != nil || in == out {
		return bad()
	}
	return entryOptions{nonce: nonce, input: in, output: out}, nil
}

func entryEnvironment(getenv func(string) string, lookup func(string) (string, bool)) error {
	for _, key := range []string{"CORSA_STARTUP_FENCE_CANDIDATE", "CORSA_STARTUP_FENCE_FD", "CORSA_STARTUP_FENCE_LAUNCHER", "CORSA_STARTUP_FENCE_STORE"} {
		if _, ok := lookup(key); ok {
			return entryError("entry-C3-not-supported")
		}
	}
	if getenv("CORSA_PPROF_ADDR") != "" || getenv("CORSA_MEM_LIMIT_BYTES") != "" {
		return entryError("entry-profile-override")
	}
	data := getenv(appdata.DataDirEnv)
	if data == "" || !filepath.IsAbs(data) {
		return entryError("entry-explicit-data-root")
	}
	return nil
}

func construct(init initFrame) (*appnode.App, error) {
	if init.Mode == "OFF" {
		return appnode.New(), nil
	}
	if init.Mode == "K10" {
		return appnode.NewWithTerminalObservation(init.Request.Terminal)
	}
	return appnode.NewWithS6ObservationProbe(init.Request)
}

func entryRunAllowed(ctx context.Context, end, now time.Time) error {
	if ctx == nil || ctx.Err() != nil || !now.Before(end) {
		return entryError("entry-deadline-or-cancel")
	}
	return nil
}

func fail(reason string) int { fmt.Fprintln(os.Stderr, "s6-observation-probe: "+reason); return 2 }
func main()                  { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	// One software deadline includes channel admission and construction. It is
	// not a physical bound on a blocked original mutex/FS or process destruction.
	end := time.Now().Add(120 * time.Second)
	options, err := entryArgs(args)
	if err != nil {
		return fail("arguments")
	}
	if options.version {
		fmt.Println(config.CorsaVersion)
		return 0
	}
	if err = entryEnvironment(os.Getenv, os.LookupEnv); err != nil {
		return fail("environment")
	}
	pipes, err := adoptControlPipes(options.input, options.output)
	if err != nil {
		return fail("control-pipes")
	}
	defer pipes.close()
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithDeadline(signalCtx, end)
	defer cancel()
	// Preserve the stock anchor-before-crashlog/identity ordering. No real user
	// path is selected implicitly by this separate diagnostic command.
	if err = appdata.Anchor(); err != nil {
		return fail("data-anchor")
	}
	channel := newChannel(pipes, options.nonce)
	initEnd := time.Now().Add(10 * time.Second)
	if end.Before(initEnd) {
		initEnd = end
	}
	init, err := channel.readInit(initEnd)
	if err != nil {
		return fail("INIT")
	}
	if err = entryRunAllowed(ctx, end, time.Now()); err != nil {
		return fail("INIT-deadline-or-cancel")
	}
	cleanup := crashlog.Setup()
	defer cleanup()
	app, err := construct(init)
	if err != nil {
		return fail("construction")
	}
	// Original App.Run can open listeners even with an already cancelled
	// context. Never use that call as cleanup for late construction. Process
	// exit and the future parent's owned reap remain the cleanup boundary.
	if err = entryRunAllowed(ctx, end, time.Now()); err != nil {
		return fail("construction-deadline-or-cancel")
	}
	if init.Mode == "K10" {
		source := app.TerminalObservation()
		if source == nil {
			return fail("K10-source")
		}
		read := func() terminalBoundary {
			return terminalBoundary{Identity: source.Identity().String(), Key: source.Key(), Snapshot: source.Snapshot()}
		}
		if err = channel.serveTerminalLifecycle(ctx, read, app.RunWithS6TerminalDrain); err != nil {
			return fail("K10-lifecycle")
		}
		return 0
	}
	var returned atomic.Bool
	done := make(chan error, 1)
	go func() { defer cancel(); done <- channel.serve(ctx, app, init.Mode, &returned) }()
	// CONSTRUCTED is not Run/listener readiness. Every actual Run return fences
	// subsequent captures independently of readout.Available or constructor state.
	if err = entryRunAllowed(ctx, end, time.Now()); err != nil {
		pipes.close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return fail("pre-Run-deadline-or-cancel")
	}
	runErr := app.Run(ctx)
	returned.Store(true)
	cancel()
	pipes.close()
	select {
	case channelErr := <-done:
		if runErr != nil || channelErr != nil {
			return fail("run-or-channel")
		}
	case <-time.After(time.Second):
		return fail("channel-join-unknown")
	}
	return 0
}
