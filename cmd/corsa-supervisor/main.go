// corsa-supervisor is the Linux-only candidate launcher. The production node
// has not installed this protocol or enabled destructive RPCs yet.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

type configuration struct {
	Broker string                    `json:"broker"`
	Cert   string                    `json:"cert"`
	Key    string                    `json:"key"`
	CA     string                    `json:"ca"`
	Node   domain.PeerIdentity       `json:"node"`
	Store  startupfence.StoreID      `json:"store"`
	Child  startupfence.ChildCommand `json:"child"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	flags := flag.NewFlagSet("corsa-supervisor", flag.ContinueOnError)
	path := flags.String("config", "", "root-owned owner-only configuration outside the node volume")
	candidate := flags.Bool("candidate", false, "explicitly run the review candidate; does not enable node pin removal")
	recovery := flags.Bool("recover", false, "run the configured recovery child in an isolated network namespace")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !*candidate {
		return errors.New("startup-fence workflow is a review candidate; production enablement unavailable")
	}
	raw, err := startupfence.ReadConfiguration(*path)
	if err != nil {
		return err
	}
	var cfg configuration
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	cert, roots, err := startupfence.LoadTLSMaterial(cfg.Cert, cfg.Key, cfg.CA)
	if err != nil {
		return err
	}
	broker, err := startupfence.NewBrokerClient(cfg.Broker, roots, cert)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	status, err := broker.Status(ctx, cfg.Node)
	if err != nil {
		return err
	}
	if status.Enrollment.Store != cfg.Store || status.Enrollment.LauncherProfile != "linux-systemd-v1" || status.Enrollment.FaultDomain == "" {
		return startupfence.ErrUnauthorized
	}
	controller, err := startupfence.NewController(broker, cfg.Node, cfg.Store)
	if err != nil {
		return err
	}
	if *recovery {
		err = controller.StartRecovery(ctx, cfg.Child)
	} else {
		err = controller.Start(ctx, cfg.Child)
	}
	if err != nil {
		return err
	}
	return controller.Wait()
}
