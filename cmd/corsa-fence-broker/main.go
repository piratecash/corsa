// corsa-fence-broker is the independently provisioned restart ledger. It is a
// candidate deployment; installing this binary does not enable node RPCs.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/piratecash/corsa/internal/startupfence"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: corsa-fence-broker serve|enroll|status|finalize [flags]")
	}
	flags := flag.NewFlagSet("corsa-fence-broker", flag.ContinueOnError)
	database := flags.String("database", "", "absolute ledger path in a private directory")
	listen := flags.String("listen", "127.0.0.1:9443", "HTTPS listener")
	certPath := flags.String("cert", "", "own owner-only certificate PEM")
	keyPath := flags.String("key", "", "own owner-only private key PEM")
	caPath := flags.String("ca", "", "owner-only trusted CA PEM")
	operatorCert := flags.String("operator-cert", "", "operator certificate PEM (serve only)")
	selection := flags.String("selection", "", "owner-only enrollment or exact receipt JSON file")
	endpoint := flags.String("broker", "", "broker HTTPS origin (status/finalize)")
	nodeText := flags.String("node", "", "enrolled 40-hex node identity (status)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "serve":
		cert, roots, err := startupfence.LoadTLSMaterial(*certPath, *keyPath, *caPath)
		if err != nil {
			return err
		}
		operator, err := startupfence.CertificateFingerprint(*operatorCert)
		if err != nil {
			return err
		}
		ledger, err := startupfence.OpenLedger(ctx, *database)
		if err != nil {
			return err
		}
		defer func() { _ = ledger.Close() }()
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			return err
		}
		server := &http.Server{Handler: startupfence.BrokerHandler{Ledger: ledger, OperatorFingerprint: operator}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, TLSConfig: startupfence.TLSConfig(cert, roots)}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		err = server.Serve(tls.NewListener(listener, server.TLSConfig))
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case "enroll":
		raw, err := startupfence.ReadConfiguration(*selection)
		if err != nil {
			return err
		}
		var e startupfence.Enrollment
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		ledger, err := startupfence.OpenLedger(ctx, *database)
		if err != nil {
			return err
		}
		defer func() { _ = ledger.Close() }()
		return ledger.Enroll(ctx, e)
	case "status", "finalize":
		cert, roots, err := startupfence.LoadTLSMaterial(*certPath, *keyPath, *caPath)
		if err != nil {
			return err
		}
		client, err := startupfence.NewBrokerClient(*endpoint, roots, cert)
		if err != nil {
			return err
		}
		if args[0] == "finalize" {
			raw, err := startupfence.ReadConfiguration(*selection)
			if err != nil {
				return err
			}
			var receipt startupfence.Receipt
			if err := json.Unmarshal(raw, &receipt); err != nil {
				return err
			}
			status, err := client.Finalize(ctx, receipt)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(status)
		}
		node, err := parseNode(*nodeText)
		if err != nil {
			return err
		}
		status, err := client.Status(ctx, node)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	default:
		return errors.New("unknown broker command")
	}
}
