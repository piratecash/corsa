package loadstand

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrInvalidConfiguration = errors.New("invalid process stand configuration")

// ChildConfig contains public settings only. Credentials are passed separately
// to the environment builder and are never part of a provenance document.
type ChildConfig struct {
	DataDir   string
	Listen    domain.ListenAddress
	RPC       domain.ListenAddress
	Bootstrap []domain.PeerAddress
	Listener  bool
	Procs     int
}

func (c ChildConfig) ValidateLoopback() error {
	if !filepath.IsAbs(c.DataDir) || c.Procs < 1 || len(c.Bootstrap) == 0 {
		return ErrInvalidConfiguration
	}
	for _, address := range append([]string{string(c.Listen), string(c.RPC)}, peerAddressStrings(c.Bootstrap)...) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return ErrInvalidConfiguration
		}
		ip, err := netip.ParseAddr(host)
		numeric, portErr := strconv.Atoi(port)
		if err != nil || !ip.IsLoopback() || host != "127.0.0.1" || portErr != nil || numeric < 1 || numeric > 65535 {
			return ErrInvalidConfiguration
		}
	}
	return nil
}
func peerAddressStrings(addresses []domain.PeerAddress) []string {
	names := make([]string, len(addresses))
	for i, address := range addresses {
		names[i] = string(address)
	}
	return names
}
func ReadChildEnvironment(getenv func(string) string) (ChildConfig, *Credentials, error) {
	procs, err := strconv.Atoi(getenv("GOMAXPROCS"))
	if err != nil {
		return ChildConfig{}, nil, ErrInvalidConfiguration
	}
	listener, err := strconv.ParseBool(getenv("LOADSTAND_LISTENER"))
	if err != nil {
		return ChildConfig{}, nil, ErrInvalidConfiguration
	}
	config := ChildConfig{DataDir: getenv("LOADSTAND_DATA_DIR"), Listen: domain.ListenAddress(getenv("LOADSTAND_LISTEN")), RPC: domain.ListenAddress(getenv("LOADSTAND_RPC")), Listener: listener, Procs: procs}
	for _, address := range strings.Split(getenv("LOADSTAND_BOOTSTRAP"), ",") {
		config.Bootstrap = append(config.Bootstrap, domain.PeerAddress(address))
	}
	if err := config.ValidateLoopback(); err != nil {
		return ChildConfig{}, nil, err
	}
	credentials, err := credentialsFromEnvironment(getenv("LOADSTAND_RPC_USER"), getenv("LOADSTAND_RPC_PASSWORD"))
	return config, credentials, err
}
func (c ChildConfig) environment(credentials *Credentials) ([]string, error) {
	if err := c.ValidateLoopback(); err != nil {
		return nil, err
	}
	if !credentials.valid() {
		return nil, ErrInvalidConfiguration
	}
	return []string{"PATH=/usr/bin:/bin", "HOME=" + c.DataDir, "TMPDIR=" + filepath.Join(c.DataDir, "tmp"), "TZ=UTC", "GOMAXPROCS=" + strconv.Itoa(c.Procs), "LOADSTAND_DATA_DIR=" + c.DataDir, "LOADSTAND_LISTEN=" + string(c.Listen), "LOADSTAND_RPC=" + string(c.RPC), "LOADSTAND_BOOTSTRAP=" + strings.Join(peerAddressStrings(c.Bootstrap), ","), "LOADSTAND_LISTENER=" + strconv.FormatBool(c.Listener), "LOADSTAND_RPC_USER=" + credentials.username, "LOADSTAND_RPC_PASSWORD=" + credentials.passwordText()}, nil
}
func ValidateAmbientEnvironment(environ []string) error {
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "CORSA_") && name != "CORSA_VERSION" {
			return fmt.Errorf("%w: ambient CORSA configuration", ErrInvalidConfiguration)
		}
	}
	return nil
}
