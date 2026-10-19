// Package loadstand supplies process-only infrastructure for the load stand.
// Functional loopback execution is not a resource-acceptance or namespace stand.
package loadstand

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ErrSecretSerialization = errors.New("load stand credentials cannot be serialized")
var ErrSecretLeak = errors.New("secret found in a public load stand artifact")
var ErrArtifactTooLarge = errors.New("load stand artifact exceeds scan bound")
var ErrUnsupportedArtifact = errors.New("public scan artifact must be an unchanged regular file")

type secretMaterial struct{ raw []byte }

// Credentials never retains a printable password value inside an enclosing
// config: the secret is behind a pointer and all formatting is redacted.
type Credentials struct {
	username string
	password *secretMaterial
}

func NewCredentials() (*Credentials, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("credential entropy: %w", err)
	}
	return &Credentials{username: "loadstand", password: &secretMaterial{raw: raw}}, nil
}
func credentialsFromEnvironment(username, password string) (*Credentials, error) {
	raw, err := hex.DecodeString(password)
	if err != nil || len(raw) != 32 || username != "loadstand" {
		return nil, errors.New("invalid child credentials")
	}
	return &Credentials{username: username, password: &secretMaterial{raw: raw}}, nil
}
func (c Credentials) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "Credentials([redacted])")
}
func (c Credentials) MarshalJSON() ([]byte, error) { return nil, ErrSecretSerialization }
func (c *Credentials) UnmarshalJSON([]byte) error  { return ErrSecretSerialization }
func (c *Credentials) passwordText() string        { return hex.EncodeToString(c.password.raw) }
func (c *Credentials) valid() bool {
	return c != nil && c.password != nil && len(c.password.raw) == 32 && c.username == "loadstand"
}

// ScanPublicArtifacts must execute in the identity-owning child when supplied
// identity material. It never copies that material into a parent journal.
func ScanPublicArtifacts(ctx context.Context, secrets [][]byte, paths []string) error {
	patterns := secretPatterns(secrets)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		contents, err := readPublicArtifact(ctx, path)
		if err != nil {
			return err
		}
		if len(contents) > 8<<20 {
			return ErrArtifactTooLarge
		}
		for _, pattern := range patterns {
			if bytes.Contains(contents, pattern) {
				return ErrSecretLeak
			}
		}
	}
	return nil
}
func secretPatterns(secrets [][]byte) [][]byte {
	var patterns [][]byte
	for _, secret := range secrets {
		if len(secret) == 0 {
			continue
		}
		forms := []string{string(secret), hex.EncodeToString(secret), strings.ToUpper(hex.EncodeToString(secret)), base64.StdEncoding.EncodeToString(secret), base64.RawStdEncoding.EncodeToString(secret), base64.URLEncoding.EncodeToString(secret), base64.RawURLEncoding.EncodeToString(secret), fmt.Sprintf("%d", secret)}
		for _, form := range forms {
			patterns = append(patterns, []byte(form))
		}
	}
	return patterns
}

// ScanPublic checks this credential's binary and environment representations.
// It exposes only an outcome; callers cannot retrieve secret material.
func (c *Credentials) ScanPublic(ctx context.Context, paths []string) error {
	if !c.valid() {
		return ErrInvalidConfiguration
	}
	return ScanPublicArtifacts(ctx, [][]byte{c.password.raw, []byte(c.passwordText())}, paths)
}

// ScanPublicArtifactFD reads the same private regular capability the launch
// owner supplied. It never reopens a pathname or closes the caller's handle.
func ScanPublicArtifactFD(ctx context.Context, secrets [][]byte, file *os.File, limit ArtifactReadLimit) error {
	raw, err := readPrivateFinalFD(ctx, file, limit)
	if err != nil {
		return err
	}
	for _, pattern := range secretPatterns(secrets) {
		if bytes.Contains(raw, pattern) {
			return ErrSecretLeak
		}
	}
	return nil
}
func (c *Credentials) ScanPublicFD(ctx context.Context, file *os.File, limit ArtifactReadLimit) error {
	if !c.valid() {
		return ErrInvalidConfiguration
	}
	return ScanPublicArtifactFD(ctx, [][]byte{c.password.raw, []byte(c.passwordText())}, file, limit)
}
