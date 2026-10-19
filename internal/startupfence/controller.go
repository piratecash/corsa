package startupfence

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Controller owns the entire prepare/consume/spawn and removal sequence under
// one mutex. A consumed grant cannot spawn a stale child after BeginRemoval.
// Only the already running child can request removal. No method unfences.
type Controller struct {
	mu                     sync.Mutex
	broker                 *BrokerClient
	node                   domain.PeerIdentity
	store                  StoreID
	lease                  Lease
	epoch                  RuntimeEpoch
	child                  *containedChild
	fenced                 bool
	recovery               *RemovalPermit
	runCtx                 context.Context
	lockFile               *os.File
	recoveryGrant          *StartupGrant
	recoveryReceipt        *Receipt
	acknowledgedReceipt    *Receipt
	serveDone              chan struct{}
	gracefullyClosed       bool
	receiptAcknowledgement CredentialToken
}

type ChildCommand struct {
	Path        string
	Args        []string
	UID         uint32
	GID         uint32
	Environment map[string]EnvironmentValue
	isolated    bool
}

// EnvironmentValue is configuration transported to the node, never printed:
// the explicit allowlist includes the node RPC password, not broker secrets.
type EnvironmentValue string

func (EnvironmentValue) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("[redacted]")) }
func (ChildCommand) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("ChildCommand{redacted}"))
}

func NewController(client *BrokerClient, node domain.PeerIdentity, store StoreID) (*Controller, error) {
	if client == nil || node.IsZero() || store == "" {
		return nil, ErrInvalid
	}
	return &Controller{broker: client, node: node, store: store}, nil
}

func (c *Controller) Start(ctx context.Context, command ChildCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := checkContainment(command); err != nil {
		return err
	}
	if c.fenced {
		return ErrFenced
	}
	if c.child != nil {
		return ErrChanged
	}
	lock, err := lockBinding(c.node, c.store)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = lock.Close()
		}
	}()
	nonce, err := randomToken()
	if err != nil {
		return err
	}
	grant, err := c.broker.PrepareStartup(ctx, c.node, nonce)
	if err != nil {
		return err
	}
	if grant.Node != c.node || grant.Store != c.store || grant.BootNonce != nonce {
		return ErrChanged
	}
	lease, err := c.broker.ConsumeStartup(ctx, grant)
	if err != nil {
		return err
	}
	if lease.Node != c.node || lease.Store != c.store || lease.Generation != grant.Generation {
		return ErrChanged
	}
	parent, child, err := privatePair()
	if err != nil {
		return err
	}
	defer func() { _ = child.Close() }()
	contained, err := startContainedChild(ctx, command, child, lock)
	if err != nil {
		_ = parent.Close()
		return err
	}
	epoch, err := randomToken()
	if err != nil {
		_ = contained.kill()
		_ = parent.Close()
		return err
	}
	c.lease, c.epoch, c.child = lease, RuntimeEpoch(epoch), contained
	c.runCtx = ctx
	c.lockFile = lock
	success = true
	c.serveDone = make(chan struct{})
	go c.serveChild(parent)
	return nil
}

// StartRecovery uses a distinct one-use grant and a child network namespace.
// The launcher still reaches the broker; the recovering child cannot use an
// accidental listener to turn recovery permission into network permission.
func (c *Controller) StartRecovery(ctx context.Context, command ChildCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := checkContainment(command); err != nil {
		return err
	}
	if c.child != nil {
		return ErrChanged
	}
	lock, err := lockBinding(c.node, c.store)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = lock.Close()
		}
	}()
	nonce, err := randomToken()
	if err != nil {
		return err
	}
	grant, err := c.broker.PrepareRecovery(ctx, c.node, nonce)
	if err != nil {
		return err
	}
	if grant.Node != c.node || grant.Store != c.store || grant.BootNonce != nonce {
		return ErrChanged
	}
	permit, err := c.broker.ConsumeRecovery(ctx, grant)
	if err != nil {
		return err
	}
	if permit.Binding.Node != c.node || permit.Binding.Store != c.store {
		return ErrChanged
	}
	parent, child, err := privatePair()
	if err != nil {
		return err
	}
	defer func() { _ = child.Close() }()
	command.isolated = true
	contained, err := startContainedChild(ctx, command, child, lock)
	if err != nil {
		_ = parent.Close()
		return err
	}
	c.child, c.fenced, c.recovery, c.runCtx = contained, true, &permit, ctx
	c.epoch = permit.Binding.RuntimeEpoch
	c.lockFile, c.recoveryGrant = lock, &grant
	success = true
	c.serveDone = make(chan struct{})
	go c.serveChild(parent)
	return nil
}

func (c *Controller) Wait() error {
	c.mu.Lock()
	child := c.child
	serveDone := c.serveDone
	c.mu.Unlock()
	if child == nil {
		return ErrChanged
	}
	<-child.done
	<-serveDone
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() {
		if c.lockFile != nil {
			_ = c.lockFile.Close()
			c.lockFile = nil
		}
	}()
	if child.err == nil && !c.gracefullyClosed {
		return ErrUnavailable
	}
	// Parent shutdown cancellation must not abandon the final receipt AFTER
	// the exact graceful handshake and actual exit zero have been established.
	completion, finish := context.WithTimeout(context.WithoutCancel(c.runCtx), 2*time.Second)
	defer finish()
	if child.err == nil && c.recoveryGrant != nil {
		if c.recoveryReceipt == nil {
			return ErrReceiptRequired
		}
		if err := c.broker.CompleteRecovery(completion, *c.recoveryGrant, *c.recoveryReceipt); err != nil {
			return err
		}
	}
	if child.err == nil && c.recoveryGrant == nil && c.fenced {
		if c.acknowledgedReceipt == nil {
			return ErrReceiptRequired
		}
		if err := c.broker.SealWriter(completion, *c.acknowledgedReceipt); err != nil {
			return err
		}
	}
	return child.err
}
func (c *Controller) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.child == nil || c.child.exited() {
		return nil
	}
	return c.child.kill()
}

func (c *Controller) BeginRemoval(ctx context.Context, b Binding) (RemovalPermit, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.child == nil || c.fenced || c.child.exited() {
		return RemovalPermit{}, ErrFenced
	}
	if b.Node != c.node || b.Store != c.store || b.RuntimeEpoch != c.epoch || b.Generation != c.lease.Generation {
		return RemovalPermit{}, ErrChanged
	}
	permit, err := c.broker.begin(ctx, c.lease, b)
	// Even a lost broker reply can mean Fenced committed. Contain the process;
	// never let an unknown outcome advance into a local destructive write.
	if err != nil {
		c.fenced = true
		_ = c.child.kill()
		return RemovalPermit{}, err
	}
	c.fenced = true
	return permit, nil
}

func (c *Controller) RecordReceipt(ctx context.Context, r Receipt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.child == nil || c.child.exited() || !c.fenced || r.Permit.Binding.RuntimeEpoch != c.epoch || r.Permit.Binding.Node != c.node || r.Permit.Binding.Store != c.store {
		return ErrChanged
	}
	if c.recovery != nil && (r.Kind != ReceiptProtectionRestored || !samePermit(*c.recovery, r.Permit)) {
		return ErrChanged
	}
	var err error
	if c.recoveryGrant != nil {
		err = c.broker.RecordRecoveryReceipt(ctx, *c.recoveryGrant, r)
	} else {
		err = c.broker.RecordReceipt(ctx, r)
	}
	if err != nil {
		return err
	}
	copied := r
	c.acknowledgedReceipt = &copied
	if c.recovery != nil {
		copied := r
		c.recoveryReceipt = &copied
	}
	return nil
}

type sessionHello struct {
	Version    int                 `json:"version"`
	Node       domain.PeerIdentity `json:"node"`
	Store      StoreID             `json:"store"`
	Epoch      RuntimeEpoch        `json:"epoch"`
	Generation Generation          `json:"generation"`
	Recovery   *RemovalPermit      `json:"recovery,omitempty"`
}

const gracefulExitTimeout = 2 * time.Second

type sessionClose struct {
	Receipt         *Receipt        `json:"receipt,omitempty"`
	Acknowledgement CredentialToken `json:"acknowledgement,omitempty"`
}

type sessionRequest struct {
	Close   *sessionClose `json:"close,omitempty"`
	Binding *Binding      `json:"binding,omitempty"`
	Receipt *Receipt      `json:"receipt,omitempty"`
}
type sessionResponse struct {
	Error           string          `json:"error,omitempty"`
	Permit          *RemovalPermit  `json:"permit,omitempty"`
	Acknowledgement CredentialToken `json:"acknowledgement,omitempty"`
}

func (c *Controller) serveChild(file *os.File) {
	defer close(c.serveDone)
	defer func() { _ = file.Close() }()
	c.mu.Lock()
	hello := sessionHello{Version: 1, Node: c.node, Store: c.store, Epoch: c.epoch, Generation: c.lease.Generation, Recovery: c.recovery}
	if c.recovery != nil {
		hello.Generation = c.recovery.Generation
	}
	runCtx := c.runCtx
	c.mu.Unlock()
	if err := encodeLocalFrame(file, hello); err != nil {
		_ = c.Stop()
		return
	}
	reader := bufio.NewReaderSize(file, 16<<10)
	for {
		var req sessionRequest
		if err := readLocalFrame(reader, &req); err != nil {
			_ = c.Stop()
			return
		}
		result := sessionResponse{}
		var err error
		switch {
		case req.Binding != nil && req.Receipt == nil && req.Close == nil:
			var permit RemovalPermit
			permit, err = c.BeginRemoval(runCtx, *req.Binding)
			if err == nil {
				result.Permit = &permit
			}
		case req.Receipt != nil && req.Binding == nil && req.Close == nil:
			err = c.RecordReceipt(runCtx, *req.Receipt)
			if err == nil {
				result.Acknowledgement, err = c.newReceiptAcknowledgement()
			}
		case req.Close != nil && req.Binding == nil && req.Receipt == nil:
			err = c.validateGracefulClose(req.Close)
		default:
			err = ErrInvalid
		}
		if err != nil {
			result.Error = protocolError(err).Error()
		}
		if err := encodeLocalFrame(file, result); err != nil {
			_ = c.Stop()
			return
		}
		if req.Close != nil {
			if err != nil {
				_ = c.Stop()
				return
			}
			c.awaitGracefulExit()
			return
		}
	}
}

func (c *Controller) newReceiptAcknowledgement() (CredentialToken, error) {
	token, err := randomToken()
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.receiptAcknowledgement = CredentialToken(token)
	return c.receiptAcknowledgement, nil
}

func (c *Controller) validateGracefulClose(request *sessionClose) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fenced {
		if request.Receipt == nil || c.acknowledgedReceipt == nil || !sameReceipt(*request.Receipt, *c.acknowledgedReceipt) || request.Acknowledgement == "" || request.Acknowledgement != c.receiptAcknowledgement {
			return ErrReceiptRequired
		}
	} else if request.Receipt != nil || request.Acknowledgement != "" {
		return ErrChanged
	}
	return nil
}

func (c *Controller) awaitGracefulExit() {
	c.mu.Lock()
	child := c.child
	c.mu.Unlock()
	timer := time.NewTimer(gracefulExitTimeout)
	defer timer.Stop()
	select {
	case <-child.done:
		c.mu.Lock()
		c.gracefullyClosed = true
		c.mu.Unlock()
	case <-timer.C:
		_ = c.Stop()
	}
}

// SupervisedSession is the concrete capability the core may require. Its
// constructor checks the private channel's root parent and approved path;
// merely injecting a RemovalFence or an HTTP client cannot produce this value.
// Physical fault-domain independence remains the administrator's attestation.
type SupervisedSession struct {
	mu                     sync.Mutex
	file                   *os.File
	hello                  sessionHello
	encoder                *json.Encoder
	reader                 *bufio.Reader
	verified               bool
	closed                 bool
	closeErr               error
	acknowledgedReceipt    *Receipt
	receiptAcknowledgement CredentialToken
}

func ConnectSupervisor(file *os.File, expectedLauncher string, node domain.PeerIdentity, store StoreID) (*SupervisedSession, error) {
	if err := verifySupervisor(file, expectedLauncher, node); err != nil {
		return nil, err
	}
	if err := file.SetDeadline(time.Now().Add(privateRequestTimeout)); err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, 16<<10)
	var hello sessionHello
	if err := readLocalFrame(reader, &hello); err != nil {
		return nil, fmt.Errorf("supervisor handshake: %w", err)
	}
	if hello.Version != 1 || hello.Node != node || hello.Store != store || hello.Epoch == "" || hello.Generation == 0 {
		return nil, ErrChanged
	}
	return &SupervisedSession{file: file, hello: hello, encoder: json.NewEncoder(file), reader: reader, verified: true}, nil
}

// NodeStore exposes only the immutable supervised scope, without inventing a proof.
func (s *SupervisedSession) NodeStore() (domain.PeerIdentity, StoreID, error) {
	if s == nil {
		return domain.PeerIdentity{}, "", ErrUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.verified || s.closed {
		return domain.PeerIdentity{}, "", ErrUnauthorized
	}
	return s.hello.Node, s.hello.Store, nil
}

func (s *SupervisedSession) Binding(identity domain.PeerIdentity, revision PinRevision, pinnedAt time.Time, metadata domain.IdentityPinMetadata) (Binding, error) {
	backup, err := domain.NewIdentityPinBackup(identity, pinnedAt, metadata)
	if err != nil {
		return Binding{}, err
	}
	return Binding{Node: s.hello.Node, Store: s.hello.Store, RuntimeEpoch: s.hello.Epoch, Generation: s.hello.Generation, Identity: identity, PinRevision: revision, PreviousPinnedAt: pinnedAt, Backup: backup}, nil
}

func (s *SupervisedSession) RecoveryPermit() (RemovalPermit, bool) {
	if s.hello.Recovery == nil {
		return RemovalPermit{}, false
	}
	return *s.hello.Recovery, true
}
func (s *SupervisedSession) Verified() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verified && !s.closed
}

func (s *SupervisedSession) BeginRemoval(ctx context.Context, b Binding) (RemovalPermit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.verified || s.closed {
		return RemovalPermit{}, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return RemovalPermit{}, err
	}
	result, err := s.roundTripLocked(ctx, sessionRequest{Binding: &b})
	if err != nil {
		return RemovalPermit{}, err
	}
	if result.Error != "" {
		return RemovalPermit{}, fmt.Errorf("%w: %s", ErrUnavailable, result.Error)
	}
	if result.Permit == nil || !sameBinding(result.Permit.Binding, b) {
		return RemovalPermit{}, ErrChanged
	}
	return *result.Permit, nil
}
func (s *SupervisedSession) RecordReceipt(ctx context.Context, r Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.verified || s.closed {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := s.roundTripLocked(ctx, sessionRequest{Receipt: &r})
	if err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf("%w: %s", ErrUnavailable, result.Error)
	}
	if result.Acknowledgement == "" {
		return ErrUnavailable
	}
	copied := r
	s.acknowledgedReceipt = &copied
	s.receiptAcknowledgement = result.Acknowledgement
	return nil
}

const privateRequestTimeout = 5 * time.Second

// A partial or unanswered request poisons framing. Keep the broker fenced and
// refuse reuse; local protective rollback can still finish before process exit.
func (s *SupervisedSession) roundTripLocked(ctx context.Context, request sessionRequest) (sessionResponse, error) {
	deadline := time.Now().Add(privateRequestTimeout)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	if err := s.file.SetDeadline(deadline); err != nil {
		s.verified = false
		return sessionResponse{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = s.file.SetDeadline(time.Now()); close(interrupted) })
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	if err := s.encoder.Encode(request); err != nil {
		s.verified = false
		return sessionResponse{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var result sessionResponse
	if err := readLocalFrame(s.reader, &result); err != nil {
		s.verified = false
		return sessionResponse{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return result, nil
}

// Close performs the final bounded shutdown handshake. Call only after Run's
// teardown and final persistence (or offline recovery) have returned. No writes
// or Run may follow it; exit immediately with the original outcome, nonzero on
// fatal storage or any Close error. FD4 remains owned until kernel process exit.
func (s *SupervisedSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.closeGracefully()
	if err := s.file.Close(); s.closeErr == nil {
		s.closeErr = err
	}
	return s.closeErr
}

func (s *SupervisedSession) closeGracefully() error {
	if !s.verified {
		return ErrUnavailable
	}
	if err := s.file.SetDeadline(time.Now().Add(gracefulExitTimeout)); err != nil {
		return err
	}
	if err := s.encoder.Encode(sessionRequest{Close: &sessionClose{Receipt: s.acknowledgedReceipt, Acknowledgement: s.receiptAcknowledgement}}); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var result sessionResponse
	if err := readLocalFrame(s.reader, &result); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if result.Error != "" || result.Permit != nil {
		return ErrUnavailable
	}
	return nil
}

func readLocalFrame(reader *bufio.Reader, dst any) error {
	raw, err := reader.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("local frame: %w", err)
	}
	if len(raw) > 16<<10 {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrInvalid
	}
	return nil
}

func encodeLocalFrame(file *os.File, value any) error {
	if err := file.SetWriteDeadline(time.Now().Add(gracefulExitTimeout)); err != nil {
		return err
	}
	return json.NewEncoder(file).Encode(value)
}
