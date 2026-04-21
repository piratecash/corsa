package protocol

import "errors"

const (
	ErrCodeProtocol                   = "protocol-error"
	ErrCodeInvalidJSON                = "invalid-json"
	ErrCodeEncodeFailed               = "encode-failed"
	ErrCodeUnknownCommand             = "unknown-command"
	ErrCodeInvalidSendMessage         = "invalid-send-message"
	ErrCodeInvalidImportMessage       = "invalid-import-message"
	ErrCodeInvalidFetchMessages       = "invalid-fetch-messages"
	ErrCodeInvalidFetchMessageIDs     = "invalid-fetch-message-ids"
	ErrCodeInvalidFetchMessage        = "invalid-fetch-message"
	ErrCodeInvalidFetchInbox          = "invalid-fetch-inbox"
	ErrCodeInvalidSendDeliveryReceipt = "invalid-send-delivery-receipt"
	ErrCodeInvalidFetchReceipts       = "invalid-fetch-delivery-receipts"
	ErrCodeInvalidSubscribeInbox      = "invalid-subscribe-inbox"
	ErrCodeInvalidPublishNotice       = "invalid-publish-notice"
	ErrCodeUnknownSenderKey           = "unknown-sender-key"
	ErrCodeUnknownMessageID           = "unknown-message-id"
	ErrCodeInvalidDirectMessageSig    = "invalid-direct-message-signature"
	ErrCodeMessageTimestampOutOfRange = "message-timestamp-out-of-range"
	ErrCodeIncompatibleProtocol       = "incompatible-protocol-version"
	ErrCodeRead                       = "read"
	ErrCodeAuthRequired               = "auth-required"
	ErrCodeInvalidAuthSignature       = "invalid-auth-signature"
	ErrCodeBlacklisted                = "blacklisted"
	ErrCodeInvalidAckDelete           = "invalid-ack-delete"
	ErrCodeFrameTooLarge              = "frame-too-large"
	ErrCodeDuplicateConnection        = "duplicate-connection"
	ErrCodeRateLimited                = "rate-limited"
	ErrCodeHelloAfterAuth             = "hello-after-auth"
	ErrCodeSenderIdentityNotVerified  = "sender-identity-not-verified"
	// ErrCodeObservedAddressMismatch signals that the advertised listen
	// address in a peer's hello frame does not match the observed remote
	// TCP endpoint. Used only inside type="connection_notice" frames with
	// machine-readable details.observed_address. The responder closes the
	// connection immediately after sending the notice — welcome is not
	// issued, auth_session is not expected.
	ErrCodeObservedAddressMismatch = "observed-address-mismatch"

	// ErrCodePeerBanned is returned by the responder when the inbound peer
	// is on the local ban list (IP-wide blacklist or per-peer timed ban).
	// Used only inside type="connection_notice" frames; Details carries a
	// machine-readable expiration so the dialler can mirror the ban and
	// stop redialling until it lifts, instead of treating the rejection
	// as a generic handshake failure and hammering the bouncer. The
	// responder closes the connection immediately after the notice.
	ErrCodePeerBanned = "peer-banned"

	// ErrCodeSelfIdentity is used when the counterpart of the handshake
	// advertises the local node's own Ed25519 identity. Raised on both
	// directions — on the inbound path when a remote hello carries our
	// identity, and on the outbound path when a welcome does — to break
	// accidental self-loopback through NAT reflection, peer-exchange
	// mirrors, onion aliases or multi-homed addresses. The responder
	// emits type="connection_notice" with details.reason="self-identity"
	// and closes the connection; the dialler records a long cooldown on
	// the address so it stops hammering its own endpoint.
	ErrCodeSelfIdentity = "self-identity"
)

var (
	ErrProtocol                   = errors.New(ErrCodeProtocol)
	ErrInvalidJSON                = errors.New(ErrCodeInvalidJSON)
	ErrEncodeFailed               = errors.New(ErrCodeEncodeFailed)
	ErrUnknownCommand             = errors.New(ErrCodeUnknownCommand)
	ErrInvalidSendMessage         = errors.New(ErrCodeInvalidSendMessage)
	ErrInvalidImportMessage       = errors.New(ErrCodeInvalidImportMessage)
	ErrInvalidFetchMessages       = errors.New(ErrCodeInvalidFetchMessages)
	ErrInvalidFetchMessageIDs     = errors.New(ErrCodeInvalidFetchMessageIDs)
	ErrInvalidFetchMessage        = errors.New(ErrCodeInvalidFetchMessage)
	ErrInvalidFetchInbox          = errors.New(ErrCodeInvalidFetchInbox)
	ErrInvalidSendDeliveryReceipt = errors.New(ErrCodeInvalidSendDeliveryReceipt)
	ErrInvalidFetchReceipts       = errors.New(ErrCodeInvalidFetchReceipts)
	ErrInvalidSubscribeInbox      = errors.New(ErrCodeInvalidSubscribeInbox)
	ErrInvalidPublishNotice       = errors.New(ErrCodeInvalidPublishNotice)
	ErrUnknownSenderKey           = errors.New(ErrCodeUnknownSenderKey)
	ErrUnknownMessageID           = errors.New(ErrCodeUnknownMessageID)
	ErrInvalidDirectMessageSig    = errors.New(ErrCodeInvalidDirectMessageSig)
	ErrMessageTimestampOutOfRange = errors.New(ErrCodeMessageTimestampOutOfRange)
	ErrIncompatibleProtocol       = errors.New(ErrCodeIncompatibleProtocol)
	ErrRead                       = errors.New(ErrCodeRead)
	ErrAuthRequired               = errors.New(ErrCodeAuthRequired)
	ErrInvalidAuthSignature       = errors.New(ErrCodeInvalidAuthSignature)
	ErrBlacklisted                = errors.New(ErrCodeBlacklisted)
	ErrInvalidAckDelete           = errors.New(ErrCodeInvalidAckDelete)
	ErrFrameTooLarge              = errors.New(ErrCodeFrameTooLarge)
	ErrDuplicateConnection        = errors.New(ErrCodeDuplicateConnection)
	ErrRateLimited                = errors.New(ErrCodeRateLimited)
	ErrHelloAfterAuth             = errors.New(ErrCodeHelloAfterAuth)
	ErrSenderIdentityNotVerified  = errors.New(ErrCodeSenderIdentityNotVerified)
	// ErrObservedAddressMismatch is the sentinel for ErrCodeObservedAddressMismatch.
	// Callers detect a pre-welcome advertise-address rejection via errors.Is.
	ErrObservedAddressMismatch = errors.New(ErrCodeObservedAddressMismatch)
	// ErrPeerBanned is the sentinel for ErrCodePeerBanned. Callers use
	// errors.Is to distinguish a ban-rejection from other pre-welcome
	// failures so that the dialler can honour the carried expiration
	// instead of applying the generic incompatible-version penalty flow.
	ErrPeerBanned = errors.New(ErrCodePeerBanned)
	// ErrSelfIdentity is the sentinel for ErrCodeSelfIdentity. The
	// dialler distinguishes self-identity rejection from other
	// handshake failures via errors.Is so that it can apply a long
	// cooldown on the address (no redial churn) instead of treating
	// the rejection as a generic retryable failure.
	ErrSelfIdentity = errors.New(ErrCodeSelfIdentity)
)

func ErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidJSON):
		return ErrCodeInvalidJSON
	case errors.Is(err, ErrEncodeFailed):
		return ErrCodeEncodeFailed
	case errors.Is(err, ErrUnknownCommand):
		return ErrCodeUnknownCommand
	case errors.Is(err, ErrInvalidSendMessage):
		return ErrCodeInvalidSendMessage
	case errors.Is(err, ErrInvalidImportMessage):
		return ErrCodeInvalidImportMessage
	case errors.Is(err, ErrInvalidFetchMessages):
		return ErrCodeInvalidFetchMessages
	case errors.Is(err, ErrInvalidFetchMessageIDs):
		return ErrCodeInvalidFetchMessageIDs
	case errors.Is(err, ErrInvalidFetchMessage):
		return ErrCodeInvalidFetchMessage
	case errors.Is(err, ErrInvalidFetchInbox):
		return ErrCodeInvalidFetchInbox
	case errors.Is(err, ErrInvalidSendDeliveryReceipt):
		return ErrCodeInvalidSendDeliveryReceipt
	case errors.Is(err, ErrInvalidFetchReceipts):
		return ErrCodeInvalidFetchReceipts
	case errors.Is(err, ErrInvalidSubscribeInbox):
		return ErrCodeInvalidSubscribeInbox
	case errors.Is(err, ErrInvalidPublishNotice):
		return ErrCodeInvalidPublishNotice
	case errors.Is(err, ErrUnknownSenderKey):
		return ErrCodeUnknownSenderKey
	case errors.Is(err, ErrUnknownMessageID):
		return ErrCodeUnknownMessageID
	case errors.Is(err, ErrInvalidDirectMessageSig):
		return ErrCodeInvalidDirectMessageSig
	case errors.Is(err, ErrMessageTimestampOutOfRange):
		return ErrCodeMessageTimestampOutOfRange
	case errors.Is(err, ErrIncompatibleProtocol):
		return ErrCodeIncompatibleProtocol
	case errors.Is(err, ErrRead):
		return ErrCodeRead
	case errors.Is(err, ErrAuthRequired):
		return ErrCodeAuthRequired
	case errors.Is(err, ErrInvalidAuthSignature):
		return ErrCodeInvalidAuthSignature
	case errors.Is(err, ErrBlacklisted):
		return ErrCodeBlacklisted
	case errors.Is(err, ErrInvalidAckDelete):
		return ErrCodeInvalidAckDelete
	case errors.Is(err, ErrFrameTooLarge):
		return ErrCodeFrameTooLarge
	case errors.Is(err, ErrDuplicateConnection):
		return ErrCodeDuplicateConnection
	case errors.Is(err, ErrRateLimited):
		return ErrCodeRateLimited
	case errors.Is(err, ErrHelloAfterAuth):
		return ErrCodeHelloAfterAuth
	case errors.Is(err, ErrSenderIdentityNotVerified):
		return ErrCodeSenderIdentityNotVerified
	case errors.Is(err, ErrObservedAddressMismatch):
		return ErrCodeObservedAddressMismatch
	case errors.Is(err, ErrPeerBanned):
		return ErrCodePeerBanned
	case errors.Is(err, ErrSelfIdentity):
		return ErrCodeSelfIdentity
	default:
		return ErrCodeProtocol
	}
}

func ErrorFromCode(code string) error {
	switch code {
	case ErrCodeInvalidJSON:
		return ErrInvalidJSON
	case ErrCodeEncodeFailed:
		return ErrEncodeFailed
	case ErrCodeUnknownCommand:
		return ErrUnknownCommand
	case ErrCodeInvalidSendMessage:
		return ErrInvalidSendMessage
	case ErrCodeInvalidImportMessage:
		return ErrInvalidImportMessage
	case ErrCodeInvalidFetchMessages:
		return ErrInvalidFetchMessages
	case ErrCodeInvalidFetchMessageIDs:
		return ErrInvalidFetchMessageIDs
	case ErrCodeInvalidFetchMessage:
		return ErrInvalidFetchMessage
	case ErrCodeInvalidFetchInbox:
		return ErrInvalidFetchInbox
	case ErrCodeInvalidSendDeliveryReceipt:
		return ErrInvalidSendDeliveryReceipt
	case ErrCodeInvalidFetchReceipts:
		return ErrInvalidFetchReceipts
	case ErrCodeInvalidSubscribeInbox:
		return ErrInvalidSubscribeInbox
	case ErrCodeInvalidPublishNotice:
		return ErrInvalidPublishNotice
	case ErrCodeUnknownSenderKey:
		return ErrUnknownSenderKey
	case ErrCodeUnknownMessageID:
		return ErrUnknownMessageID
	case ErrCodeInvalidDirectMessageSig:
		return ErrInvalidDirectMessageSig
	case ErrCodeMessageTimestampOutOfRange:
		return ErrMessageTimestampOutOfRange
	case ErrCodeIncompatibleProtocol:
		return ErrIncompatibleProtocol
	case ErrCodeRead:
		return ErrRead
	case ErrCodeAuthRequired:
		return ErrAuthRequired
	case ErrCodeInvalidAuthSignature:
		return ErrInvalidAuthSignature
	case ErrCodeBlacklisted:
		return ErrBlacklisted
	case ErrCodeInvalidAckDelete:
		return ErrInvalidAckDelete
	case ErrCodeFrameTooLarge:
		return ErrFrameTooLarge
	case ErrCodeDuplicateConnection:
		return ErrDuplicateConnection
	case ErrCodeRateLimited:
		return ErrRateLimited
	case ErrCodeHelloAfterAuth:
		return ErrHelloAfterAuth
	case ErrCodeSenderIdentityNotVerified:
		return ErrSenderIdentityNotVerified
	case ErrCodeObservedAddressMismatch:
		return ErrObservedAddressMismatch
	case ErrCodePeerBanned:
		return ErrPeerBanned
	case ErrCodeSelfIdentity:
		return ErrSelfIdentity
	default:
		return ErrProtocol
	}
}

// NoticeErrorFromFrame maps a connection_notice frame to its sentinel
// error, taking details.reason into account so callers can discriminate
// sub-kinds of the same Code. This is essential for `peer-banned`: the
// notice carries different reasons (`peer-ban`, `blacklisted`,
// `self-identity`) that demand different caller reactions — a
// self-identity notice MUST route through the 24h self-loopback
// cooldown, while a generic peer-ban triggers the advertise-mismatch
// cooldown path. A plain ErrorFromCode(code) lookup collapses all
// reasons into ErrPeerBanned and strips the discrimination signal, so
// the dialler would churn against a self-looping endpoint instead of
// suppressing it for the full window. Non-peer-banned codes fall
// through to ErrorFromCode unchanged.
//
// Details parse errors degrade to the generic sentinel — a malformed
// details blob is a wire protocol violation, but the outer code is
// still trustworthy so the caller at least learns "peer-banned".
func NoticeErrorFromFrame(frame Frame) error {
	if frame.Code != ErrCodePeerBanned {
		return ErrorFromCode(frame.Code)
	}
	details, _, err := ParsePeerBannedDetails(frame.Details)
	if err != nil {
		return ErrPeerBanned
	}
	switch details.Reason {
	case PeerBannedReasonSelfIdentity:
		return ErrSelfIdentity
	default:
		return ErrPeerBanned
	}
}
