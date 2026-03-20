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
	default:
		return ErrProtocol
	}
}
