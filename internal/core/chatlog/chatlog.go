// Package chatlog provides append-only, file-backed storage for chat messages.
//
// Each conversation partner gets its own file in the .corsa directory:
//
//	dm-<identity_short>-<peer_address>-<port>.jsonl   (DMs with a peer)
//	global-<identity_short>-<port>.jsonl              (broadcast / public messages)
//
// Files use JSON Lines format — one JSON object per line, appended atomically.
// Messages are stored as-is: incoming DM bodies are already sealed envelopes,
// and outgoing DMs are encrypted with the sender's own key before storage.
// This means reading a chatlog always requires decryption via the identity key.
//
// The chatlog is never held entirely in memory. When the desktop client switches
// to a conversation, it reads the file on demand and then receives new messages
// via the normal subscription mechanism.
package chatlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Entry is a single chatlog record persisted as one JSON line.
type Entry struct {
	ID        string `json:"id"`
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
	Flag      string `json:"flag,omitempty"`
}

// Store manages chatlog files for a single node identity.
type Store struct {
	dir           string // directory for chatlog files (typically .corsa/)
	identityShort string // first 8 chars of identity address
	port          string // listen port suffix
	mu            sync.Mutex
}

// NewStore creates a chatlog store.
//   - dir:           base directory for chatlog files (e.g. ".corsa")
//   - identityAddr:  full 40-char hex identity address
//   - listenAddress: node listen address (e.g. ":64646") — port is extracted
func NewStore(dir string, identityAddr string, listenAddress string) *Store {
	short := identityAddr
	if len(short) > 8 {
		short = short[:8]
	}

	port := portSuffix(listenAddress)

	return &Store{
		dir:           dir,
		identityShort: short,
		port:          port,
	}
}

// Append adds a message entry to the appropriate chatlog file.
// For DMs the peer is the other party (sender or recipient that is not self).
// For global/broadcast messages use topic "global".
func (s *Store) Append(topic string, selfAddress string, entry Entry) error {
	path := s.filePath(topic, selfAddress, entry.Sender, entry.Recipient)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure the chatlog directory exists. This is important when a custom
	// CORSA_CHATLOG_DIR is configured — the directory may not yet exist.
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("chatlog: mkdir %s: %w", filepath.Dir(path), err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("chatlog: open %s: %w", path, err)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("chatlog: marshal entry: %w", err)
	}
	data = append(data, '\n')

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("chatlog: write %s: %w", path, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("chatlog: close %s: %w", path, err)
	}

	return nil
}

// Read returns all entries from the chatlog file for a given conversation.
// peerAddress is the conversation partner's identity address (40-char hex).
// For global messages pass topic="global" and peerAddress="".
func (s *Store) Read(topic string, peerAddress string) ([]Entry, error) {
	path := s.peerFilePath(topic, peerAddress)

	s.mu.Lock()
	defer s.mu.Unlock()

	return readEntriesFromFile(path)
}

// ReadLast returns the last n entries from a chatlog file.
// If the file has fewer than n entries, all entries are returned.
func (s *Store) ReadLast(topic string, peerAddress string, n int) ([]Entry, error) {
	entries, err := s.Read(topic, peerAddress)
	if err != nil {
		return nil, err
	}
	if len(entries) <= n {
		return entries, nil
	}
	return entries[len(entries)-n:], nil
}

// Conversations returns a list of peer addresses that have chatlog files,
// along with the timestamp of the last message in each.
type ConversationSummary struct {
	PeerAddress string    `json:"peer_address"`
	LastMessage time.Time `json:"last_message"`
	Count       int       `json:"count"`
}

// ListConversations scans the chatlog directory for DM files belonging to
// this identity+port and returns a summary for each.
func (s *Store) ListConversations() ([]ConversationSummary, error) {
	prefix := fmt.Sprintf("dm-%s-", s.identityShort)
	suffix := fmt.Sprintf("-%s.jsonl", s.port)

	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("chatlog: readdir %s: %w", s.dir, err)
	}

	var result []ConversationSummary
	for _, de := range entries {
		name := de.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		// Extract peer address from filename:
		// dm-<identity_short>-<peer_address>-<port>.jsonl
		inner := strings.TrimPrefix(name, prefix)
		inner = strings.TrimSuffix(inner, suffix)
		if inner == "" {
			continue
		}
		peerAddr := inner

		path := filepath.Join(s.dir, name)
		logEntries, err := readEntriesFromFile(path)
		if err != nil {
			continue
		}

		var lastTime time.Time
		if len(logEntries) > 0 {
			last := logEntries[len(logEntries)-1]
			if t, err := time.Parse(time.RFC3339Nano, last.CreatedAt); err == nil {
				lastTime = t
			}
		}

		result = append(result, ConversationSummary{
			PeerAddress: peerAddr,
			LastMessage: lastTime,
			Count:       len(logEntries),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastMessage.After(result[j].LastMessage)
	})

	return result, nil
}

// ReadLastEntry returns the last entry for a single conversation.
// Returns nil if the conversation has no entries.
// ReadLastEntry returns the last entry for a single conversation.
// Reads only the tail of the file — O(1) per entry, not O(N).
// Returns nil if the conversation has no entries.
func (s *Store) ReadLastEntry(topic string, peerAddress string) (*Entry, error) {
	path := s.peerFilePath(topic, peerAddress)

	s.mu.Lock()
	defer s.mu.Unlock()

	return readLastEntryFromFile(path)
}

// ReadLastEntryPerPeer scans all DM chatlog files and returns the last
// entry for each conversation partner. Only the last line of each file
// is read — total work is O(files), not O(total messages).
func (s *Store) ReadLastEntryPerPeer() (map[string]Entry, error) {
	prefix := fmt.Sprintf("dm-%s-", s.identityShort)
	suffix := fmt.Sprintf("-%s.jsonl", s.port)

	s.mu.Lock()
	defer s.mu.Unlock()

	dirEntries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("chatlog: readdir %s: %w", s.dir, err)
	}

	result := make(map[string]Entry)
	for _, de := range dirEntries {
		name := de.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		inner := strings.TrimPrefix(name, prefix)
		inner = strings.TrimSuffix(inner, suffix)
		if inner == "" {
			continue
		}
		peerAddr := inner

		path := filepath.Join(s.dir, name)
		entry, err := readLastEntryFromFile(path)
		if err != nil || entry == nil {
			continue
		}
		result[peerAddr] = *entry
	}

	return result, nil
}

// readLastEntryFromFile reads only the last valid JSON line from a JSONL file
// by seeking from the end of the file. This avoids reading the entire file
// into memory — important for large chat histories.
func readLastEntryFromFile(path string) (*Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("chatlog: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("chatlog: stat %s: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}

	// Read a tail chunk from the end of the file. We read up to tailSize
	// bytes which should be enough for one JSONL entry (sealed envelopes
	// are typically a few KB). If the last line is larger, we fall back
	// to a full scan.
	const tailSize = 64 * 1024 // 64 KB
	offset := size - tailSize
	if offset < 0 {
		offset = 0
	}

	buf := make([]byte, size-offset)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("chatlog: readat %s: %w", path, err)
	}

	// Walk backward through the buffer to find the last non-empty line.
	// Lines are separated by '\n'. We need to find the last complete line.
	data := strings.TrimRight(string(buf), "\n\r ")
	if data == "" {
		return nil, nil
	}

	lastNL := strings.LastIndex(data, "\n")
	var lastLine string
	if lastNL >= 0 {
		lastLine = strings.TrimSpace(data[lastNL+1:])
	} else {
		// Only one line in the chunk (or the entire file is one line).
		if offset == 0 {
			lastLine = strings.TrimSpace(data)
		} else {
			// The tail chunk doesn't contain a full line break and we
			// started mid-file — fall back to full scan for this file.
			return readLastEntryFullScan(path)
		}
	}

	if lastLine == "" {
		return nil, nil
	}

	var e Entry
	if err := json.Unmarshal([]byte(lastLine), &e); err != nil {
		// Last line is malformed; fall back to full scan to find
		// the last valid entry.
		return readLastEntryFullScan(path)
	}
	return &e, nil
}

// readLastEntryFullScan reads the entire file and returns the last valid entry.
// Used as a fallback when tail-read fails (e.g. very long lines or corruption).
func readLastEntryFullScan(path string) (*Entry, error) {
	entries, err := readEntriesFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	last := entries[len(entries)-1]
	return &last, nil
}

// HasEntryID checks if an entry with the given ID already exists in the file.
// This is used to prevent duplicate appends (idempotency).
func (s *Store) HasEntryID(topic string, peerAddress string, id string) bool {
	entries, err := s.Read(topic, peerAddress)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.ID == id {
			return true
		}
	}
	return false
}

// filePath determines the correct file path based on topic and addresses.
func (s *Store) filePath(topic string, selfAddress string, sender string, recipient string) string {
	if topic == "dm" {
		peer := recipient
		if recipient == selfAddress {
			peer = sender
		}
		return s.peerFilePath("dm", peer)
	}
	return s.peerFilePath(topic, "")
}

// peerFilePath returns the path for a given topic and peer.
func (s *Store) peerFilePath(topic string, peerAddress string) string {
	if topic == "dm" && peerAddress != "" {
		name := fmt.Sprintf("dm-%s-%s-%s.jsonl", s.identityShort, peerAddress, s.port)
		return filepath.Join(s.dir, name)
	}
	name := fmt.Sprintf("global-%s-%s.jsonl", s.identityShort, s.port)
	return filepath.Join(s.dir, name)
}

func readEntriesFromFile(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("chatlog: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var entries []Entry
	scanner := bufio.NewScanner(f)
	// Allow lines up to 1MB for large sealed envelopes.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue // skip malformed lines
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return entries, fmt.Errorf("chatlog: scan %s: %w", path, err)
	}

	return entries, nil
}

func portSuffix(listenAddress string) string {
	port := "default"
	if idx := strings.LastIndex(listenAddress, ":"); idx >= 0 && idx < len(listenAddress)-1 {
		port = listenAddress[idx+1:]
	}
	return port
}
