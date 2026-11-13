package identity

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStoredRecordSeqFloorOnlyReadsMatchingBoundedIdentity(t *testing.T) {
	t.Parallel()
	owner, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	other, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(storedIdentity{
		PrivateKey: base64.StdEncoding.EncodeToString(owner.PrivateKey), RecordSeqFloor: 41,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name  string
		body  []byte
		owner *Identity
		want  uint64
	}{
		{name: "legacy-read-only", body: legacy, owner: owner, want: 41},
		{name: "different-key", body: legacy, owner: other},
		{name: "oversize", body: append(bytes.Clone(legacy), bytes.Repeat([]byte(" "), 4096)...), owner: owner},
		{name: "malformed", body: []byte("{"), owner: owner},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "identity.json")
			if err := os.WriteFile(path, scenario.body, 0o600); err != nil {
				t.Fatal(err)
			}
			if got := StoredRecordSeqFloor(path, scenario.owner); got != scenario.want {
				t.Fatalf("read floor %d, want %d", got, scenario.want)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, scenario.body) {
				t.Fatal("record floor read rewrote the identity file")
			}
		})
	}
}
