package domain

import "testing"

// TestDecideIdentityRecordMerge pins the five-way seq-merge contract every
// record store relies on.
func TestDecideIdentityRecordMerge(t *testing.T) {
	bodyA := []byte(`{"seq":2,"x":"a"}`)
	bodyB := []byte(`{"seq":2,"x":"b"}`)

	cases := []struct {
		name     string
		stored   StoredIdentityRecordState
		seq      IdentityRecordSeq
		body     []byte
		expected IdentityRecordMergeOutcome
	}{
		{"no stored record inserts", AbsentIdentityRecord(), 1, bodyA, IdentityRecordMergeInserted},
		{"higher seq replaces", ExistingIdentityRecord(2, bodyA), 3, bodyB, IdentityRecordMergeReplaced},
		{"lower seq is stale", ExistingIdentityRecord(2, bodyA), 1, bodyB, IdentityRecordMergeStale},
		{"same seq same bytes is duplicate", ExistingIdentityRecord(2, bodyA), 2, bodyA, IdentityRecordMergeDuplicate},
		{"same seq different bytes is conflict", ExistingIdentityRecord(2, bodyA), 2, bodyB, IdentityRecordMergeConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecideIdentityRecordMerge(tc.stored, tc.seq, tc.body); got != tc.expected {
				t.Errorf("outcome = %s, want %s", got, tc.expected)
			}
		})
	}
}

// TestIdentityRecordMergeAccepted: only insert and replace mutate a store.
func TestIdentityRecordMergeAccepted(t *testing.T) {
	accepted := map[IdentityRecordMergeOutcome]bool{
		IdentityRecordMergeInserted:  true,
		IdentityRecordMergeReplaced:  true,
		IdentityRecordMergeDuplicate: false,
		IdentityRecordMergeStale:     false,
		IdentityRecordMergeConflict:  false,
	}
	for outcome, want := range accepted {
		if outcome.Accepted() != want {
			t.Errorf("%s.Accepted() = %v, want %v", outcome, outcome.Accepted(), want)
		}
	}
}

// TestParseIdentityRecordDTypesField pins the record-local bounds: the cap
// counts wire elements before deduplication, and any breach degrades the
// field to absent instead of rejecting the record.
func TestParseIdentityRecordDTypesField(t *testing.T) {
	t.Run("nil field is absent", func(t *testing.T) {
		if set := ParseIdentityRecordDTypesField(nil); set.Declaration() != DTypeDeclarationAbsent {
			t.Errorf("declaration = %s, want absent", set.Declaration())
		}
	})

	t.Run("cap counts pre-dedup elements", func(t *testing.T) {
		names := make([]string, MaxIdentityRecordDTypes+1)
		for i := range names {
			names[i] = "same_name"
		}
		if set := ParseIdentityRecordDTypesField(&names); set.Declaration() != DTypeDeclarationAbsent {
			t.Errorf("declaration = %s, want absent (9 wire elements)", set.Declaration())
		}
	})

	t.Run("at the cap parses", func(t *testing.T) {
		names := make([]string, MaxIdentityRecordDTypes)
		for i := range names {
			names[i] = "t" + string(rune('a'+i))
		}
		set := ParseIdentityRecordDTypesField(&names)
		if set.Declaration() != DTypeDeclarationExplicit || set.Len() != MaxIdentityRecordDTypes {
			t.Errorf("got %s/%d, want explicit/%d", set.Declaration(), set.Len(), MaxIdentityRecordDTypes)
		}
	})
}
