package domain

import "testing"

func TestMessageIDIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		id    MessageID
		valid bool
	}{
		// Valid v4 UUIDs: version=4, variant=8/9/a/b.
		{"valid v4 variant 8", MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"), true},
		{"valid v4 variant 9", MessageID("a1b2c3d4-e5f6-4a7b-9c9d-e0f1a2b3c4d5"), true},
		{"valid v4 variant a", MessageID("a1b2c3d4-e5f6-4a7b-ac9d-e0f1a2b3c4d5"), true},
		{"valid v4 variant b", MessageID("a1b2c3d4-e5f6-4a7b-bc9d-e0f1a2b3c4d5"), true},
		{"valid v4 min nonzero", MessageID("00000001-0000-4000-8000-000000000001"), true},

		// Invalid: wrong version nibble.
		{"all zeros not v4", MessageID("00000000-0000-0000-0000-000000000000"), false},
		{"all f's not v4", MessageID("ffffffff-ffff-ffff-ffff-ffffffffffff"), false},
		{"uuid v1 shape", MessageID("a1b2c3d4-e5f6-1a7b-8c9d-e0f1a2b3c4d5"), false},
		{"uuid v5 shape", MessageID("a1b2c3d4-e5f6-5a7b-8c9d-e0f1a2b3c4d5"), false},
		{"uuid v7 shape", MessageID("a1b2c3d4-e5f6-7a7b-8c9d-e0f1a2b3c4d5"), false},

		// Invalid: wrong variant nibble (must be 8/9/a/b).
		{"variant 0", MessageID("a1b2c3d4-e5f6-4a7b-0c9d-e0f1a2b3c4d5"), false},
		{"variant c", MessageID("a1b2c3d4-e5f6-4a7b-cc9d-e0f1a2b3c4d5"), false},
		{"variant f", MessageID("a1b2c3d4-e5f6-4a7b-fc9d-e0f1a2b3c4d5"), false},

		// Invalid: structural.
		{"empty string", MessageID(""), false},
		{"too short", MessageID("a1b2c3d4"), false},
		{"too long", MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5-extra"), false},
		{"uppercase hex", MessageID("A1B2C3D4-E5F6-4A7B-8C9D-E0F1A2B3C4D5"), false},
		{"no dashes", MessageID("a1b2c3d4e5f64a7b8c9de0f1a2b3c4d5"), false},
		{"special chars", MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4g5"), false},
		{"spaces", MessageID("a1b2c3d4 e5f6 4a7b 8c9d e0f1a2b3c4d5"), false},

		// Invalid: injection and garbage.
		{"json injection", MessageID(`","malicious":"true`), false},
		{"megabyte garbage", MessageID(string(make([]byte, 1<<20))), false},
		{"boolean true", MessageID("true"), false},
		{"null string", MessageID("null"), false},
		{"number 123", MessageID("123"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.id.IsValid()
			if got != tt.valid {
				t.Errorf("MessageID(%q).IsValid() = %v, want %v", tt.id, got, tt.valid)
			}
		})
	}
}

func TestMessageIDIsValidOrEmpty(t *testing.T) {
	t.Parallel()

	// Empty is valid for optional fields.
	if !MessageID("").IsValidOrEmpty() {
		t.Error("empty MessageID should be valid-or-empty")
	}

	// Valid UUID is valid-or-empty.
	if !MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5").IsValidOrEmpty() {
		t.Error("valid UUID should be valid-or-empty")
	}

	// Invalid format is not valid-or-empty.
	if MessageID("garbage").IsValidOrEmpty() {
		t.Error("garbage should not be valid-or-empty")
	}
}
