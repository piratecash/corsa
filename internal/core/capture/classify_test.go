package capture

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestClassifyPayload(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want domain.PayloadKind
	}{
		{
			name: "valid json object",
			raw:  `{"type":"ping"}`,
			want: domain.PayloadKindJSON,
		},
		{
			name: "valid json array",
			raw:  `[1,2,3]`,
			want: domain.PayloadKindJSON,
		},
		{
			name: "invalid json starting with brace",
			raw:  `{"broken":`,
			want: domain.PayloadKindInvalidJSON,
		},
		{
			name: "invalid json starting with bracket",
			raw:  `[1,2,`,
			want: domain.PayloadKindInvalidJSON,
		},
		{
			name: "non json text",
			raw:  "hello world",
			want: domain.PayloadKindNonJSON,
		},
		{
			name: "empty string",
			raw:  "",
			want: domain.PayloadKindMalformed,
		},
		{
			name: "plain number",
			raw:  "42",
			want: domain.PayloadKindNonJSON,
		},
		{
			name: "nested valid json",
			raw:  `{"a":{"b":true},"c":[1,2]}`,
			want: domain.PayloadKindJSON,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyPayload(tt.raw)
			if got != tt.want {
				t.Errorf("ClassifyPayload(%q) = %s, want %s", tt.raw, got, tt.want)
			}
		})
	}
}
