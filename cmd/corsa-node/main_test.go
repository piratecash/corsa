package main

import "testing"

func TestVersionRequested(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "long flag",
			args: []string{"--version"},
			want: true,
		},
		{
			name: "single dash flag",
			args: []string{"-version"},
			want: true,
		},
		{
			name: "unknown args are ignored",
			args: []string{"--unknown-supervisor-arg", "--version"},
			want: true,
		},
		{
			name: "unknown only",
			args: []string{"--unknown-supervisor-arg"},
			want: false,
		},
		{
			name: "explicit true",
			args: []string{"--version=true"},
			want: true,
		},
		{
			name: "explicit false",
			args: []string{"--version=false"},
			want: false,
		},
		{
			name: "separator stops scan",
			args: []string{"--", "--version"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := versionRequested(tt.args); got != tt.want {
				t.Fatalf("versionRequested(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}
