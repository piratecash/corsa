package sessionv2

import (
	"errors"
	"testing"
)

func TestTheModeFollowsTheVersionConstants(t *testing.T) {
	const activation = 31
	cases := []struct {
		protocol, minimum int
		want              Mode
	}{
		{30, 26, ModeLegacyOnly},
		{31, 26, ModeTransition},
		{31, 30, ModeTransition},
		{32, 31, ModeV2Only},
		{31, 31, ModeV2Only},
	}
	for _, tc := range cases {
		got, err := ModeFor(tc.protocol, tc.minimum, activation)
		if err != nil || got != tc.want {
			t.Errorf("ModeFor(%d, %d) = %v, %v; want %v", tc.protocol, tc.minimum, got, err, tc.want)
		}
	}
	if _, err := ModeFor(30, 31, activation); !errors.Is(err, ErrVersionOrder) {
		t.Errorf("a minimum above the protocol = %v, want ErrVersionOrder", err)
	}
}
