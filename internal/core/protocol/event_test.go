package protocol

import "testing"

// TestIsDMTopic pins the DM-class predicate that routing target
// selection (GossipRouter, TableRouter, routingTargetsForMessage),
// push-subscriber gating, relay tracking, and per-message verification
// gates rely on. A regression here would silently route control DMs
// through the non-DM/broadcast path and break point-to-point delivery
// to directly-connected recipients.
func TestIsDMTopic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		topic string
		want  bool
	}{
		{"dm", true},
		{TopicControlDM, true}, // == "dm-control"
		{"", false},
		{"DM", false},       // case-sensitive: wire format is lowercase
		{"dm-other", false}, // similar prefix is not DM-class
		{"dm-control-x", false},
		{"gazeta", false},
		{"file", false}, // FileTransferTopic — file-command frame, not DM
	}

	for _, tc := range cases {
		got := IsDMTopic(tc.topic)
		if got != tc.want {
			t.Errorf("IsDMTopic(%q) = %v, want %v", tc.topic, got, tc.want)
		}
	}
}
