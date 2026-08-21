package domain

import "testing"

// TestMessageDeleteContextRoute pins the classification every delete
// surface shares. The local copy goes in every row — reachability is not
// an input, because a peer being offline is never a reason to keep a
// message the user deleted — and what varies is what the peer is asked.
func TestMessageDeleteContextRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  MessageDeleteContext
		want MessageDeleteRoute
	}{
		{
			name: "an incoming row is scheduled: the author is asked, whatever they answer",
			ctx:  MessageDeleteContext{Outgoing: false},
			want: MessageDeleteRouteScheduled,
		},
		{
			name: "outgoing row the peer confirmed is scheduled",
			ctx:  MessageDeleteContext{Outgoing: true, ConfirmedByPeer: true},
			want: MessageDeleteRouteScheduled,
		},
		{
			name: "outgoing row still unconfirmed is withdrawn",
			ctx:  MessageDeleteContext{Outgoing: true, ConfirmedByPeer: false},
			want: MessageDeleteRouteWithdraw,
		},
		{
			name: "outgoing row proven never to have gone out is recalled",
			ctx:  MessageDeleteContext{Outgoing: true, ConfirmedByPeer: false, NeverEmitted: true},
			want: MessageDeleteRouteRecalled,
		},
		{
			name: "a confirmed row is never recalled, whatever the node claims",
			ctx:  MessageDeleteContext{Outgoing: true, ConfirmedByPeer: true, NeverEmitted: true},
			want: MessageDeleteRouteScheduled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.ctx.Route(); got != tc.want {
				t.Fatalf("Route() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMessageDeleteRoutePredicates pins the two questions callers ask a
// route: is a delivery of ours still to be stopped, and is anything owed
// to the peer once the call returns.
func TestMessageDeleteRoutePredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		route     MessageDeleteRoute
		schedules bool
		cancels   bool
	}{
		{MessageDeleteRouteWithdraw, true, true},
		{MessageDeleteRouteRecalled, false, true},
		{MessageDeleteRouteScheduled, true, false},
	}

	for _, tc := range tests {
		t.Run(string(tc.route), func(t *testing.T) {
			t.Parallel()
			if got := tc.route.SchedulesPeerDeletion(); got != tc.schedules {
				t.Errorf("SchedulesPeerDeletion() = %v, want %v", got, tc.schedules)
			}
			if got := tc.route.CancelsDelivery(); got != tc.cancels {
				t.Errorf("CancelsDelivery() = %v, want %v", got, tc.cancels)
			}
		})
	}
}
