package node

import "testing"

func TestLoadStandPlanSDKOrigin(t *testing.T) {
	for _, profile := range loadStandPlanFixtureProfiles {
		plan, err := buildLoadStandPlan(loadStandPlanFixture(profile, 13))
		if err != nil {
			t.Fatal(err)
		}
		roles := map[loadStandNodeID]loadStandRole{}
		for _, n := range plan.Nodes() {
			roles[n.ID()] = n.Role()
		}
		for _, e := range plan.Timeline() {
			if (e.Kind() == loadStandEventDirectMessage || e.Kind() == loadStandEventGossip) && roles[e.Node()] != loadStandRoleEdge {
				t.Fatalf("non-SDK full member originates user traffic: %+v", e)
			}
		}
	}
}
