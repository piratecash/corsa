//go:build linux

package loadstand

import "encoding/json"

func plannedChildEnvironment(config ChildConfig, plan PlannedChildConfig, credentials *Credentials) ([]string, error) {
	config, plan, err := copyPlannedChild(config, plan)
	if err != nil {
		return nil, err
	}
	environment, err := config.environmentUnchecked(credentials)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(plan)
	if err != nil || len(raw) > linuxPacketLimit {
		return nil, ErrInvalidConfiguration
	}
	return append(environment, "LOADSTAND_MEMBER_PLAN="+string(raw)), nil
}
