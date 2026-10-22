//go:build linux

package loadstand

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/netip"
	"slices"
)

// Only the real Member factory can mint this private original graph. The wire
// projection is public configuration over original private helper channels;
// it is never itself a namespace or successfulStart capability.
type linuxPlannedLaunch struct {
	membership                  *linuxMemberMembership
	previous                    *LinuxFixture
	network                     *plannedNetwork
	member                      plannedNetworkMember
	plan                        PlannedChildConfig
	ordinal                     IncarnationOrdinal
	binding                     launchBinding
	profileDevice, profileInode uint64
}
type linuxPlannedInit struct {
	Membership                  *linuxMemberMembershipDescriptor `json:",omitempty"`
	ProfileDevice, ProfileInode uint64
	Plan                        PlannedChildConfig
	Ordinal                     IncarnationOrdinal
	FinalNonce                  LaunchNonce
	FinalFingerprint            LaunchFingerprint
}

func plannedLaunchFingerprint(config ChildConfig, plan PlannedChildConfig, target ExecutableSHA256, ordinal IncarnationOrdinal) (LaunchFingerprint, error) {
	raw, err := json.Marshal(struct {
		Version uint8
		Config  ChildConfig
		Plan    PlannedChildConfig
		Target  ExecutableSHA256
		Ordinal IncarnationOrdinal
	}{2, config, plan, target, ordinal})
	if err != nil {
		return LaunchFingerprint{}, err
	}
	return sha256.Sum256(raw), nil
}
func newPlannedLaunchBinding(config ChildConfig, plan PlannedChildConfig, target ExecutableSHA256, ordinal IncarnationOrdinal) (launchBinding, error) {
	var binding launchBinding
	if ordinal == 0 {
		return binding, ErrInvalidConfiguration
	}
	if _, err := rand.Read(binding.nonce[:]); err != nil {
		return binding, err
	}
	var err error
	binding.fingerprint, err = plannedLaunchFingerprint(config, plan, target, ordinal)
	return binding, err
}
func (p *linuxPlannedLaunch) configValid(spec LinuxFixtureSpec, namespace *OwnedNetNS) bool {
	if p == nil || p.ordinal > 1 && (p.profileDevice == 0 || p.profileInode == 0) || p.network == nil || !p.network.ready || p.ordinal == 0 || p.binding.nonce == (LaunchNonce{}) || p.network.plan.prefix != p.plan.Prefix {
		return false
	}
	found := false
	for _, member := range p.network.plan.members {
		if member == p.member {
			found = true
		}
	}
	if !found {
		return false
	}
	listen, err := netip.ParseAddrPort(string(spec.Config.Listen))
	if err != nil || listen.Addr() != p.member.address {
		return false
	}
	backend, ok := p.network.backend.(*linuxPlannedNetworkBackend)
	if !ok {
		return false
	}
	row := backend.resource(plannedNamespace, string(p.member.id))
	if row == nil || row.namespace != namespace || p.member.id != p.plan.ID {
		return false
	}
	_, _, err = copyPlannedChild(spec.Config, p.plan)
	if err != nil {
		return false
	}
	for _, peer := range p.plan.Peers {
		found := false
		address, err := netip.ParseAddrPort(string(peer.Endpoint))
		if err != nil {
			return false
		}
		for _, member := range p.network.plan.members {
			if member.id == peer.ID && member.role == peer.Role && member.endpoint == peer.Endpoint && member.address == address.Addr() {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	fingerprint, err := plannedLaunchFingerprint(spec.Config, p.plan, spec.TargetSHA256, p.ordinal)
	return err == nil && fingerprint == p.binding.fingerprint
}
func validLinuxFixtureConfiguration(spec LinuxFixtureSpec, namespace *OwnedNetNS) bool {
	if spec.planned == nil {
		return spec.Config.ValidateLoopback() == nil && len(spec.Config.Bootstrap) == 1
	}
	return spec.planned.configValid(spec, namespace)
}
func samePlannedChild(a, b PlannedChildConfig) bool {
	return a.ID == b.ID && a.Role == b.Role && a.Prefix == b.Prefix && slices.Equal(a.Peers, b.Peers)
}
func (p *linuxPlannedInit) valid(config ChildConfig, target ExecutableSHA256) bool {
	if p == nil || p.Ordinal == 0 || p.FinalNonce == (LaunchNonce{}) || p.Ordinal > 1 && (p.ProfileDevice == 0 || p.ProfileInode == 0) || p.Ordinal == 1 && (p.ProfileDevice != 0 || p.ProfileInode != 0) {
		return false
	}
	_, _, err := copyPlannedChild(config, p.Plan)
	if err != nil {
		return false
	}
	fingerprint, err := plannedLaunchFingerprint(config, p.Plan, target, p.Ordinal)
	return err == nil && fingerprint == p.FinalFingerprint
}

func validLinuxHelperChildConfiguration(init linuxInit) bool {
	if init.Planned == nil {
		return init.Config.ValidateLoopback() == nil
	}
	return init.Planned.valid(init.Config, init.TargetHash)
}
