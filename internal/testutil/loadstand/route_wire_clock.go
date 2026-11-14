package loadstand

// RouteWireClockDomain identifies the kernel boot and its current time
// namespace. A BOOTTIME reading can only be compared inside this domain.
type RouteWireClockDomain struct {
	BootID string
	Device uint64
	Inode  uint64
}

func (d RouteWireClockDomain) valid() bool {
	if len(d.BootID) != 36 || d.Device == 0 || d.Inode == 0 || d.BootID == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i := range len(d.BootID) {
		c := d.BootID[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
