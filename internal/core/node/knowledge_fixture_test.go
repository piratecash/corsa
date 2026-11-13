package node

// Individual field writers let fixtures construct partial cache entries.
func (s *Service) addKnownBoxKey(address, boxKey string) {
	if address == "" || boxKey == "" || s.suppressesSelfBoxKey(address) {
		return
	}
	s.knowledgeMu.Lock()
	novel := s.known.Add(address)
	s.boxKeys[address] = boxKey
	s.knowledgeMu.Unlock()
	s.noteIdentityDiscovered(address, novel)
}

func (s *Service) addKnownBoxSig(address, boxSig string) {
	if address == "" || boxSig == "" || s.suppressesSelfBoxKey(address) {
		return
	}
	s.knowledgeMu.Lock()
	novel := s.known.Add(address)
	s.boxSigs[address] = boxSig
	s.knowledgeMu.Unlock()
	s.noteIdentityDiscovered(address, novel)
}
