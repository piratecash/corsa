package findnodebridge

import "testing"

func TestConstructionPointRequiresOriginalImmutableClockDomain(t *testing.T) {
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	d, err := NewRuntimeDomain(done)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewRuntimeDomain(done)
	if err != nil {
		t.Fatal(err)
	}
	clock, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		t.Fatal(err)
	}
	if !d.Consumer().MatchesPoint(at) || other.Consumer().MatchesPoint(at) || (Consumer{}).MatchesPoint(at) || d.Consumer().MatchesPoint(OwnedInstant{}) {
		t.Fatal("construction point provenance escaped original domain")
	}
	if _, err = d.Observer().BeginCapture(); err == nil {
		t.Fatal("immutable construction point comparison granted Ready capture")
	}
}
