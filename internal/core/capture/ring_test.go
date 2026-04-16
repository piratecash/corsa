package capture

import "testing"

func TestRingBuffer_PushPop(t *testing.T) {
	r := newRingBuffer[int](3)

	if r.len() != 0 {
		t.Fatalf("expected empty ring, got len=%d", r.len())
	}

	dropped := r.push(10)
	if dropped {
		t.Fatal("unexpected drop on first push")
	}
	dropped = r.push(20)
	if dropped {
		t.Fatal("unexpected drop on second push")
	}
	dropped = r.push(30)
	if dropped {
		t.Fatal("unexpected drop on third push")
	}

	if r.len() != 3 {
		t.Fatalf("expected len=3, got %d", r.len())
	}

	v, ok := r.pop()
	if !ok || v != 10 {
		t.Fatalf("expected pop=10, got %d ok=%v", v, ok)
	}
	v, ok = r.pop()
	if !ok || v != 20 {
		t.Fatalf("expected pop=20, got %d ok=%v", v, ok)
	}
	v, ok = r.pop()
	if !ok || v != 30 {
		t.Fatalf("expected pop=30, got %d ok=%v", v, ok)
	}

	_, ok = r.pop()
	if ok {
		t.Fatal("expected empty pop to return false")
	}
}

func TestRingBuffer_DropOldest(t *testing.T) {
	r := newRingBuffer[int](2)

	r.push(1)
	r.push(2)

	// Full — next push should drop oldest (1).
	dropped := r.push(3)
	if !dropped {
		t.Fatal("expected drop=true when ring is full")
	}

	if r.len() != 2 {
		t.Fatalf("expected len=2 after overflow, got %d", r.len())
	}

	v, _ := r.pop()
	if v != 2 {
		t.Fatalf("expected 2 (oldest surviving), got %d", v)
	}
	v, _ = r.pop()
	if v != 3 {
		t.Fatalf("expected 3, got %d", v)
	}
}

func TestRingBuffer_DropOldestMultiple(t *testing.T) {
	r := newRingBuffer[int](3)
	r.push(1)
	r.push(2)
	r.push(3)

	// Overflow twice — should retain 3, 4, 5.
	r.push(4)
	r.push(5)

	if r.len() != 3 {
		t.Fatalf("expected len=3, got %d", r.len())
	}

	items := r.drain()
	if len(items) != 3 {
		t.Fatalf("expected 3 drained items, got %d", len(items))
	}
	if items[0] != 3 || items[1] != 4 || items[2] != 5 {
		t.Fatalf("expected [3,4,5], got %v", items)
	}
}

func TestRingBuffer_Drain(t *testing.T) {
	r := newRingBuffer[string](4)
	r.push("a")
	r.push("b")
	r.push("c")

	items := r.drain()
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("expected [a,b,c], got %v", items)
	}

	if r.len() != 0 {
		t.Fatalf("ring should be empty after drain, got len=%d", r.len())
	}

	// Drain on empty should return nil.
	items = r.drain()
	if items != nil {
		t.Fatalf("expected nil drain on empty ring, got %v", items)
	}
}

func TestRingBuffer_CapacityOne(t *testing.T) {
	r := newRingBuffer[int](1)

	r.push(42)
	if r.len() != 1 {
		t.Fatalf("expected len=1, got %d", r.len())
	}

	dropped := r.push(99)
	if !dropped {
		t.Fatal("expected drop on capacity-1 overflow")
	}

	v, ok := r.pop()
	if !ok || v != 99 {
		t.Fatalf("expected 99, got %d ok=%v", v, ok)
	}
}

func TestRingBuffer_ZeroCapacity(t *testing.T) {
	// Zero capacity is normalised to 1.
	r := newRingBuffer[int](0)
	if r.cap != 1 {
		t.Fatalf("expected cap=1 for zero input, got %d", r.cap)
	}
}
