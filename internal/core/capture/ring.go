package capture

// ringBuffer is a bounded ring buffer that implements drop_oldest overflow
// policy required by plan §9.
//
// A plain buffered channel with select{default} gives drop_newest, which is
// explicitly prohibited. This type provides correct drop_oldest: when the
// buffer is full, push() overwrites the oldest element and returns
// dropped=true so the caller can increment the atomic counter.
//
// Thread safety: the ring is NOT goroutine-safe on its own. The owning
// Session serialises access with its own mutex — the network goroutine
// holds the lock to push(), the writer goroutine holds the lock to pop().
type ringBuffer[T any] struct {
	buf   []T
	head  int // next slot to read (oldest live element)
	tail  int // next slot to write
	count int // number of live elements
	cap   int
}

func newRingBuffer[T any](capacity int) *ringBuffer[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &ringBuffer[T]{
		buf: make([]T, capacity),
		cap: capacity,
	}
}

// push adds an element to the ring. If the ring is full, the oldest
// element is overwritten and the function returns true (dropped=true).
func (r *ringBuffer[T]) push(v T) (dropped bool) {
	if r.count == r.cap {
		r.buf[r.tail] = v
		r.tail = (r.tail + 1) % r.cap
		r.head = (r.head + 1) % r.cap
		return true
	}
	r.buf[r.tail] = v
	r.tail = (r.tail + 1) % r.cap
	r.count++
	return false
}

// pop removes and returns the oldest element. Returns false if empty.
func (r *ringBuffer[T]) pop() (T, bool) {
	if r.count == 0 {
		var zero T
		return zero, false
	}
	v := r.buf[r.head]
	var zero T
	r.buf[r.head] = zero // clear for GC
	r.head = (r.head + 1) % r.cap
	r.count--
	return v, true
}

// len returns the number of live elements.
func (r *ringBuffer[T]) len() int { return r.count }

// drain removes all elements and returns them in FIFO order.
func (r *ringBuffer[T]) drain() []T {
	if r.count == 0 {
		return nil
	}
	out := make([]T, 0, r.count)
	for r.count > 0 {
		v, _ := r.pop()
		out = append(out, v)
	}
	return out
}
