"""Exact-arithmetic reference for the proposed, inactive D1 admission rule.

This is a design witness, not the node implementation. Time is in seconds;
all arithmetic is rational. Test bounds are synthetic, not Tor measurements.
Run: python3 docs/design/d1_admission_reference.py
"""

from collections import deque
from dataclasses import dataclass
from fractions import Fraction
import unittest


@dataclass
class Bucket:
    rate: Fraction
    burst: Fraction
    tokens: Fraction
    at: Fraction = Fraction(0)

    def available_at(self, now):
        now = max(Fraction(now), self.at)
        tokens = min(self.burst, self.tokens + self.rate * (now - self.at))
        return now if tokens >= 1 else now + (1 - tokens) / self.rate

    def consume(self, now):
        now = Fraction(now)
        if now < self.at:
            raise ValueError("reservation moved backwards")
        tokens = min(self.burst, self.tokens + self.rate * (now - self.at))
        if tokens < 1:
            raise ValueError("reservation exceeds the token budget")
        self.tokens, self.at = tokens - 1, now


def forward_batch(requests, tokens, latest, prior=(), cursor=0, capacity=16):
    """Reserve a bounded batch, one request per nonempty source per round.

    Prior reservations, including lower levels, are irrevocable in this
    conservative append-only model: it does not fill earlier calendar gaps
    and does not prove maximal admission. Spent tokens are in `tokens`.
    The caller must pass only requests that passed the existing source,
    pair, node and distinct-target gates. A real implementation also needs
    a bounded intake/round policy; batching here makes no memory claim.
    """
    bucket = Bucket(Fraction(1), Fraction(4), Fraction(tokens))
    for reserved in prior:
        bucket.consume(reserved)
    sources = sorted(requests)
    if not sources:
        return [], cursor
    cursor %= len(sources)
    order = sources[cursor:] + sources[:cursor]
    queues = {source: deque(requests[source]) for source in sources}
    ready = deque(source for source in order if queues[source])
    accepted = []
    next_cursor = cursor
    while ready and len(accepted) + len(prior) < capacity:
        source = ready.popleft()
        send_at = bucket.available_at(0)
        if send_at > latest:
            break
        bucket.consume(send_at)
        accepted.append((source, queues[source].popleft(), send_at))
        next_cursor = (sources.index(source) + 1) % len(sources)
        if queues[source]:
            ready.append(source)
    return accepted, next_cursor


def signature_batch(sources, latest):
    """Proposed no-extra-burst signer, also respecting session buckets."""
    node = Bucket(Fraction(4), Fraction(1), Fraction(1))
    sessions = {}
    accepted = []
    for source in sources:
        session = sessions.setdefault(
            source, Bucket(Fraction(1), Fraction(4), Fraction(4))
        )
        slot = max(node.available_at(0), session.available_at(0))
        if slot > latest:
            continue
        node.consume(slot)
        session.consume(slot)
        accepted.append((source, slot))
    return accepted


class AdmissionReferenceTests(unittest.TestCase):
    # Synthetic bounds: 6 - target(1) - network(1) - runtime(1/4).
    latest = Fraction(15, 4)

    def test_maximum_legal_level_one_burst(self):
        requests = {f"s{i:02}": list(range(3)) for i in range(10)}
        accepted, _ = forward_batch(requests, 4, self.latest)
        self.assertEqual(len(accepted), 7)
        self.assertEqual([x[2] for x in accepted], [0, 0, 0, 0, 1, 2, 3])
        self.assertEqual(len({x[0] for x in accepted}), 7)
        self.assertEqual(30 - len(accepted), 23)

    def test_empty_bucket_cannot_borrow_spent_lower_level_tokens(self):
        accepted, _ = forward_batch({"a": range(3), "b": range(3)}, 0, self.latest)
        self.assertEqual([x[2] for x in accepted], [1, 2, 3])

    def test_existing_lower_level_reservations_reduce_budget(self):
        prior = (0, 0, 0, 0, 1, 2)
        accepted, _ = forward_batch({"a": range(3)}, 4, self.latest, prior)
        self.assertEqual([x[2] for x in accepted], [3])

    def test_rotation_does_not_always_choose_the_first_seven_sources(self):
        requests = {f"s{i:02}": range(3) for i in range(10)}
        counts, cursor = {source: 0 for source in requests}, 0
        # Independent windows 30 seconds apart, with a refilled bucket.
        for _ in range(10):
            accepted, cursor = forward_batch(requests, 4, self.latest, cursor=cursor)
            for source, _, _ in accepted:
                counts[source] += 1
        self.assertEqual(set(counts.values()), {7})

    def test_partial_tokens_deadlines_and_queue_capacity(self):
        for eighths in range(33):
            for quarters in range(25):
                for capacity in (0, 1, 4, 7, 16):
                    tokens, latest = Fraction(eighths, 8), Fraction(quarters, 4)
                    accepted, _ = forward_batch(
                        {"a": range(100)}, tokens, latest, capacity=capacity
                    )
                    self.assertLessEqual(len(accepted), capacity)
                    self.assertLessEqual(len(accepted), int(tokens + latest))
                    replay = Bucket(Fraction(1), Fraction(4), tokens)
                    for _, _, slot in accepted:
                        self.assertLessEqual(slot, latest)
                        replay.consume(slot)

    def test_node_and_session_signing_budgets_intersect(self):
        accepted = signature_batch(["a"] * 8 + ["b"] * 8, Fraction(2))
        self.assertTrue(accepted)
        for (_, first), (_, second) in zip(accepted, accepted[1:]):
            self.assertGreaterEqual(second - first, Fraction(1, 4))
        by_session = {}
        for source, slot in accepted:
            bucket = by_session.setdefault(
                source, Bucket(Fraction(1), Fraction(4), Fraction(4))
            )
            bucket.consume(slot)
            self.assertLessEqual(slot, 2)

    def test_average_load_is_not_a_six_second_burst_bound(self):
        accepted, _ = forward_batch({"a": range(30)}, 4, Fraction(6), capacity=30)
        self.assertEqual(len(accepted), 10)
        self.assertLess(len(accepted), 30)


if __name__ == "__main__":
    unittest.main()
