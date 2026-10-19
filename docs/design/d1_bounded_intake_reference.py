"""R2 bounded design witness; no production/default approval.

Covers L1 intake, trial/commit group-target accounting and sticky 256+1 registry.
Not implemented: C5, lower-level RR, T composition, canonical target registry,
input/pair/session-distinct/degree gates, runtime/network measurements.
Times/budgets are exact rational TEST INPUTS, never approved timing profiles.
"""
from dataclasses import dataclass
from fractions import Fraction as F
import unittest


@dataclass
class Row:
    session: int
    target: int
    received: F
    level: int = 1
    admitted: bool = False
    slot: F = F(0)
    group: str = ""


class Bucket:
    def __init__(self, at, tokens):
        self.at, self.tokens = F(at), F(tokens)

    def reserve(self, now, latest):
        at = max(F(now), self.at)
        available = min(F(4), self.tokens + at - self.at)
        if available < 1:
            at += 1 - available
            available = F(1)
        if at > latest:
            return None
        self.at, self.tokens = at, available - 1
        return at

    def replay(self, at):
        at = F(at)
        if at < self.at:
            raise ValueError('unordered prior reservation')
        available = min(F(4), self.tokens + at - self.at)
        if available < 1:
            raise ValueError('prior reservation exceeds existing bucket')
        self.at, self.tokens = at, available - 1


class Intake:
    """One global round, bounded rows; L1-only fairness witness, not all gates."""
    def __init__(self, cursor=0, watermark=0, delta=F(1, 10), budget=F(15, 4), held=()):
        if not 0 < delta < 6 or not 0 < budget < 6:
            raise ValueError('explicit synthetic profile required')
        self.cursor, self.watermark = cursor, watermark
        self.delta, self.budget = F(delta), F(budget)
        self.rows, self.cut, self.start = list(held), None, None
        self.refused, self.replaced = 0, 0
        self.check()

    def check(self):
        assert len(self.rows) <= 512
        for row in self.rows:
            assert sum(r.target == row.target for r in self.rows) <= 16
            assert sum(r.session == row.session for r in self.rows) <= 16
        staged = [r.session for r in self.rows if not r.admitted]
        assert len(staged) == len(set(staged))

    def rank(self, row):
        return row.level, int(row.session <= self.cursor), row.session

    def offer(self, session, target, at, max_live_id, level=1, group=None):
        at = F(at)
        assert 0 < session <= max_live_id and level in (1, 2, 3)
        if self.cut is None:
            self.start, self.cut = at, at + self.delta
            if not self.watermark:
                self.watermark = max_live_id
        if at >= self.cut:
            self.refused += 1
            return 'next_round'
        if level == 1 and session > self.watermark:
            self.refused += 1
            return 'next_sweep'
        if any(r.session == session and not r.admitted for r in self.rows):
            self.refused += 1
            return 'one_candidate'
        if sum(r.session == session for r in self.rows) >= 16:
            self.refused += 1
            return 'source_full'
        row = Row(session, target, at, level, group=group or f"g{session}")
        target_full = sum(r.target == target for r in self.rows) >= 16
        if target_full or len(self.rows) >= 512:
            victims = [r for r in self.rows if not r.admitted and
                       (not target_full or r.target == target)]
            if not victims or self.rank(row) >= self.rank(max(victims, key=self.rank)):
                self.refused += 1
                return 'physical_full'
            self.rows.remove(max(victims, key=self.rank))
            self.replaced += 1
        self.rows.append(row)
        self.check()
        return 'provisional'

    def close(self, now, max_live_id, tokens=4, prior=(), quota=None):
        if self.cut is None:
            return []
        staged = sorted((r for r in self.rows if not r.admitted), key=self.rank)
        if F(now) != self.cut:  # zero dispatch slack in this witness
            self.refused += len(staged)
            self.rows = [r for r in self.rows if r.admitted]
            return []
        calendars, accepted = {}, []  # bounded by the physical <=512 rows
        old_cursor, wrapped = self.cursor, False
        for row in staged:
            if row.target not in calendars:
                bucket = Bucket(self.start, tokens)
                for reserved in prior:
                    bucket.replay(self.start + F(reserved))
                calendars[row.target] = bucket
            current = calendars[row.target]
            trial = Bucket(current.at, current.tokens)
            slot = trial.reserve(now, row.received + self.budget)
            if slot is None or (quota is not None and
                                not quota.commit(row.group, row.target, now, slot)):
                self.rows.remove(row)
                self.refused += 1
                continue
            calendars[row.target] = trial  # publish calendar only with group debit
            row.admitted, row.slot = True, slot
            accepted.append(row)
            if row.level == 1:
                wrapped |= row.session <= old_cursor
                self.cursor = row.session
        if wrapped:
            self.watermark = max_live_id
        elif not any(r.level == 1 for r in staged):
            self.cursor, self.watermark = 0, max_live_id
        self.check()
        return accepted


class Groups:
    """256 named + ONE overflow, no physical-overflow membership structure."""
    OVERFLOW, W = 256, F(600)

    def __init__(self):
        self.named, self.generation, self.last, self.refs = {}, 1, None, 0

    def lookup(self, group, now, level):
        now = F(now)
        if self.last is not None and now < self.last:
            raise ValueError('clock moved backwards')
        self.last = now
        if group not in self.named and len(self.named) < self.OVERFLOW:
            self.named[group] = len(self.named)
        cell = self.named.get(group, self.OVERFLOW)
        assert len(self.named) + 1 <= 257
        return (self.generation, cell), level

    def peek(self, group):
        if group in self.named:
            return self.named[group]
        return min(len(self.named), self.OVERFLOW)

    def resolve(self, stale, group, now, level):
        # A generation tag never substitutes for authoritative reclassification.
        return self.lookup(group, now, level)

    def reset(self, now, rolling_records_empty):
        if self.refs or not rolling_records_empty:
            return False
        if self.last is not None and F(now) - self.last < self.W:
            return False
        self.named.clear()  # same one physical registry, never two epochs
        self.generation += 1
        self.last = None
        return True


class GroupQuota:
    """Single-owner trial/commit model; records <= C_node + 16*257 <= 7952.

    Counts aggregate (cell,target) commitments, conservatively until reserved
    forwarding time + W. Repeated target refreshes lifetime, not distinct count.
    No names of physical overflow groups are stored. Session-distinct accounting
    is a separate missing gate. Production must transact this with real calendar.
    """
    def __init__(self, c_node=512):
        if not 512 <= c_node <= 3840:
            raise ValueError('C_node outside existing cap')
        self.groups, self.records = Groups(), {}
        self.c_node, self.hard = c_node, c_node + 16 * 257
        self.now = F(0)

    def commit(self, group, target, now, slot):
        now, slot = F(now), F(slot)
        if now < self.now or slot < now:
            raise ValueError('nonmonotonic or invalid forwarding slot')
        self.now = now
        self.records = {key: expiry for key, expiry in self.records.items()
                        if expiry > now}  # bounded scan, no unbounded staging
        cell = self.groups.peek(group)
        key = cell, target
        cells = {c for c, _ in self.records}
        k = len(cells | {cell})
        share = max(16, self.c_node // k)  # explicit conservative integer proposal
        occupancy = sum(c == cell for c, _ in self.records)
        if key not in self.records and (occupancy >= share or len(self.records) >= self.hard):
            return False
        actual, _ = self.groups.lookup(group, now, 1)
        assert actual == (self.groups.generation, cell)
        self.records[key] = max(self.records.get(key, F(0)), slot + Groups.W)
        assert len(self.records) <= self.hard <= 7952
        assert len(self.groups.named) <= 256
        return True

    def seed_named(self):
        # Test fixture: 256 live accounting cells, no physical overflow names.
        for i in range(256):
            assert self.commit(f'named{i}', -(i + 1), 0, 0)


class BoundedWitnessTests(unittest.TestCase):
    def test_ten_level_one_sessions_rotate_seven_admissions(self):
        counts, cursor, watermark = {s: 0 for s in range(1, 11)}, 0, 10
        for window in range(10):
            round_ = Intake(cursor, watermark)
            at = F(30 * window)
            for session in range(1, 11):
                self.assertEqual(round_.offer(session, 1, at, 10), 'provisional')
                self.assertEqual(round_.offer(session, 1, at, 10), 'one_candidate')
            admitted = round_.close(at + round_.delta, 10)
            self.assertEqual(len(admitted), 7)
            for row in admitted:
                counts[row.session] += 1
            cursor, watermark = round_.cursor, round_.watermark
        self.assertEqual(set(counts.values()), {7})

    def test_round_is_not_extended_and_late_timer_does_not_admit(self):
        round_ = Intake()
        round_.offer(1, 1, 0, 2)
        round_.offer(2, 1, F(9, 100), 2)
        self.assertEqual(round_.cut, F(1, 10))
        self.assertEqual(round_.offer(2, 1, round_.cut, 2), 'next_round')
        self.assertEqual(round_.close(round_.cut + F(1, 1000), 2), [])
        self.assertEqual(len(round_.rows), 0)

    def test_admitted_occupancy_and_lower_level_tokens_not_refunded(self):
        held = [Row(i + 1, 1, F(0), admitted=True) for i in range(16)]
        full = Intake(watermark=100, held=held)
        self.assertEqual(full.offer(100, 1, 0, 100), 'physical_full')
        self.assertTrue(all(r.admitted for r in full.rows))
        round_ = Intake()
        for s in range(1, 11):
            round_.offer(s, 1, 0, 10)
        accepted = round_.close(round_.cut, 10, prior=(0, 0, 0, 0))
        self.assertEqual([r.slot for r in accepted], [F(1), F(2), F(3)])

    def test_better_provisional_replaces_lower_level_without_extra_storage(self):
        round_ = Intake(watermark=100)
        for s in range(1, 17):
            round_.offer(s, 1, 0, 100, level=3)
        self.assertEqual(round_.offer(100, 1, 0, 100), 'provisional')
        self.assertEqual(len(round_.rows), 16)
        self.assertEqual(round_.replaced, 1)
        self.assertEqual(round_.close(round_.cut, 100)[0].session, 100)

    def test_node512_and_source16_and_one_candidate_are_physical(self):
        held = [Row(s, (s + j) % 32, F(0), admitted=True)
                for s in range(1, 33) for j in range(16)]
        full = Intake(watermark=100, held=held)
        self.assertEqual(full.offer(100, 99, 0, 100), 'physical_full')
        source_full = Intake(watermark=100, held=held[:16])
        self.assertEqual(source_full.offer(1, 99, 0, 100), 'source_full')
        round_ = Intake()
        self.assertEqual(round_.offer(1, 1, 0, 2), 'provisional')
        self.assertEqual(round_.offer(1, 2, 0, 2), 'one_candidate')

    def test_watermark_defeats_endless_high_id_insertion(self):
        # With only a stable cursor, endlessly new IDs prevent wrap to old1.
        cursor = 2
        for _ in range(20):
            cursor = min(s for s in (1, cursor + 1) if s > cursor)
        self.assertGreater(cursor, 20)
        round_ = Intake(cursor=2, watermark=2, budget=F(1, 4))
        self.assertEqual(round_.offer(3, 1, 0, 3), 'next_sweep')
        self.assertEqual(round_.offer(1, 1, 0, 3), 'provisional')
        admitted = round_.close(round_.cut, 3, tokens=1)
        self.assertEqual([r.session for r in admitted], [1])
        self.assertEqual(round_.watermark, 3)

    def test_257_aged_groups_fit257_but_only256_are_named(self):
        registry = Groups()
        refs = [registry.lookup(f'g{i}', 0, level=2)[0] for i in range(257)]
        self.assertEqual(len(set(refs)), 257)
        self.assertNotEqual(refs[255], refs[256])
        extra, _ = registry.lookup("group258", 0, 2)
        self.assertEqual(refs[256], extra)
        self.assertEqual(len(registry.named), 256)
        (ref, priority) = registry.lookup('late_contact', 1, level=1)
        self.assertEqual(ref[1], Groups.OVERFLOW)
        self.assertEqual(priority, 1)
        self.assertNotIn('late_contact', registry.named)

    def test_mapping_no_epoch_boundary_reset_or_overflow_promotion(self):
        registry = Groups()
        for i in range(256):
            registry.lookup(f'g{i}', 0, 3)
        old, _ = registry.lookup('overflow_honest', 599, 1)
        self.assertFalse(registry.reset(600, rolling_records_empty=True))
        new, _ = registry.lookup('overflow_honest', 600, 1)
        self.assertEqual(old, new)
        registry.refs = 1
        self.assertFalse(registry.reset(1200, rolling_records_empty=True))
        registry.refs = 0
        self.assertFalse(registry.reset(1200, rolling_records_empty=False))
        self.assertTrue(registry.reset(1200, rolling_records_empty=True))
        reclassified, level = registry.resolve(old, 'overflow_honest', 1200, 1)
        self.assertNotEqual(old[0], reclassified[0])
        self.assertEqual(level, 1)
        self.assertEqual(len(registry.named) + 1, 2)

    def test_reference_deadline_boundary_is_exact(self):
        round_ = Intake(budget=F(1, 10))
        round_.offer(1, 1, 0, 1)
        accepted = round_.close(round_.cut, 1, tokens=1)
        self.assertEqual(accepted[0].slot, F(1, 10))


    def test_16_distinct_offers_cannot_spend_overflow_before_fair_round(self):
        quota = GroupQuota()
        quota.seed_named()
        round_ = Intake()
        self.assertEqual(round_.offer(1, 100, 0, 16, group='abuser'), 'provisional')
        for target in range(101, 116):
            self.assertEqual(round_.offer(1, target, 0, 16, group='abuser'), 'one_candidate')
        self.assertEqual(len(quota.records), 256)
        for session in range(2, 17):
            self.assertEqual(round_.offer(session, 100 + session, 0, 16,
                                          group=f'honest{session}'), 'provisional')
        accepted = round_.close(round_.cut, 16, quota=quota)
        self.assertEqual([r.session for r in accepted], list(range(1, 17)))
        charged = {target for cell, target in quota.records if cell == Groups.OVERFLOW}
        self.assertEqual(charged, {100} | set(range(102, 117)))
        self.assertEqual(sum(r.session == 1 for r in accepted), 1)
        self.assertEqual(len(quota.groups.named), 256)  # no overflow names retained

    def test_shared_group_scarcity_rotates_instead_of_first_arrival_debit(self):
        quota, cursor, watermark, counts = GroupQuota(), 0, 10, {s: 0 for s in range(1, 11)}
        # Each 601s cycle leaves exactly seven of the common floor's slots free.
        for cycle in range(10):
            at = F(601 * cycle)
            if cycle == 0:
                quota.seed_named()
            else:
                for i in range(256):
                    self.assertTrue(quota.commit(f'named{i}', -(i + 1), at, at))
            for target in range(9):
                self.assertTrue(quota.commit('earlier_overflow', -(1000 + target), at, at))
            round_ = Intake(cursor, watermark)
            for s in range(1, 11):
                round_.offer(s, 100 + s, at, 10, group=f'overflow{s}')
            accepted = round_.close(round_.cut, 10, quota=quota)
            self.assertEqual(len(accepted), 7)
            for row in accepted:
                counts[row.session] += 1
            cursor, watermark = round_.cursor, round_.watermark
        self.assertEqual(set(counts.values()), {7})

    def test_group_refusal_publishes_no_calendar_and_no_record(self):
        quota = GroupQuota()
        quota.seed_named()
        for i in range(16):
            self.assertTrue(quota.commit('overflow', i, 0, 0))
        original = dict(quota.records)
        round_ = Intake()
        round_.offer(1, 99, 0, 2, group='refused_overflow')
        round_.offer(2, 99, 0, 2, group='named0')
        accepted = round_.close(round_.cut, 2, tokens=1, quota=quota)
        self.assertEqual([(r.session, r.slot) for r in accepted], [(2, round_.cut)])
        self.assertNotIn((Groups.OVERFLOW, 99), quota.records)
        self.assertEqual(len(quota.records), len(original) + 1)
        self.assertNotIn('refused_overflow', quota.groups.named)

    def test_deadline_refusal_cancellation_and_reserved_time_accounting(self):
        quota = GroupQuota()
        round_ = Intake(budget=F(1, 20))
        round_.offer(1, 1, 0, 1, group='never_committed')
        self.assertEqual(round_.close(round_.cut, 1, quota=quota), [])
        self.assertEqual(quota.records, {})
        self.assertEqual(quota.groups.named, {})
        self.assertTrue(quota.commit('committed_then_cancelled', 1, 0, 3))
        # Cancel/close has no debit-removal API: unsent failed commitments stay.
        self.assertEqual(quota.records[(0, 1)], F(603))
        self.assertTrue(quota.commit('another', 2, 600, 600))
        self.assertIn((0, 1), quota.records)
        self.assertTrue(quota.commit('another', 3, 603, 603))
        self.assertNotIn((0, 1), quota.records)

    def test_hard_record_limit_never_grants_fresh_credit_after_k_changes(self):
        quota = GroupQuota(3840)
        self.assertEqual(quota.hard, 7952)
        # Construct an artificial internal snapshot to exercise the hard guard.
        # This is not a claim that one cell can legally acquire 7952 targets.
        quota.groups.lookup('old', 0, 1)
        quota.records = {(0, target): F(600) for target in range(7952)}
        self.assertFalse(quota.commit('new', 99999, 1, 1))
        self.assertEqual(len(quota.records), 7952)
        self.assertNotIn('new', quota.groups.named)
        self.assertTrue(quota.commit('old', 0, 1, 1))
        self.assertEqual(len(quota.records), 7952)


    def test_hard_bound_is_reached_by_ordinary_partial_model_commits(self):
        quota = GroupQuota(3840)
        count = 0
        for group, size in enumerate((3840, 1920, 1280, 912)):
            for target in range(size):
                at = F(count, 32)
                self.assertTrue(quota.commit(f'g{group}', target, at, at))
                count += 1
        self.assertEqual(count, 7952)
        self.assertEqual(len({target for _, target in quota.records}), 3840)
        self.assertEqual(sum(cell == 3 for cell, _ in quota.records), 912)
        self.assertLess(912, quota.c_node // 4)
        self.assertFalse(quota.commit('g3', 912, F(count, 32), F(count, 32)))
        self.assertEqual(len(quota.records), quota.hard)
        # Only ordinary ledger commits were used, with no expiry yet (t < W).
        # Missing source/pair/degree/runtime gates are not proved by this model.

    def test_old_precharge_counterexample_excludes_concurrent_honest_sessions(self):
        quota = GroupQuota()
        quota.seed_named()
        round_ = Intake()
        for target in range(100, 116):
            # Deliberately reproduce R1's wrong ordering outside the R2 producer.
            self.assertTrue(quota.commit('abuser', target, 0, 0))
            round_.offer(1, target, 0, 16, group='abuser')
        for session in range(2, 17):
            round_.offer(session, 200 + session, 0, 16, group=f'honest{session}')
        admitted = round_.close(round_.cut, 16, quota=quota)
        self.assertEqual([row.session for row in admitted], [1])
        self.assertEqual(len(quota.records), 272)

    def test_full_level_one_intake_replaces_by_rotation_and_cannot_grow(self):
        round_ = Intake(cursor=8, watermark=100)
        for session in range(1, 17):
            self.assertEqual(round_.offer(session, 1, 0, 100), 'provisional')
        self.assertEqual(round_.offer(17, 1, 0, 100), 'provisional')
        self.assertEqual(len(round_.rows), 16)
        self.assertNotIn(8, [row.session for row in round_.rows])
        self.assertEqual(round_.offer(100, 1, 0, 100), 'provisional')
        self.assertEqual(len(round_.rows), 16)
        # All rows remain L1 provisional, so replacement revoked no admission.
        self.assertTrue(all(row.level == 1 and not row.admitted for row in round_.rows))
        admitted = round_.close(round_.cut, 100)
        self.assertEqual([row.session for row in admitted], list(range(9, 16)))


if __name__ == '__main__':
    unittest.main()
