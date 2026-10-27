import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from hookcontract.core import Event, Snapshot
from hookcontract.store import InjectedFailure, Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fixture.sqlite"
        self.store = Store(self.path)

    def apply(self, event, **kwargs):
        return self.store.apply(event, event.body(), **kwargs)

    def test_duplicate_and_conflicting_identity(self):
        event = Event("a", "s", 1, 10)
        self.assertEqual(self.apply(event), "applied")
        self.assertEqual(self.apply(event), "duplicate")
        self.assertEqual(self.apply(Event("a", "s", 2, 20)), "conflict")
        self.assertEqual(self.store.observe("s"), Snapshot(1, 10, 1, 1))

    def test_stale_and_redundant_receipts_are_durable(self):
        for name, head_version, incoming in (("stale", 3, 2), ("redundant", 3, 3)):
            self.apply(Event(name + "_head", name, head_version, 30))
            self.assertIn(self.apply(Event(name + "_receipt", name, incoming, 30)),
                          ("stale", "redundant"))
            reopened = Store(self.path)
            changed = Event(name + "_receipt", name, 4, 40)
            self.assertEqual(reopened.apply(changed, changed.body()), "conflict")
            self.assertEqual(reopened.observe(name), Snapshot(3, 30, 1, 2))

    def test_current_version_collision_leaves_no_receipt(self):
        self.apply(Event("a", "s", 1, 10))
        self.assertEqual(self.apply(Event("b", "s", 1, 20)), "conflict")
        self.assertEqual(self.store.observe("s"), Snapshot(1, 10, 1, 1))

    def test_transaction_failpoints_roll_back_every_write(self):
        for point in ("after_receipt", "after_state"):
            event = Event(point, point, 1, 10)
            with self.assertRaises(InjectedFailure):
                self.apply(event, failpoint=point)
            self.assertEqual(self.store.observe(point), Snapshot())
            self.assertEqual(self.apply(event), "applied")
            self.assertEqual(self.store.observe(point), Snapshot(1, 10, 1, 1))

    def test_concurrent_duplicates_apply_once(self):
        event = Event("a", "s", 1, 10)
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(lambda _: self.apply(event), range(8)))
        self.assertEqual(outcomes.count("applied"), 1)
        self.assertEqual(self.store.observe("s"), Snapshot(1, 10, 1, 1))

    def test_concurrent_versions_preserve_maximum(self):
        events = [Event("low", "s", 1, 10), Event("high", "s", 2, 20)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(self.apply, events))
        value = self.store.observe("s")
        self.assertEqual((value.version, value.value, value.receipts), (2, 20, 2))
        self.assertIn(value.effects, (1, 2))

    def test_streams_are_independent_and_new_version_counts_as_effect(self):
        self.apply(Event("a", "one", 1, 10))
        self.apply(Event("b", "two", 1, 20))
        self.apply(Event("c", "one", 2, 10))
        self.assertEqual(self.store.observe("one"), Snapshot(2, 10, 2, 2))
        self.assertEqual(self.store.observe("two"), Snapshot(1, 20, 1, 1))

    def test_maximum_profile_version_remains_observable(self):
        self.apply(Event("max", "s", 2 ** 31 - 1, 10))
        self.assertEqual(self.store.observe("s").version, 2 ** 31 - 1)

    def test_naive_control_reapplies_known_receipts(self):
        event = Event("a", "s", 1, 10)
        self.apply(event, naive=True)
        self.apply(event, naive=True)
        self.assertEqual(self.store.observe("s").effects, 2)
