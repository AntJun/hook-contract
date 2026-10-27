import json
import math
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime

from hookcontract.core import (
    Delivery, EvidenceError, Event, HTTPClient, InputError, Reply, RetryPolicy,
    Snapshot, authenticated, decode_json, deliver, retry_delay, signature,
)

KEY = bytes(range(32))


class EventTests(unittest.TestCase):
    def test_canonical_round_trip(self):
        event = Event("evt_1", "stream_1", 1, 10)
        self.assertEqual(Event.parse(event.body()), event)
        self.assertEqual(event.body(), b'{"id":"evt_1","stream":"stream_1","value":10,"version":1}')

    def test_noncanonical_and_duplicate_fields_rejected(self):
        event = Event("evt_1", "stream_1", 1, 10)
        samples = [json.dumps(event.__dict__).encode(),
                   b'{"id":"x","id":"y","stream":"s","version":1,"value":1}',
                   b'{"id":"x","stream":"s","version":1,"value":1,"extra":1}']
        for body in samples:
            with self.subTest(body=body), self.assertRaises(InputError):
                Event.parse(body)

    def test_invalid_event_values(self):
        for values in (("", "s", 1, 1), ("x", "../s", 1, 1), ("x", "s", True, 1),
                       ("x", "s", 0, 1), ("x", "s", 2 ** 63, 1),
                       ("x", "s", 1, False), ("x", "s", 1, math.inf)):
            with self.subTest(values=values), self.assertRaises(InputError):
                Event(*values)

    def test_invalid_json_and_body_limit(self):
        for body in (b"\xff", b"{", b"NaN", b"[" * 2000, b"x" * 65537):
            with self.subTest(length=len(body)), self.assertRaises(InputError):
                decode_json(body)

    def test_observer_requires_complete_typed_state(self):
        with self.assertRaises(EvidenceError):
            Snapshot.parse(b"{}")
        with self.assertRaises(EvidenceError):
            Snapshot(effects=True)
        with self.assertRaises(EvidenceError):
            Snapshot(value="untrusted")
        self.assertEqual(Snapshot.parse(b'{"effects":0,"receipts":0,"settled":true,"value":0,"version":0}'),
                         Snapshot())


class AuthenticationTests(unittest.TestCase):
    def test_valid_signature_and_changed_body(self):
        body = Event("e", "s", 1, 1).body()
        digest = signature(KEY, 1000, body)
        self.assertTrue(authenticated(KEY, "1000", digest, body, 1000))
        self.assertFalse(authenticated(KEY, "1000", digest, body + b" ", 1000))

    def test_fresh_attempt_timestamp_changes_signature(self):
        body = Event("e", "s", 1, 1).body()
        self.assertNotEqual(signature(KEY, 1000, body), signature(KEY, 1001, body))

    def test_timestamp_window_boundaries(self):
        body = b"test"
        for stamp, accepted in ((700, True), (1300, True), (699, False), (1301, False)):
            self.assertEqual(authenticated(KEY, str(stamp), signature(KEY, stamp, body), body, 1000),
                             accepted)

    def test_malformed_and_wrong_signatures(self):
        body = b"test"
        digest = signature(KEY, 1000, body)
        for stamp, value in (("001000", digest), ("-1", digest), ("1000", "x" * 64),
                             ("1000", "0" * 64), ("1000", digest.upper())):
            self.assertFalse(authenticated(KEY, stamp, value, body, 1000))


class SenderTests(unittest.TestCase):
    def test_url_is_literal_loopback_with_explicit_port(self):
        for url in ("https://127.0.0.1:10", "http://localhost:10", "http://example.com:10",
                    "http://127.0.0.1", "http://127.0.0.1:10/path",
                    "http://user:pass@127.0.0.1:10", "http://127.0.0.1:10?x=1"):
            with self.subTest(url=url), self.assertRaises(InputError):
                HTTPClient(url, KEY)
        HTTPClient("http://127.0.0.1:12345", KEY)
        HTTPClient("http://[::1]:12345", KEY)

    def test_retry_policy_validation(self):
        for kwargs in ({"attempts": True}, {"attempts": 0}, {"attempts": 7},
                       {"base_delay": math.nan}, {"max_delay": math.inf},
                       {"base_delay": 1, "max_delay": 0.5}):
            with self.subTest(kwargs=kwargs), self.assertRaises(InputError):
                RetryPolicy(**kwargs)

    def test_retry_after_seconds_and_http_date(self):
        policy = RetryPolicy()
        self.assertEqual(retry_delay("0", 0, policy, 1000), 0)
        date = format_datetime(datetime.fromtimestamp(1001, timezone.utc), usegmt=True)
        self.assertEqual(retry_delay(date, 0, policy, 1000), 1)
        self.assertEqual(retry_delay(None, 2, policy, 1000), 0.04)

    def test_invalid_and_excessive_retry_after(self):
        for value in ("-1", "1.5", "invalid", "1000000", "x" * 101):
            with self.subTest(value=value), self.assertRaises(EvidenceError):
                retry_delay(value, 0, RetryPolicy(), 1000)

    def test_retries_preserve_event_and_stop_on_acceptance(self):
        replies = iter([Reply(503), Reply(429, retry_after="0"), Reply(200)])
        bodies = []
        class Client:
            def send(self, event, **_):
                bodies.append(event.body())
                return next(replies)
        sleeps = []
        result = deliver(Client(), Event("e", "s", 1, 1), sleep=sleeps.append)
        self.assertTrue(result.accepted)
        self.assertEqual(len(set(bodies)), 1)
        self.assertEqual(sleeps, [0.01, 0])

    def test_permanent_and_redirect_responses_stop(self):
        for status in (400, 401, 409, 301, 302, 307, 308):
            class Client:
                def send(self, *_args, **_kwargs):
                    return Reply(status)
            result = deliver(Client(), Event("e", "s", 1, 1), sleep=lambda _: None)
            self.assertFalse(result.accepted)
            self.assertEqual(len(result.replies), 1)

    def test_exhaustion_and_transport_ambiguity_are_bounded(self):
        class Client:
            def send(self, *_args, **_kwargs):
                return Reply(None)
        result = deliver(Client(), Event("e", "s", 1, 1), sleep=lambda _: None)
        self.assertEqual(result.reason, "attempt_limit")
        self.assertEqual(len(result.replies), 3)

    def test_elapsed_deadline_and_total_delay_budget(self):
        class Client:
            def send(self, *_args, **_kwargs):
                return Reply(503)
        times = iter([0, 11])
        with self.assertRaises(EvidenceError):
            deliver(Client(), Event("e", "s", 1, 1), monotonic=lambda: next(times))
        with self.assertRaises(EvidenceError):
            deliver(Client(), Event("e", "s", 1, 1),
                    policy=RetryPolicy(base_delay=0.1, max_total_delay=0.05), sleep=lambda _: None)

    def test_backoff_cannot_issue_a_request_after_deadline(self):
        clock, calls = [0.0], []
        class Client:
            def send(self, event, **kwargs):
                calls.append(kwargs["timeout"])
                return Reply(503)
        def oversleep(delay):
            clock[0] += delay + 0.1
        with self.assertRaises(EvidenceError):
            deliver(Client(), Event("e", "s", 1, 1),
                    policy=RetryPolicy(max_elapsed=0.05), sleep=oversleep,
                    monotonic=lambda: clock[0])
        self.assertEqual(len(calls), 1)
        self.assertLessEqual(calls[0], 0.05)

    def test_delay_larger_than_remaining_deadline_is_not_slept(self):
        calls, sleeps = [], []
        class Client:
            def send(self, *_args, **kwargs):
                calls.append(kwargs["timeout"])
                return Reply(503)
        with self.assertRaises(EvidenceError):
            deliver(Client(), Event("e", "s", 1, 1),
                    policy=RetryPolicy(base_delay=0.1, max_elapsed=0.05),
                    sleep=sleeps.append, monotonic=lambda: 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleeps, [])

    def test_reply_evidence_rejects_untrusted_types(self):
        for kwargs in ({"status": True}, {"status": "private"}, {"status": 700},
                       {"status": 200, "elapsed_ms": math.nan}):
            with self.assertRaises(EvidenceError):
                Reply(**kwargs)
