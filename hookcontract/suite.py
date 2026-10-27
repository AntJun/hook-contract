"""Observable contract scenarios; missing evidence takes precedence over failures."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

from .core import Delivery, EvidenceError, Event, Reply, Snapshot, deliver


class Target(Protocol):
    def send(self, event: Event, *, fault=None, tamper=False, age=0) -> Reply: ...
    def send_raw(self, body: bytes) -> Reply: ...
    def observe(self, stream: str) -> Snapshot: ...
    def restart(self, *, after_crash=False) -> bool: ...


class Violation(AssertionError):
    pass


class Case:
    def __init__(self, target):
        self.target = target
        self.requests = []
        self.observations = []
        self.restarts = 0
        self.lock = threading.Lock()

    def record(self, reply):
        if not isinstance(reply, Reply):
            raise EvidenceError("delivery adapter supplied no typed HTTP evidence")
        reply = Reply(reply.status, reply.elapsed_ms, reply.retry_after)
        with self.lock:
            self.requests.append({"status": reply.status,
                                  "outcome": "response" if reply.status else "transport_ambiguous",
                                  "elapsed_ms": round(reply.elapsed_ms, 3)})
        return reply

    def send(self, *args, **kwargs):
        return self.record(self.target.send(*args, **kwargs))

    def raw(self, body):
        return self.record(self.target.send_raw(body))

    def accepted(self, event, *, fault=None):
        result = deliver(self, event, fault=fault)
        if all(reply.status is None for reply in result.replies):
            raise EvidenceError("no HTTP response was observed")
        require(result.accepted, "delivery was not accepted within the sender policy")
        return result

    def observe(self, stream):
        value = self.target.observe(stream)
        if not isinstance(value, Snapshot):
            raise EvidenceError("durable observer supplied no typed state")
        # Adapter extensions cannot add private fields to public observations.
        approved = Snapshot(value.version, value.value, value.effects, value.receipts, value.settled)
        if not approved.settled:
            raise EvidenceError("durable observer has not confirmed settled state")
        return approved

    def expect(self, stream, version=0, value=0, effects=0, receipts=0):
        actual = self.observe(stream)
        allowed = effects if isinstance(effects, tuple) else (effects,)
        expected = {"version": version, "value": value, "effects": list(allowed), "receipts": receipts}
        observed = {name: getattr(actual, name)
                    for name in ("version", "value", "effects", "receipts", "settled")}
        self.observations.append({"expected": expected, "observed": observed})
        require(actual.version == version and actual.value == value
                and actual.effects in allowed and actual.receipts == receipts,
                "committed state differs from the contract")

    def restart(self, *, after_crash=False):
        if self.target.restart(after_crash=after_crash) is not True:
            raise EvidenceError("receiver restart was not confirmed")
        self.restarts += 1


def require(condition, reason):
    if not condition:
        raise Violation(reason)


def event(name, version=1, value=10, *, id=None):
    return Event(id or name, name, version, value)


def valid(c):
    c.accepted(event("valid"))
    c.expect("valid", 1, 10, 1, 1)


def duplicate(c):
    e = event("duplicate")
    c.accepted(e)
    c.accepted(e)
    c.expect("duplicate", 1, 10, 1, 1)


def concurrent_duplicate(c):
    e = event("concurrent")
    with ThreadPoolExecutor(max_workers=4) as pool:
        replies = list(pool.map(lambda _: c.send(e), range(4)))
    require(all(reply.status == 200 for reply in replies), "concurrent acknowledgments differ")
    c.expect("concurrent", 1, 10, 1, 1)


def lost_ack_restart(c):
    e = event("lost_ack")
    reply = c.send(e, fault="crash_after_commit")
    require(reply.status is None, "post-commit failure produced an acknowledgment")
    c.restart(after_crash=True)
    c.accepted(e)
    c.expect("lost_ack", 1, 10, 1, 1)


def retry_503(c):
    result = c.accepted(event("retry_503"), fault="retry_503")
    require([r.status for r in result.replies] == [503, 503, 200], "transient response sequence differs")
    c.expect("retry_503", 1, 10, 1, 1)


def retry_429(c):
    result = c.accepted(event("retry_429"), fault="retry_429")
    require([r.status for r in result.replies] == [429, 200], "rate-limit recovery sequence differs")
    c.expect("retry_429", 1, 10, 1, 1)


def out_of_order(c):
    c.accepted(event("ordering", 3, 30, id="ordering_new"))
    c.accepted(event("ordering", 2, 20, id="ordering_old"))
    c.expect("ordering", 3, 30, 1, 2)


def id_conflict(c):
    c.accepted(event("id_conflict"))
    require(c.send(event("id_conflict", 2, 20)).status == 409, "conflicting event ID was accepted")
    c.expect("id_conflict", 1, 10, 1, 1)


def version_conflict(c):
    c.accepted(event("version_conflict", id="version_a"))
    require(c.send(event("version_conflict", 1, 20, id="version_b")).status == 409,
            "current-version collision was accepted")
    c.expect("version_conflict", 1, 10, 1, 1)


def invalid_signature(c):
    require(c.send(event("invalid_mac"), tamper=True).status == 401, "invalid signature was accepted")
    c.expect("invalid_mac")


def expired_signature(c):
    require(c.send(event("expired"), age=-600).status == 401, "expired signature was accepted")
    c.expect("expired")


def known_id_invalid_signature(c):
    e = event("known_mac")
    c.accepted(e)
    require(c.send(e, tamper=True).status == 401, "known ID bypassed authentication")
    c.expect("known_mac", 1, 10, 1, 1)


def malformed(c):
    require(c.raw(b"{").status == 400, "malformed authenticated JSON was accepted")
    c.expect("malformed")


def durable_applied_id(c):
    e = event("durable_applied")
    c.accepted(e)
    c.restart()
    require(c.send(event("durable_applied", 2, 20)).status == 409, "accepted ID did not survive restart")
    c.expect("durable_applied", 1, 10, 1, 1)


def durable_stale_id(c):
    c.accepted(event("durable_stale", 3, 30, id="stale_head"))
    c.accepted(event("durable_stale", 2, 20, id="stale_receipt"))
    c.restart()
    require(c.send(event("durable_stale", 4, 40, id="stale_receipt")).status == 409,
            "accepted stale ID did not survive restart")
    c.expect("durable_stale", 3, 30, 1, 2)


def durable_redundant_id(c):
    c.accepted(event("durable_redundant", id="redundant_head"))
    c.accepted(event("durable_redundant", id="redundant_receipt"))
    c.restart()
    require(c.send(event("durable_redundant", 2, 20, id="redundant_receipt")).status == 409,
            "accepted redundant ID did not survive restart")
    c.expect("durable_redundant", 1, 10, 1, 2)


def rollback_receipt(c):
    e = event("rollback_receipt")
    require(c.send(e, fault="rollback_after_receipt").status == 503, "receipt failpoint did not fire")
    c.expect("rollback_receipt")
    c.accepted(e)
    c.expect("rollback_receipt", 1, 10, 1, 1)


def rollback_state(c):
    e = event("rollback_state")
    require(c.send(e, fault="rollback_after_state").status == 503, "state failpoint did not fire")
    c.expect("rollback_state")
    c.accepted(e)
    c.expect("rollback_state", 1, 10, 1, 1)


def concurrent_versions(c):
    values = [event("version_race", 1, 10, id="race_low"),
              event("version_race", 2, 20, id="race_high")]
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(c.send, values))
    require(all(r.status == 200 for r in replies), "version race was not acknowledged")
    c.expect("version_race", 2, 20, (1, 2), 2)


def same_version_race(c):
    values = [event("collision_race", 1, 10, id="collision_a"),
              event("collision_race", 1, 20, id="collision_b")]
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(c.send, values))
    require(sorted(r.status or 0 for r in replies) == [200, 409], "version race has no unique winner")
    winner = next(event for event, reply in zip(values, replies) if reply.status == 200)
    c.expect("collision_race", 1, winner.value, 1, 1)


def newer_same_value(c):
    c.accepted(event("newer_same", 1, 10, id="same_low"))
    c.accepted(event("newer_same", 2, 10, id="same_high"))
    c.expect("newer_same", 2, 10, 2, 2)


SCENARIOS = (
    valid, duplicate, concurrent_duplicate, lost_ack_restart, retry_503, retry_429,
    out_of_order, id_conflict, version_conflict, invalid_signature, expired_signature,
    known_id_invalid_signature, malformed, durable_applied_id, durable_stale_id,
    durable_redundant_id, rollback_receipt, rollback_state, concurrent_versions,
    same_version_race, newer_same_value,
)


def run_suite(target: Target) -> dict:
    results = []
    for scenario in SCENARIOS:
        case = Case(target)
        status, reason = "pass", "observed state satisfies this scenario"
        try:
            scenario(case)
        except Violation:
            status, reason = "fail", "observed behavior violates this scenario"
        except Exception:
            status, reason = "error", "required delivery, observation, or restart evidence is unavailable"
        results.append({"id": scenario.__name__, "status": status, "reason": reason,
                        "requests": case.requests, "observations": case.observations,
                        "confirmed_restarts": case.restarts})
    counts = {name: sum(row["status"] == name for row in results) for name in ("pass", "fail", "error")}
    code = 2 if counts["error"] else 1 if counts["fail"] else 0
    return {"schema_version": 1, "profile": "durable-snapshot-v1", "tool_version": "0.1.0",
            "status": "error" if code == 2 else "fail" if code == 1 else "pass",
            "exit_code": code, "summary": counts, "results": results}
