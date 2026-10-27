"""Versioned fixture models, raw-body authentication, and bounded delivery."""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import re
import socket
import threading
import time
from dataclasses import asdict, dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode, urlsplit

MAX_BODY = 65536
LABEL = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")


class InputError(ValueError):
    pass


class EvidenceError(RuntimeError):
    pass


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise InputError("duplicate JSON key")
        result[key] = value
    return result


def _constant(_):
    raise InputError("nonfinite JSON")


def decode_json(body: bytes):
    if len(body) > MAX_BODY:
        raise InputError("body exceeds the fixture limit")
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                          parse_constant=_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise InputError("invalid UTF-8 JSON") from exc


@dataclass(frozen=True)
class Event:
    id: str
    stream: str
    version: int
    value: int

    def __post_init__(self):
        if any(not isinstance(v, str) or not LABEL.fullmatch(v) for v in (self.id, self.stream)):
            raise InputError("event labels require 1-64 safe ASCII characters")
        if type(self.version) is not int or not 1 <= self.version <= 2 ** 31 - 1:
            raise InputError("version is outside the fixture range")
        if type(self.value) is not int or abs(self.value) > 1_000_000:
            raise InputError("value is outside the fixture range")

    def body(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()

    @classmethod
    def parse(cls, body: bytes):
        value = decode_json(body)
        if not isinstance(value, dict) or set(value) != {"id", "stream", "version", "value"}:
            raise InputError("event fields do not match durable-snapshot-v1")
        event = cls(**value)
        if body != event.body():
            raise InputError("event body requires canonical JSON encoding")
        return event


@dataclass(frozen=True)
class Snapshot:
    version: int = 0
    value: int = 0
    effects: int = 0
    receipts: int = 0
    settled: bool = True

    def __post_init__(self):
        if type(self.settled) is not bool:
            raise EvidenceError("observer settling state is invalid")
        if type(self.version) is not int or not 0 <= self.version <= 2 ** 31 - 1:
            raise EvidenceError("observer version is invalid")
        for value in (self.effects, self.receipts):
            if type(value) is not int or not 0 <= value <= 1_000_000:
                raise EvidenceError("observer counters are invalid")
        if type(self.value) is not int or abs(self.value) > 1_000_000:
            raise EvidenceError("observer value is invalid")

    @classmethod
    def parse(cls, body):
        value = decode_json(body)
        if not isinstance(value, dict) or set(value) != {"version", "value", "effects", "receipts", "settled"}:
            raise EvidenceError("observer schema is incomplete")
        if type(value["settled"]) is not bool:
            raise EvidenceError("observer settling state is invalid")
        for name in ("version", "effects", "receipts"):
            if type(value[name]) is not int or value[name] < 0:
                raise EvidenceError("observer counters are invalid")
        if type(value["value"]) is not int:
            raise EvidenceError("observer value is invalid")
        return cls(**value)


def signature(key: bytes, stamp: int, body: bytes) -> str:
    return hmac.new(key, b"hookcontract-v1\n" + str(stamp).encode("ascii") + b"\n" + body, hashlib.sha256).hexdigest()


def authenticated(key: bytes, stamp: str, supplied: str, body: bytes, now: float) -> bool:
    if not isinstance(stamp, str) or not re.fullmatch(r"[0-9]{1,12}", stamp):
        return False
    if stamp != str(int(stamp)) or abs(now - int(stamp)) > 300:
        return False
    if not isinstance(supplied, str) or not re.fullmatch(r"[0-9a-f]{64}", supplied):
        return False
    return hmac.compare_digest(signature(key, int(stamp), body), supplied)


@dataclass(frozen=True)
class Reply:
    status: int | None
    elapsed_ms: float = 0
    retry_after: str | None = None

    def __post_init__(self):
        if self.status is not None and (type(self.status) is not int or not 100 <= self.status <= 599):
            raise EvidenceError("HTTP evidence has an invalid status")
        if (isinstance(self.elapsed_ms, bool) or not isinstance(self.elapsed_ms, (int, float))
                or not 0 <= self.elapsed_ms <= 1_000_000):
            raise EvidenceError("HTTP evidence has an invalid elapsed duration")


class HTTPClient:
    """Literal loopback HTTP only; redirect responses are returned to the caller."""

    def __init__(self, base_url: str, key: bytes, timeout: float = 2):
        try:
            parsed = urlsplit(base_url)
            valid = (parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "::1")
                     and parsed.port is not None and 1 <= parsed.port <= 65535
                     and parsed.path in ("", "/") and not parsed.query and not parsed.fragment
                     and parsed.username is None and parsed.password is None)
        except ValueError as exc:
            raise InputError("invalid loopback URL") from exc
        if not valid:
            raise InputError("a literal loopback HTTP URL and explicit port are required")
        if not isinstance(key, bytes) or len(key) < 32:
            raise InputError("the in-memory fixture key requires at least 32 bytes")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 0 < timeout <= 10:
            raise InputError("timeout must be greater than zero and at most 10 seconds")
        self.host, self.port, self.key, self.timeout = parsed.hostname, parsed.port, key, timeout

    def _request(self, method, path, body=b"", *, fault=None, tamper=False, age=0):
        stamp = int(time.time()) + age
        digest = signature(self.key, stamp, body)
        if tamper:
            digest = ("0" if digest[0] != "0" else "1") + digest[1:]
        headers = {"X-Contract-Timestamp": str(stamp), "X-Contract-Signature": digest,
                   "Content-Type": "application/json", "Content-Length": str(len(body))}
        if fault:
            headers["X-Contract-Fault"] = fault
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        started = time.perf_counter()
        timer = None
        expired = threading.Event()
        try:
            connection.connect()
            remaining = self.timeout - (time.perf_counter() - started)
            if remaining <= 0:
                raise TimeoutError()
            connected_socket = connection.sock

            def expire():
                expired.set()
                try:
                    connected_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            timer = threading.Timer(remaining, expire)
            timer.daemon = True
            timer.start()
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            expected_length = response.length
            payload = response.read(MAX_BODY + 1)
            if expired.is_set() or time.perf_counter() - started > self.timeout:
                raise TimeoutError()
            if expected_length is not None and len(payload) != expected_length:
                raise http.client.IncompleteRead(payload)
            if len(payload) > MAX_BODY:
                raise EvidenceError("response exceeds the fixture limit")
            return Reply(response.status, (time.perf_counter() - started) * 1000,
                         response.getheader("Retry-After")), payload
        except (OSError, http.client.HTTPException):
            return Reply(None, (time.perf_counter() - started) * 1000), b""
        finally:
            if timer is not None:
                timer.cancel()
            connection.close()

    def send(self, event: Event, *, fault=None, tamper=False, age=0) -> Reply:
        return self._request("POST", "/hooks", event.body(), fault=fault, tamper=tamper, age=age)[0]

    def send_raw(self, body: bytes) -> Reply:
        return self._request("POST", "/hooks", body)[0]

    def observe(self, stream: str) -> Snapshot:
        if not isinstance(stream, str) or not LABEL.fullmatch(stream):
            raise InputError("invalid stream label")
        response, body = self._request("GET", "/__contract/state?" + urlencode({"stream": stream}))
        if response.status != 200:
            raise EvidenceError("durable observer is unavailable")
        return Snapshot.parse(body)


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    base_delay: float = 0.01
    max_delay: float = 0.1
    max_total_delay: float = 1
    max_elapsed: float = 10

    def __post_init__(self):
        if type(self.attempts) is not int or not 1 <= self.attempts <= 6:
            raise InputError("retry attempts must be between 1 and 6")
        for value in (self.base_delay, self.max_delay, self.max_total_delay, self.max_elapsed):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 10:
                raise InputError("retry delays require finite bounds of at most 10 seconds")
        if self.max_elapsed <= 0:
            raise InputError("elapsed deadline must be positive")
        if self.base_delay > self.max_delay:
            raise InputError("base delay exceeds maximum delay")


def retry_delay(header: str | None, attempt: int, policy: RetryPolicy, now: float) -> float:
    if header is None:
        return min(policy.max_delay, policy.base_delay * 2 ** attempt)
    if len(header) > 100:
        raise EvidenceError("Retry-After is invalid")
    if re.fullmatch(r"[0-9]+", header):
        delay = float(int(header))
    else:
        try:
            date = parsedate_to_datetime(header)
            if date.tzinfo is None:
                raise ValueError()
            delay = max(0, date.timestamp() - now)
        except (ValueError, OverflowError, TypeError) as exc:
            raise EvidenceError("Retry-After is invalid") from exc
    if delay > policy.max_total_delay:
        raise EvidenceError("Retry-After exceeds the retry budget")
    return delay


@dataclass(frozen=True)
class Delivery:
    accepted: bool
    replies: tuple[Reply, ...]
    delays: tuple[float, ...]
    reason: str


def deliver(client, event, *, fault=None, policy=None, sleep=time.sleep, now=time.time,
            monotonic=time.monotonic) -> Delivery:
    policy = policy or RetryPolicy()
    replies, delays = [], []
    started = monotonic()
    for attempt in range(policy.attempts):
        # A new attempt timestamp and HMAC are generated by each send call.
        reply = client.send(event, fault=fault)
        replies.append(reply)
        if monotonic() - started > policy.max_elapsed:
            raise EvidenceError("delivery deadline exceeded")
        if reply.status is not None and 200 <= reply.status < 300:
            return Delivery(True, tuple(replies), tuple(delays), "accepted")
        if reply.status is not None and reply.status not in (408, 425, 429, 500, 502, 503, 504):
            return Delivery(False, tuple(replies), tuple(delays), "permanent_response")
        if attempt + 1 == policy.attempts:
            break
        delay = retry_delay(reply.retry_after, attempt, policy, now())
        if sum(delays) + delay > policy.max_total_delay:
            raise EvidenceError("total retry delay exceeds the budget")
        delays.append(delay)
        sleep(delay)
    return Delivery(False, tuple(replies), tuple(delays), "attempt_limit")
