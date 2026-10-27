"""An isolated, authenticated HTTP fixture with real process restart support."""

from __future__ import annotations

import json
import multiprocessing
import os
import secrets
import sqlite3
import tempfile
import threading
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .core import EvidenceError, Event, HTTPClient, InputError, LABEL, MAX_BODY, authenticated
from .store import InjectedFailure, Store


def serve_process(path: str, key: bytes, ready, naive: bool):
    store = Store(Path(path))
    counts = {}
    counter_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, *_):
            pass

        def response(self, status, value=None, retry=False):
            body = json.dumps(value or {}, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if retry:
                self.send_header("Retry-After", "0")
            self.end_headers()
            self.wfile.write(body)

        def authorized(self, body):
            stamps = self.headers.get_all("X-Contract-Timestamp") or []
            digests = self.headers.get_all("X-Contract-Signature") or []
            return (len(stamps) == len(digests) == 1
                    and authenticated(key, stamps[0], digests[0], body, time.time()))

        def do_POST(self):
            if self.path != "/hooks":
                return self.response(404)
            lengths = self.headers.get_all("Content-Length") or []
            if (self.headers.get("Transfer-Encoding") is not None or len(lengths) != 1
                    or not lengths[0].isdigit() or len(lengths[0]) > 8):
                return self.response(400)
            size = int(lengths[0])
            if size > MAX_BODY:
                return self.response(413)
            body = self.rfile.read(size)
            if len(body) != size:
                return self.response(400)
            # Authentication precedes both parsing and known-ID acknowledgment.
            if not self.authorized(body):
                return self.response(401)
            try:
                event = Event.parse(body)
            except InputError:
                return self.response(400)
            fault = self.headers.get("X-Contract-Fault", "")
            with counter_lock:
                token = (event.id, fault)
                counts[token] = counts.get(token, 0) + 1
                attempt = counts[token]
            if fault == "retry_503" and attempt <= 2:
                return self.response(503, retry=True)
            if fault == "retry_429" and attempt == 1:
                return self.response(429, retry=True)
            failpoint = None
            if attempt == 1:
                if fault == "rollback_after_receipt":
                    failpoint = "after_receipt"
                elif fault == "rollback_after_state":
                    failpoint = "after_state"
            try:
                result = store.apply(event, body, naive=naive, failpoint=failpoint)
            except (InjectedFailure, sqlite3.OperationalError):
                return self.response(503, retry=True)
            if result == "conflict":
                return self.response(409)
            if fault == "crash_after_commit":
                # The database commit is complete; no response bytes are sent.
                os._exit(23)
            return self.response(200)

        def do_GET(self):
            if not self.authorized(b""):
                return self.response(401)
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            values = query.get("stream", [])
            if (parsed.path != "/__contract/state" or set(query) != {"stream"}
                    or len(values) != 1 or not LABEL.fullmatch(values[0])):
                return self.response(400)
            try:
                return self.response(200, asdict(store.observe(values[0])))
            except sqlite3.OperationalError:
                return self.response(503, retry=True)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    ready.send(server.server_address[1])
    ready.close()
    server.serve_forever(poll_interval=0.05)


class ManagedTarget:
    """Owns only its spawned fixture process and fresh temporary database."""

    def __init__(self, *, naive=False):
        self.naive = naive
        self.key = secrets.token_bytes(32)
        self.temporary = None
        self.process = None
        self.client = None
        self.restarts = 0

    def __enter__(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hookcontract-")
        self.path = str(Path(self.temporary.name) / "fixture.sqlite")
        try:
            self.start()
        except BaseException:
            self.__exit__()
            raise
        return self

    def start(self):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe(duplex=False)
        self.process = context.Process(target=serve_process,
                                       args=(self.path, self.key, child, self.naive), daemon=True)
        self.process.start()
        child.close()
        try:
            if not parent.poll(10):
                raise EvidenceError("fixture startup deadline exceeded")
            port = parent.recv()
            if type(port) is not int or not 1 <= port <= 65535:
                raise EvidenceError("fixture startup evidence is invalid")
            self.client = HTTPClient(f"http://127.0.0.1:{port}", self.key)
        except (EOFError, OSError) as exc:
            raise EvidenceError("fixture failed to start") from exc
        finally:
            parent.close()

    def stop(self):
        if self.process is not None and self.process.pid is not None:
            if self.process.is_alive():
                self.process.terminate()
            self.process.join(3)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(3)
            if self.process.is_alive():
                raise EvidenceError("fixture stop deadline exceeded")

    def restart(self, *, after_crash=False):
        if after_crash:
            self.process.join(2)
            if self.process.exitcode != 23:
                raise EvidenceError("post-commit crash was not confirmed")
        self.stop()
        self.start()
        self.restarts += 1
        return True

    def send(self, *args, **kwargs):
        return self.client.send(*args, **kwargs)

    def send_raw(self, *args, **kwargs):
        return self.client.send_raw(*args, **kwargs)

    def observe(self, stream):
        return self.client.observe(stream)

    def __exit__(self, *_):
        try:
            self.stop()
        finally:
            if self.temporary is not None:
                self.temporary.cleanup()
