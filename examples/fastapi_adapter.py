"""Process lifecycle and independent read-only observation for the ASGI example."""

from __future__ import annotations

import http.client
import multiprocessing
import secrets
import socket
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

from hookcontract.core import EvidenceError, HTTPClient, InputError, LABEL, Snapshot, signature

from .fastapi_receiver import serve_process


class FastAPITarget:
    def __init__(self, *, enable_failpoints=True, body_timeout=2, startup_timeout=10):
        if type(startup_timeout) not in (int, float) or not 0.05 <= startup_timeout <= 15:
            raise InputError("startup deadline is outside the fixture range")
        self.key = secrets.token_bytes(32)
        self.enable_failpoints = enable_failpoints
        self.body_timeout = body_timeout
        self.startup_timeout = startup_timeout
        self.process = None
        self.temporary = None
        self.client = None
        self.path = None

    def __enter__(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hookcontract-asgi-")
        self.path = Path(self.temporary.name) / "consumer.sqlite"
        try:
            self.start()
        except BaseException:
            self.__exit__()
            raise
        return self

    def start(self):
        if self.process is not None:
            raise EvidenceError("a receiver process already exists")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
            listener.listen(64)
            listener.setblocking(False)
            context = multiprocessing.get_context("spawn")
            self.process = context.Process(
                target=serve_process,
                args=(str(self.path), self.key, listener,
                      self.enable_failpoints, self.body_timeout), daemon=True)
            self.process.start()
        finally:
            listener.close()
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if not self.process.is_alive():
                raise EvidenceError("ASGI receiver failed to start")
            if self.is_ready(min(0.2, max(0.001, deadline - time.monotonic()))):
                self.client = HTTPClient(f"http://127.0.0.1:{self.port}", self.key)
                return
            time.sleep(min(0.02, max(0, deadline - time.monotonic())))
        raise EvidenceError("ASGI receiver startup deadline exceeded")

    def is_ready(self, timeout):
        stamp = int(time.time())
        headers = {"X-Contract-Timestamp": str(stamp),
                   "X-Contract-Signature": signature(self.key, stamp, b""),
                   "Connection": "close"}
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            connection.request("GET", "/__contract/ready", headers=headers)
            reply = connection.getresponse()
            return reply.status == 204 and reply.read(1) == b""
        except (OSError, http.client.HTTPException):
            return False
        finally:
            connection.close()

    def stop(self):
        if self.process is None:
            return
        if self.process.pid is not None:
            if self.process.is_alive():
                self.process.terminate()
            self.process.join(3)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(3)
            if self.process.is_alive():
                raise EvidenceError("ASGI receiver stop deadline exceeded")
        self.process.close()
        self.process = None
        self.client = None

    def restart(self, *, after_crash=False):
        if self.process is None:
            raise EvidenceError("ASGI receiver process is unavailable")
        if after_crash:
            self.process.join(2)
            if self.process.exitcode != 23:
                raise EvidenceError("post-commit ASGI process exit was not confirmed")
        self.stop()
        self.start()
        return True

    def send(self, *args, **kwargs):
        return self.client.send(*args, **kwargs)

    def send_raw(self, body):
        return self.client.send_raw(body)

    def observe(self, stream):
        if not isinstance(stream, str) or not LABEL.fullmatch(stream):
            raise InputError("invalid observation stream")
        # A read-only connection in the harness supplies independent durable evidence.
        uri = self.path.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2, isolation_level=None)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            head = connection.execute(
                "SELECT version,value FROM heads WHERE stream=?", (stream,)).fetchone()
            effects = connection.execute(
                "SELECT count(*) FROM changes WHERE stream=?", (stream,)).fetchone()[0]
            receipts = connection.execute(
                "SELECT count(*) FROM accepted WHERE stream=?", (stream,)).fetchone()[0]
            connection.commit()
        return Snapshot(head[0] if head else 0, head[1] if head else 0, effects, receipts)

    def __exit__(self, *_):
        self.stop()
        if self.temporary is not None:
            self.temporary.cleanup()
            self.temporary = None


def target():
    return FastAPITarget()
