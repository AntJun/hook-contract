"""Atomic accepted-receipt, snapshot, and effect storage for the fixture."""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

from .core import Event, Snapshot


class InjectedFailure(RuntimeError):
    pass


class Store:
    def __init__(self, path: Path):
        self.path = path
        with closing(self.connect()) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS receipts(
                    event_id TEXT PRIMARY KEY, stream TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, outcome TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS snapshots(
                    stream TEXT PRIMARY KEY, version INTEGER NOT NULL, value INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS effects(
                    n INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT NOT NULL,
                    event_id TEXT NOT NULL);
            """)

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=2000")
        return connection

    def apply(self, event: Event, body: bytes, *, naive=False, failpoint=None) -> str:
        fingerprint = hashlib.sha256(body).hexdigest()
        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                receipt = connection.execute(
                    "SELECT fingerprint FROM receipts WHERE event_id=?", (event.id,)).fetchone()
                if receipt:
                    if receipt[0] != fingerprint:
                        connection.rollback()
                        return "conflict"
                    if not naive:
                        connection.commit()
                        return "duplicate"
                head = connection.execute(
                    "SELECT version,value FROM snapshots WHERE stream=?", (event.stream,)).fetchone()
                if head and event.version == head[0] and event.value != head[1]:
                    connection.rollback()
                    return "conflict"
                outcome = ("applied" if naive or head is None or event.version > head[0]
                           else "stale" if event.version < head[0] else "redundant")
                if not receipt:
                    connection.execute("INSERT INTO receipts VALUES(?,?,?,?)",
                                       (event.id, event.stream, fingerprint, outcome))
                if failpoint == "after_receipt":
                    raise InjectedFailure()
                if outcome == "applied":
                    connection.execute(
                        "INSERT INTO snapshots VALUES(?,?,?) ON CONFLICT(stream) DO UPDATE SET "
                        "version=excluded.version,value=excluded.value",
                        (event.stream, event.version, event.value))
                    if failpoint == "after_state":
                        raise InjectedFailure()
                    connection.execute("INSERT INTO effects(stream,event_id) VALUES(?,?)",
                                       (event.stream, event.id))
                connection.commit()
                return outcome
            except BaseException:
                connection.rollback()
                raise

    def observe(self, stream: str) -> Snapshot:
        with closing(self.connect()) as connection:
            # One read transaction supplies a consistent independent observation.
            connection.execute("BEGIN")
            head = connection.execute(
                "SELECT version,value FROM snapshots WHERE stream=?", (stream,)).fetchone()
            effects = connection.execute(
                "SELECT count(*) FROM effects WHERE stream=?", (stream,)).fetchone()[0]
            receipts = connection.execute(
                "SELECT count(*) FROM receipts WHERE stream=?", (stream,)).fetchone()[0]
            connection.commit()
            return Snapshot(head[0] if head else 0, head[1] if head else 0, effects, receipts)
