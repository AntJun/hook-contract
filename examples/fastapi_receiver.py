"""Optional ASGI consumer with a separate transactional persistence implementation."""

import asyncio
import hashlib
import logging
import os
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path

from hookcontract.core import Event, InputError, MAX_BODY, authenticated


class RollbackProbe(RuntimeError):
    pass


def connect(path):
    connection = sqlite3.connect(path, timeout=2, isolation_level=None)
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=2000")
    return connection


def initialize(path):
    with closing(connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS accepted(
                id TEXT PRIMARY KEY, stream TEXT NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS heads(
                stream TEXT PRIMARY KEY, version INTEGER NOT NULL, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS changes(
                sequence INTEGER PRIMARY KEY, stream TEXT NOT NULL, event_id TEXT NOT NULL);
        """)


def persist(path, event, body, failpoint):
    digest = hashlib.sha256(body).hexdigest()
    with closing(connect(path)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            receipt = connection.execute(
                "SELECT digest FROM accepted WHERE id=?", (event.id,)).fetchone()
            if receipt is not None:
                connection.rollback()
                return 200 if receipt[0] == digest else 409
            head = connection.execute(
                "SELECT version,value FROM heads WHERE stream=?", (event.stream,)).fetchone()
            if head is not None and event.version == head[0] and event.value != head[1]:
                connection.rollback()
                return 409
            connection.execute("INSERT INTO accepted VALUES(?,?,?)",
                               (event.id, event.stream, digest))
            if failpoint == "rollback_after_receipt":
                raise RollbackProbe()
            if head is None or event.version > head[0]:
                connection.execute(
                    "INSERT INTO heads VALUES(?,?,?) ON CONFLICT(stream) DO UPDATE SET "
                    "version=excluded.version,value=excluded.value",
                    (event.stream, event.version, event.value))
                if failpoint == "rollback_after_state":
                    raise RollbackProbe()
                connection.execute("INSERT INTO changes(stream,event_id) VALUES(?,?)",
                                   (event.stream, event.id))
            connection.commit()
            return 200
        except BaseException:
            connection.rollback()
            raise


def create_app(path, key, *, enable_failpoints=False, body_timeout=2):
    from fastapi import FastAPI, Request, Response
    from starlette.concurrency import run_in_threadpool
    from starlette.requests import ClientDisconnect

    if not isinstance(key, bytes) or len(key) < 32:
        raise InputError("an in-memory fixture key is required")
    if type(body_timeout) not in (int, float) or not 0 < body_timeout <= 2:
        raise InputError("body deadline is outside the fixture range")
    initialize(path)
    app = FastAPI(debug=False, docs_url=None, redoc_url=None, openapi_url=None)
    attempts = {}
    lock = threading.Lock()

    def authorized(request, body):
        stamps = request.headers.getlist("x-contract-timestamp")
        digests = request.headers.getlist("x-contract-signature")
        return (len(stamps) == len(digests) == 1
                and authenticated(key, stamps[0], digests[0], body, time.time()))

    @app.get("/__contract/ready")
    async def ready(request: Request):
        return Response(status_code=204 if authorized(request, b"") else 401)

    @app.post("/hooks")
    async def hooks(request: Request):
        lengths = request.headers.getlist("content-length")
        if (request.headers.get("transfer-encoding") is not None or len(lengths) != 1
                or not lengths[0].isdigit() or len(lengths[0]) > 8):
            return Response(status_code=400)
        expected = int(lengths[0])
        if expected > MAX_BODY:
            return Response(status_code=413)
        body = bytearray()
        try:
            async with asyncio.timeout(body_timeout):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > min(MAX_BODY, expected):
                        return Response(status_code=413)
                    body.extend(chunk)
        except TimeoutError:
            return Response(status_code=408, headers={"Connection": "close"})
        except ClientDisconnect:
            return Response(status_code=400)
        body = bytes(body)
        if len(body) != expected:
            return Response(status_code=400)
        if not authorized(request, body):
            return Response(status_code=401)
        try:
            event = Event.parse(body)
        except InputError:
            return Response(status_code=400)
        fault = request.headers.get("x-contract-fault", "") if enable_failpoints else ""
        with lock:
            token = (event.id, fault)
            attempts[token] = attempts.get(token, 0) + 1
            attempt = attempts[token]
        if (fault == "retry_503" and attempt <= 2) or (fault == "retry_429" and attempt == 1):
            return Response(status_code=503 if fault == "retry_503" else 429,
                            headers={"Retry-After": "0"})
        failpoint = fault if attempt == 1 else None
        try:
            # Keep SQLite lock waits off the ASGI event loop.
            status = await run_in_threadpool(persist, path, event, body, failpoint)
        except (RollbackProbe, sqlite3.OperationalError):
            return Response(status_code=503, headers={"Retry-After": "0"})
        if status == 200 and fault == "crash_after_commit" and attempt == 1:
            os._exit(23)
        return Response(status_code=status)

    return app


def serve_process(path, key, listener, enable_failpoints, body_timeout):
    # Child-only logging suppression keeps raw requests and process details private.
    logging.disable(logging.CRITICAL)
    try:
        import uvicorn

        app = create_app(Path(path), key, enable_failpoints=enable_failpoints,
                         body_timeout=body_timeout)
        config = uvicorn.Config(app, loop="asyncio", http="h11", ws="none",
                                lifespan="on", access_log=False, log_config=None,
                                proxy_headers=False, server_header=False, date_header=False,
                                timeout_keep_alive=1, timeout_graceful_shutdown=1,
                                limit_concurrency=64)
        uvicorn.Server(config).run(sockets=[listener])
    except BaseException:
        # The parent verifies readiness and reports a fixed, sanitized startup error.
        os._exit(24)
    finally:
        listener.close()
