import http.client
import importlib.util
import json
import multiprocessing
import os
import socket
import threading
import time
import unittest
from unittest.mock import patch

from hookcontract.core import EvidenceError, Event, MAX_BODY, Snapshot, signature
from hookcontract.suite import run_suite

AVAILABLE = (importlib.util.find_spec("fastapi") is not None
             and importlib.util.find_spec("uvicorn") is not None)
if AVAILABLE:
    from examples.fastapi_adapter import FastAPITarget


def exit_before_ready(*_):
    os._exit(24)


def never_ready(*_):
    threading.Event().wait(30)


@unittest.skipUnless(AVAILABLE, "optional FastAPI dependencies are installed in the ASGI CI matrix")
class FastAPIIntegrationTests(unittest.TestCase):
    def test_all_contract_scenarios_and_confirmed_restarts(self):
        with FastAPITarget() as target:
            report = run_suite(target)
            forbidden = (target.key.hex(), str(target.path), "127.0.0.1",
                         "X-Contract", "digest")
        self.assertEqual(report["summary"], {"pass": 22, "fail": 0, "error": 0})
        self.assertEqual(report["exit_code"], 0)
        self.assertEqual(sum(row["confirmed_restarts"] for row in report["results"]), 5)
        text = json.dumps(report)
        self.assertFalse(any(value in text for value in forbidden),
                         "private fixture material appeared in a report")

    def test_duplicate_authentication_headers_do_not_write_state(self):
        with FastAPITarget() as target:
            for duplicated in ("X-Contract-Timestamp", "X-Contract-Signature"):
                with self.subTest(header=duplicated):
                    event = Event("duplicate_headers", "headers", 1, 10)
                    body = event.body()
                    stamp = int(time.time())
                    headers = {"X-Contract-Timestamp": str(stamp),
                               "X-Contract-Signature": signature(target.key, stamp, body)}
                    connection = http.client.HTTPConnection("127.0.0.1", target.port, timeout=2)
                    try:
                        connection.putrequest("POST", "/hooks")
                        connection.putheader("Content-Length", str(len(body)))
                        for name, value in headers.items():
                            connection.putheader(name, value)
                        connection.putheader(duplicated, headers[duplicated])
                        connection.endheaders(body)
                        response = connection.getresponse()
                        self.assertEqual(response.status, 401)
                        response.read()
                    finally:
                        connection.close()
                    self.assertEqual(target.observe("headers"), Snapshot())

    def test_oversized_authenticated_body_is_rejected(self):
        with FastAPITarget() as target:
            self.assertEqual(target.send_raw(b" " * (MAX_BODY + 1)).status, 413)
            self.assertEqual(target.observe("oversized"), Snapshot())

    def test_raw_canonical_validation_runs_before_framework_coercion(self):
        bodies = (
            b'{"id":"invalid","stream":"invalid","value":10,"version":true}',
            b'{"extra":0,"id":"invalid","stream":"invalid","value":10,"version":1}',
            b'{"id":"invalid", "stream":"invalid","value":10,"version":1}',
        )
        with FastAPITarget() as target:
            for body in bodies:
                with self.subTest(body_index=bodies.index(body)):
                    self.assertEqual(target.send_raw(body).status, 400)
                    self.assertEqual(target.observe("invalid"), Snapshot())

    def test_stalled_body_read_expires_without_writes(self):
        with FastAPITarget(body_timeout=0.2) as target:
            body = Event("stalled", "stalled", 1, 10).body()
            stamp = int(time.time())
            headers = (
                "POST /hooks HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Length: {len(body)}\r\nX-Contract-Timestamp: {stamp}\r\n"
                f"X-Contract-Signature: {signature(target.key, stamp, body)}\r\n\r\n"
            ).encode("ascii")
            with socket.create_connection(("127.0.0.1", target.port), timeout=2) as transport:
                transport.sendall(headers + body[:1])
                response = http.client.HTTPResponse(transport)
                try:
                    response.begin()
                    self.assertEqual(response.status, 408)
                    response.read()
                finally:
                    response.close()
            self.assertEqual(target.observe("stalled"), Snapshot())

    def test_fault_injection_requires_explicit_app_enablement(self):
        with FastAPITarget(enable_failpoints=False) as target:
            reply = target.send(Event("disabled", "disabled", 1, 10), fault="crash_after_commit")
            self.assertEqual(reply.status, 200)
            self.assertTrue(target.process.is_alive())
            self.assertEqual(target.observe("disabled"), Snapshot(1, 10, 1, 1))

    def test_failed_and_timed_out_startup_clean_up_owned_resources(self):
        for entry, limit in ((exit_before_ready, 1), (never_ready, 0.15)):
            with self.subTest(entry=entry.__name__):
                target = FastAPITarget(startup_timeout=limit)
                with patch("examples.fastapi_adapter.serve_process", entry):
                    with self.assertRaises(EvidenceError):
                        with target:
                            self.fail("a receiver without readiness was accepted")
                self.assertIsNone(target.process)
                self.assertIsNone(target.temporary)
                self.assertFalse(target.path.parent.exists())

    def test_context_cleanup_removes_storage_after_process_exit(self):
        target = FastAPITarget()
        with target:
            target.send(Event("cleanup", "cleanup", 1, 10))
            receiver_pid = target.process.pid
            directory = target.path.parent
            self.assertTrue(directory.exists())
        self.assertFalse(directory.exists())
        self.assertIsNone(target.process)
        self.assertFalse(any(child.pid == receiver_pid for child in multiprocessing.active_children()))
