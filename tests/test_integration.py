import contextlib
import io
import json
import threading
import unittest
from dataclasses import dataclass
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from hookcontract.__main__ import load_factory, main
from hookcontract.core import Event, EvidenceError, HTTPClient, Reply, Snapshot, deliver
from hookcontract.reference import ManagedTarget
from hookcontract.report import render_html, render_junit
from hookcontract.suite import Case, Violation, run_suite, same_version_race

KEY = bytes(range(32))


class IntegrationTests(unittest.TestCase):
    def test_transactional_receiver_passes_all_scenarios(self):
        with ManagedTarget() as target:
            report = run_suite(target)
            key_hex = target.key.hex()
        self.assertEqual(report["exit_code"], 0)
        self.assertEqual(report["summary"], {"pass": 21, "fail": 0, "error": 0})
        self.assertEqual(sum(r["confirmed_restarts"] for r in report["results"]), 4)
        text = json.dumps(report)
        self.assertFalse(key_hex in text, "authentication material appeared in a report")
        self.assertNotIn("127.0.0.1", text)
        self.assertNotIn("X-Contract", text)

    def test_naive_receiver_has_named_violations_without_incomplete_evidence(self):
        with ManagedTarget(naive=True) as target:
            report = run_suite(target)
        self.assertEqual(report["exit_code"], 1)
        self.assertEqual(report["summary"]["error"], 0)
        failed = {row["id"] for row in report["results"] if row["status"] == "fail"}
        self.assertTrue({"duplicate", "concurrent_duplicate", "lost_ack_restart", "out_of_order"} <= failed)

    def test_missing_observation_is_incomplete_and_exception_is_private(self):
        class Missing:
            def send(self, *_args, **_kwargs):
                return Reply(200)
            def send_raw(self, *_args):
                return Reply(400)
            def observe(self, *_args):
                raise RuntimeError("private-adapter-" + "sentinel")
            def restart(self, **_kwargs):
                raise NotImplementedError()
        report = run_suite(Missing())
        self.assertEqual(report["exit_code"], 2)
        self.assertNotIn("private-adapter", json.dumps(report))

    def test_unsettled_observer_cannot_pass(self):
        class Missing:
            def send(self, *_args, **_kwargs):
                return Reply(200)
            def send_raw(self, *_args):
                return Reply(400)
            def observe(self, *_args):
                return Snapshot(settled=False)
            def restart(self, **_kwargs):
                return True
        self.assertEqual(run_suite(Missing())["exit_code"], 2)

    def test_adapter_snapshot_extensions_are_excluded(self):
        @dataclass(frozen=True)
        class Extended(Snapshot):
            internal_url: str = "private-extension-" + "sentinel"
        class Target:
            def observe(self, _):
                return Extended(1, 10, 1, 1)
        case = Case(Target())
        case.expect("s", 1, 10, 1, 1)
        text = json.dumps(case.observations)
        self.assertNotIn("internal_url", text)
        self.assertNotIn("private-extension", text)

    def test_adapter_cannot_bypass_base_snapshot_validation(self):
        class Invalid(Snapshot):
            def __post_init__(self):
                pass
        class Target:
            def observe(self, _):
                return Invalid(value="private-value")
        case = Case(Target())
        with self.assertRaises(EvidenceError):
            case.observe("s")
        self.assertEqual(case.observations, [])

    def test_race_observation_must_match_acknowledged_winner(self):
        class Misrouted:
            def send(self, event, **_):
                return Reply(200 if event.id == "collision_a" else 409)
            def observe(self, _):
                return Snapshot(1, 20, 1, 1)
        with self.assertRaises(Violation):
            same_version_race(Case(Misrouted()))

    def test_redirect_is_returned_without_following_location(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(302)
                self.send_header("Location", "http://127.0.0.1:9/unreached")
                self.send_header("Content-Length", "0")
                self.end_headers()
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = HTTPClient(f"http://127.0.0.1:{server.server_port}", KEY)
            result = deliver(client, Event("a", "s", 1, 1))
            self.assertEqual([r.status for r in result.replies], [302])
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_truncated_response_cannot_count_as_acknowledged(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Length", "10")
                self.end_headers()
                self.wfile.write(b"x")
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = HTTPClient(f"http://127.0.0.1:{server.server_port}", KEY)
            self.assertIsNone(client.send(Event("a", "s", 1, 1)).status)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_whole_request_deadline_interrupts_a_trickling_body(self):
        release = threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Length", "30")
                self.end_headers()
                try:
                    for _ in range(30):
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        if release.wait(0.02):
                            break
                except OSError:
                    pass
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = HTTPClient(f"http://127.0.0.1:{server.server_port}", KEY, timeout=0.1)
            self.assertIsNone(client.send(Event("a", "s", 1, 1)).status)
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            worker.join()


class OutputTests(unittest.TestCase):
    def report(self):
        class Missing:
            def send(self, *_args, **_kwargs):
                return Reply(None)
            def observe(self, *_args):
                raise EvidenceError()
        return run_suite(Missing())

    def test_html_title_is_escaped_and_script_free(self):
        text = render_html(self.report(), '<img src=x onerror="alert(1)">')
        self.assertIn("&lt;img", text)
        self.assertNotIn("<img", text)
        self.assertNotIn("<script", text)

    def test_junit_preserves_failure_and_incomplete_counts(self):
        report = self.report()
        suite = ET.fromstring(render_junit(report))
        self.assertEqual(int(suite.attrib["errors"]), report["summary"]["error"])
        self.assertEqual(int(suite.attrib["failures"]), report["summary"]["fail"])
        self.assertEqual(len(suite), 21)

    def test_invalid_adapter_returns_generic_incomplete_error(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            code = main(["check", "--adapter", "private_missing_module:factory"])
        self.assertEqual(code, 2)
        self.assertNotIn("private_missing_module", output.getvalue())

    def test_runnable_adapter_factory_is_loadable(self):
        self.assertTrue(callable(load_factory("examples.adapter:target")))
