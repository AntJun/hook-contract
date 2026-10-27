"""Command-line contracts and a positive/negative-control demonstration."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

from .core import InputError
from .reference import ManagedTarget
from .report import render_html, render_junit
from .suite import SCENARIOS, run_suite


def write_atomic(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n",
                                         dir=path.parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(content)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def export(folder, report, title):
    write_atomic(folder / "report.json", json.dumps(report, indent=2, allow_nan=False) + "\n")
    write_atomic(folder / "index.html", render_html(report, title))
    write_atomic(folder / "junit.xml", render_junit(report))


def demo(folder):
    with ManagedTarget() as target:
        healthy = run_suite(target)
    with ManagedTarget(naive=True) as target:
        naive = run_suite(target)
    failures = {row["id"] for row in naive["results"] if row["status"] == "fail"}
    required = {"duplicate", "concurrent_duplicate", "lost_ack_restart", "out_of_order"}
    if healthy["exit_code"] != 0 or naive["exit_code"] != 1 or not required <= failures:
        raise InputError("positive or negative control did not match its expected contract outcome")
    export(folder / "healthy", healthy, "Transactional receiver")
    export(folder / "naive", naive, "Seeded duplicate and ordering defects")
    method = {
        "profile": "durable-snapshot-v1", "scenarios": len(SCENARIOS),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "sqlite": sqlite3.sqlite_version, "journal_mode": "DELETE", "synchronous": "FULL",
        "transport": "genuine loopback HTTP", "restart": "spawned process, same temporary database",
        "failure_point": "committed transaction, before response; controlled process exit",
        "inputs": "fixed generated versioned snapshots", "key_storage": "memory only",
        "positive_control_exit": healthy["exit_code"], "negative_control_exit": naive["exit_code"],
        "negative_control_required_findings": sorted(required),
        "limits": "Named fixture scenarios and observed durable state. No power-loss, distributed external-effect, or production guarantee.",
    }
    write_atomic(folder / "method.json", json.dumps(method, indent=2) + "\n")
    landing = """<!doctype html><html lang="en-US"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>HookContract</title>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<style>body{font:18px/1.6 system-ui,sans-serif;background:#f2f6f5;color:#17332f;margin:0}
main{max-width:960px;margin:auto;padding:64px 24px}h1{font-size:clamp(2.5rem,6vw,4rem);line-height:1.1}
a{color:#05695d}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:24px}
article{background:#fff;border:1px solid #cadbd6;border-top:4px solid #087f70;border-radius:6px;padding:26px}
pre{overflow:auto;background:#17332f;color:white;padding:24px;border-radius:6px}.brand{color:#087f70;font-weight:800}
footer{margin-top:36px;font-size:.85rem;color:#506d65}</style></head><body><main>
<div class="brand">HookContract / 0.1.0</div><h1>Did the delivery change durable state exactly as intended?</h1>
<p>Exercise a webhook consumer over HTTP. Inspect duplicates, concurrency, bounded retries,
reordered versions, invalid signatures, rollback boundaries, and a real post-commit process failure.</p>
<div class="grid"><article><h2>Transactional receiver</h2><p>Accepted receipts and state transitions commit atomically.
The suite checks the resulting database state.</p><a href="healthy/index.html">Inspect the passing control</a></article>
<article><h2>Seeded defects</h2><p>A deliberately naive receiver reapplies duplicate and stale snapshots.
The same suite must detect the named violations.</p><a href="naive/index.html">Inspect detected violations</a></article></div>
<h2>One command, inspectable evidence</h2><pre>python -m hookcontract demo --output artifacts/demo</pre>
<p>Run your adapter with <code>hookcontract check --adapter your_module:factory</code>.
The fixture profile and adapter capabilities are explicit.</p>
<p><a href="method.json">Measurement method</a> &middot;
<a href="https://github.com/AntJun/hook-contract">Source, profile, and integration guide</a></p>
<footer>Maintained by AntJun. MIT licensed. Reports contain constructed fixture observations.</footer>
</main></body></html>"""
    write_atomic(folder / "index.html", landing)
    print(f"Positive control: PASS; {len(SCENARIOS)} scenarios.")
    print("Negative control: expected violations detected.")
    return 0


def load_factory(spec):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*", spec):
        raise InputError("adapter requires module:factory syntax")
    module, name = spec.split(":")
    factory = getattr(importlib.import_module(module), name)
    if not callable(factory):
        raise InputError("adapter factory must be callable")
    return factory


def main(argv=None):
    parser = argparse.ArgumentParser(description="Webhook consumer conformance with durable-state evidence.")
    sub = parser.add_subparsers(dest="command", required=True)
    example = sub.add_parser("demo", help="run actual HTTP positive and negative controls")
    example.add_argument("--output", type=Path, default=Path("artifacts/demo"))
    check = sub.add_parser("check", help="run an operator-controlled Python target adapter")
    check.add_argument("--adapter", default="hookcontract.reference:ManagedTarget")
    check.add_argument("--output", type=Path, default=Path("artifacts/check"))
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            return demo(args.output)
        with load_factory(args.adapter)() as target:
            report = run_suite(target)
        export(args.output, report, "Consumer contract report")
        print(f'HookContract: {report["status"].upper()} | {report["summary"]}')
        return report["exit_code"]
    except Exception:
        print("HookContract could not collect complete contract evidence.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
