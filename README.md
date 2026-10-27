# HookContract

[![CI](https://github.com/AntJun/hook-contract/actions/workflows/ci.yml/badge.svg)](https://github.com/AntJun/hook-contract/actions/workflows/ci.yml)

**Webhook consumer conformance tests with fault injection and durable-state evidence.**

HookContract sends actual loopback HTTP requests and checks committed state under
22 named scenarios: duplicate and concurrent delivery, bounded retries, reordered
snapshots, conflicting identities, invalid signatures, rollback boundaries, and
a post-commit process failure followed by restart.

[Live reports](https://antjun.github.io/hook-contract/) |
[Contract profile](docs/profile.md) |
[Architecture](docs/design.md)

Python 3.11+. Zero runtime dependencies. Maintained by [AntJun](https://github.com/AntJun).

## Why it exists

An HTTP acknowledgment alone supplies limited evidence about the resulting business state.
HookContract combines delivery observations with an independent durable-state observer.
It checks the same documented profile against a transactional reference receiver
and a deliberately naive receiver with seeded duplicate/ordering defects.

This is an integration-test harness. Its small SQLite receiver supplies a runnable
fixture and a reference for adapter behavior. Existing applications integrate
through an operator-controlled Python target adapter.

## Run the demonstration

~~~sh
python -m pip install .
hookcontract demo --output artifacts/demo
~~~

This starts isolated receiver processes, exercises real HTTP and SQLite transactions,
verifies positive and negative controls, and writes self-contained HTML, JSON, and JUnit.
The positive control must pass all 22 scenarios. The negative control must produce
named contract violations, with complete evidence.

The demo command returns zero when both controls match their expected outcomes.
The intentionally faulty receiver's own report has exit code one.
A startup error or incomplete negative-control observation fails the demo.

## Check an adapter

~~~sh
hookcontract check --adapter examples.adapter:target --output artifacts/check
~~~

The runnable example uses the reference receiver. An application adapter exposes:

- send(event, fault=None, tamper=False, age=0, timeout=None): one delivery attempt honoring the remaining deadline, with typed HTTP evidence.
- send_raw(body): one authenticated raw-body attempt for malformed-message checks.
- observe(stream): an independent, settled Snapshot of version, value, effects, and receipts.
- restart(after_crash=False): confirmed restart from durable storage.

The factory returns a context manager that owns its test environment and cleans it up.
Run adapters you control in an isolated namespace with generated fixture data.
Adapters are Python code; module loading executes their imports and factory.

The built-in HTTP client permits literal loopback addresses with an explicit port.
It bounds response bytes and whole-request time and returns redirects without following them.
An adapter for an asynchronous application must implement bounded settling observation;
unconfirmed state produces incomplete evidence.

## Decisions and artifacts

| Exit | Meaning |
|---|---|
| 0 | All named scenarios supplied complete evidence and satisfied the profile |
| 1 | At least one complete scenario violated the profile |
| 2 | Required delivery, observation, settling, restart, or output evidence was incomplete |

Incomplete evidence takes precedence. Reports preserve every scenario's result.
JSON/HTML include response status codes, relative request durations, approved expected/observed
fixture state, and confirmed restart counts. JUnit represents violations as failures and
incomplete evidence as errors.

Raw payloads, HTTP headers, signatures, authentication keys, database/journal files,
addresses, ports, and raw exception text are excluded. Demo keys are generated in memory
and are never written to configuration, artifacts, or Git.

## What the reference receiver demonstrates

- Raw-body HMAC authentication and a bounded attempt timestamp window, checked before deduplication.
- Canonical event identity with a durable global event-ID ledger.
- Receipt, state, and effect writes in one BEGIN IMMEDIATE transaction.
- Accepted stale and redundant IDs surviving receiver restart.
- Four concurrent identical deliveries producing one effect.
- A process exit after commit and before acknowledgment, followed by safe redelivery.
- Rollback after receipt insertion and after state update, before the effect is written.

The receiver uses SQLite rollback journaling and synchronous FULL, with one connection
per request and a bounded busy timeout. The runtime SQLite version appears in CI and
the demo method JSON.

## Scope

The versioned durable-snapshot-v1 profile defines a small synchronous snapshot model.
Its object version is separate from signed attempt time. Collision and stale-event
policies are explicit; they can differ from a particular provider or application.

A passing result covers these scenarios and observed state. It establishes no general
exactly-once guarantee, production certification, power-loss tolerance, distributed
external-effect atomicity, GPU behavior, or provider-wide interoperability.

The sender uses its own declared retry policy. Provider retry schedules and signature
schemes need their own adapter or profile. The fixture HMAC format is documented;
there is no claim of full Stripe or Standard Webhooks protocol compatibility.

## Development

~~~sh
python -m unittest discover -s tests -v
python -m hookcontract demo --output artifacts/demo
python tools/check_demo.py artifacts/demo
python tools/audit_public.py --history artifacts/demo
~~~

CI runs on Linux and Windows, installs the package, exercises actual HTTP/process
failures, checks both controls, scans all fetched Git history for credentials, and
audits public artifacts. Workflow actions are pinned to full commit SHAs.

MIT licensed.

Technical references: [Stripe webhooks](https://docs.stripe.com/webhooks) |
[Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md) |
[SQLite transactions](https://www.sqlite.org/lang_transaction.html) |
[HTTP Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).
