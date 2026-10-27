# FastAPI integration example

This optional example runs a real FastAPI application behind Uvicorn in a separate
spawned process. It implements the durable-snapshot-v1 storage rules separately from
the built-in receiver and supplies an independent read-only SQLite observer.

[Published integration report](https://www.antjun.com/hook-contract/fastapi/index.html).

From a repository checkout:

~~~sh
python -m pip install . -r examples/fastapi-requirements.txt
python -m hookcontract check --adapter examples.fastapi_adapter:target --output artifacts/fastapi
~~~

The command exercises all 22 scenarios and exports HTML, JSON, and JUnit evidence.
The core package keeps zero runtime dependencies. The example requirements pin the
tested FastAPI and Uvicorn versions; their transitive dependencies are resolved by pip.
The source example remains outside the distributed core package.

## Integration boundary

examples/fastapi_receiver.py contains the service. It shares the fixture Event model
and HMAC helpers with the harness. Its SQL schema and transaction implementation are
separate from hookcontract.store.Store. These shared wire definitions mean this example
supplies framework-integration evidence, with no claim of independent protocol certification.

examples/fastapi_adapter.py owns process startup, requests, observation, restart, and
cleanup. It implements the same Target protocol as the built-in receiver.

| Concern | Implementation |
|---|---|
| Body handling | Bounded raw ASGI stream and a whole-body read deadline |
| Authentication | Duplicate-header rejection, raw-body HMAC, timestamp window |
| Transactions | Per-request SQLite connection, BEGIN IMMEDIATE, synchronous FULL |
| Concurrency | SQLite work runs in a threadpool outside the ASGI event loop |
| Observation | Harness-owned read-only connection and consistent read transaction |
| Readiness | Authenticated health response after ASGI startup and HTTP serving |
| Restart | Confirmed exit code for post-commit failure, then a new process on the same database |
| Cleanup | Owned process termination, socket closure, then temporary directory removal |

The observer reads receipts and effects independently of HTTP acknowledgments.
The service exposes no state-inspection route. The loopback listener is bound before
process creation and transferred through multiprocessing, which avoids selecting an
unreserved port. Generated authentication keys travel through the spawn mechanism
in memory and stay outside command-line arguments, configuration files, logs, and reports.

## Failpoints

create_app defaults to disabled failpoints. The conformance adapter enables them
explicitly in its isolated test process. It supports transient 429/503 responses,
pre-commit rollback probes, and an immediate process exit after commit and before
response creation. The adapter confirms that exit before restarting.

The example has one worker, no reload, no public network listener, and no request logs.
Its control hooks and temporary database belong to disposable test environments.
Applications adapting this pattern supply their own event mapping, storage, independent
observer, and controlled failure mechanism.

## Verification

Dedicated Linux and Windows CI jobs install the optional dependencies, run the full
contract suite and HTTP/lifecycle regression tests, and execute the documented CLI.
Missing optional dependencies fail these jobs. The base package matrix reports the
optional tests as skipped when those dependencies are absent.

Additional checks cover duplicate authentication headers, oversized bodies, strict
canonical JSON, stalled body deadlines, disabled failpoints, failed startup cleanup,
and removal of temporary storage after process exit. Generated reports pass the
same public-artifact audit as the built-in controls.

References: [FastAPI Request access](https://fastapi.tiangolo.com/advanced/using-request-directly/) |
[Uvicorn server lifecycle](https://github.com/Kludex/uvicorn/blob/0.53.0/uvicorn/server.py).
