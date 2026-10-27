# Architecture and verification

~~~text
Scenario runner -> bounded sender -> literal loopback HTTP -> receiver
       |                                                  |
       +-> independent state observer <- SQLite transaction
       |
       +-> typed observations -> HTML / JSON / JUnit
~~~

core.py owns canonical fixtures, authentication, typed evidence, and sender policy.
store.py owns accepted IDs, versioned state, and effect writes.
reference.py owns isolated receiver processes and named failpoints.
suite.py defines the reusable Target protocol and 21 scenarios.
report.py renders approved evidence fields without raw request data.

## Transaction boundary

The reference uses BEGIN IMMEDIATE for the complete read/check/write sequence.
SQLite serializes writers. Each request uses a separate connection and a bounded
busy timeout. Rollback journaling with synchronous FULL keeps the fixture's storage
behavior explicit and broadly testable.

The effect journal is independent from snapshot state and acknowledgment counts.
Its rows and accepted receipts commit in the same transaction as state.
This lets the observer detect repeated effects even when final state looks unchanged.

The negative control retains authentication and transactions while intentionally
reapplying duplicates and stale/redundant snapshots. The demonstration requires
named findings and exit one. Incomplete evidence fails the detector demonstration.

## Failure experiments

- Identical concurrent requests test deduplication under competing transactions.
- Different versions can commit in either order; the maximum final version is required,
  while one or two effects are allowed.
- Equal-version conflicting values require one winner, one conflict, and one effect.
- A confirmed post-commit process exit creates an ambiguous delivery outcome.
- Applied, stale, and redundant ID-conflict probes after restart verify receipt durability.
- Two pre-commit failures verify that partial receipt/state writes roll back.

## Boundaries

The HTTP server is an isolated test fixture bound to loopback.
Its failpoint header belongs to the controlled fixture. Production applications
implement their own test adapter and post-commit failure mechanism.

The reference body scheme and immediate state model are versioned fixture choices.
An adapter must map the application's event model, observation, and restart behavior
explicitly. Missing capabilities produce incomplete results.

Reports contain fixed scenario identifiers, approved state numbers, status codes,
and relative durations. Arbitrary exceptions become a generic incomplete finding.
The generated authentication key, raw requests, database, journals, and process details
remain outside public artifacts.

The suite covers named schedules and failure boundaries. Extend it with application-specific
scenarios and real producer adapters when those semantics are known.
