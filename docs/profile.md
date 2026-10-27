# durable-snapshot-v1

## Event identity

The event is a canonical UTF-8 JSON object with exactly id, stream, version, and value.
Keys are sorted, separators have no spaces, and ASCII escaping is used.

id and stream contain 1-64 ASCII letters, digits, underscores, periods, or hyphens.
Version is an integer from 1 through 2^31-1. Value is an integer from -1,000,000
through 1,000,000. Booleans, duplicate keys, unknown fields, nonfinite JSON, and
noncanonical encoding are rejected.

The event ID is global within the isolated fixture. Identity uses the exact canonical
body fingerprint. Attempt timestamps and signatures do not change event identity.

## Authentication

The fixture signs these exact bytes with HMAC-SHA256:

~~~text
hookcontract-v1\n{canonical integer timestamp}\n{canonical event body}
~~~

X-Contract-Timestamp carries the integer timestamp.
X-Contract-Signature carries the lowercase hexadecimal digest.
Exactly one of each authentication header is required.
The accepted window includes both boundaries at +/-300 seconds.
Authentication uses the received bytes and constant-time digest comparison.

Authentication precedes parsing, receipt lookup, and acknowledgment.
A known event ID with an invalid signature still receives 401.
This is a fixture protocol; other producer protocols require explicit adapters.

## State transition and receipt rules

1. An accepted ID with the same canonical body receives 200 and creates no new effect.
2. An accepted ID with a different body receives 409 and mutates nothing.
3. A new ID with a version above the stream head advances state and creates one effect,
   including when its value matches the previous value.
4. A new ID below the head is accepted/stale and creates no effect.
5. A new ID equal to the head with the same value is accepted/redundant and creates no effect.
6. A new ID equal to the head with a conflicting value receives 409 and creates no receipt.

Every accepted applied, stale, or redundant event retains its ID durably.
Only a current-head version collision is rejected; older versions are treated as stale.
The model retains no complete historical value timeline.

Receipt insertion, snapshot update, and effect insertion share one transaction.
Acknowledgment follows commit. An independent read transaction observes a consistent
snapshot and effect/receipt counts.

## Sender policy

The default sender allows three attempts. Transport ambiguity and statuses
408, 425, 429, 500, 502, 503, and 504 can retry. Other statuses, including redirects,
stop immediately. The same canonical event bytes and ID are sent on each attempt;
authentication is regenerated per attempt.

Backoff begins at 0.01 seconds and is capped at 0.1 seconds.
Retry-After supports nonnegative integer seconds and timezone-aware HTTP dates.
The total scheduled delay budget is one second, with a ten-second elapsed budget.
The remaining elapsed budget is checked before every request and backoff, passed
to the transport, and checked after waiting.
The built-in HTTP client's default whole-request deadline is two seconds.
Fixture inputs and responses are bounded to 64 KiB. Policy limits are explicit.

## Observation and restart

The observer supplies version, value, committed effect count, accepted receipt count,
and a settled flag for an isolated stream. An asynchronous adapter must reach its
settling condition within its own bounded deadline and confirm that flag.

The post-commit failpoint exits the receiver process before response bytes are sent.
The restart adapter confirms the controlled exit and starts a new process on the same
database. Other restart scenarios prove that applied, stale, and redundant IDs persist.

These observations cover process termination and restart in the test environment.
They establish no guarantee for power loss, disk corruption, or external side effects.
