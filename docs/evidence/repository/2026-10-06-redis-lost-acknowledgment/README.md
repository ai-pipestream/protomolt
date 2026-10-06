# Lost Redis write acknowledgment and abandoned-object cleanup

The real RESP pass-through fixture can discard a successful conditional-write
reply after Redis executes the EVAL. It closes that connection without synthesizing
any response. An independent Redis read first establishes that the bytes exist,
while SQL still marks the upload STAGING.

The qualification requires the original archive RPC to fail and GetEntry to return
NOT_FOUND. Repeating the same request must publish version one using a different
physical object UUID; another retry must return that result with `deduplicated=true`.
The original upload is observed for its actual production five-minute lease.
It must remain STAGING before expiry, never acquire an archive-version reference,
and eventually become DELETED through the host's normal recovery loop.

After recovery, a direct Redis read must confirm the abandoned bytes are gone.
The successful retry must remain readable and retain its exact deduplicated result.
No test shortens the upload lease, edits its SQL timestamps, changes the Redis socket
timeout or replaces the provider. Only the existing recovery polling interval and
minimum age are configured for prompt eligibility checks; they cannot bypass the lease.

Sol reviewed the causal chain and identified the need to refresh gRPC deadlines
after the lease wait. That correction is included. An initial run was deliberately
stopped before the long wait completed to correct those stale test deadlines; it
is not qualification evidence.

Both the short delayed-reply and lost-acknowledgment cases passed, with no failures,
errors or skips, in 5 minutes 10 seconds:

```sh
./gradlew :protomolt-repo-service:test --tests '*BoundedArchiveRedisReplyIT' --console=plain
```

Log: `/tmp/protomolt-redis-lost-ack-recovery-qualified.log`. The shell invocation had
an eight-minute outer deadline; the build completed normally within it.
This test covers loss of a completed write's acknowledgment while the service stays
alive. It does not establish crash durability or the safety of an indefinitely
running remote write; those remain separate boundaries.
