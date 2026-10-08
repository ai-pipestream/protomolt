# Historical upload provider faults

Base: abbcd1845. One aggregate production-JAR test passed with real PostgreSQL and
LocalStack, zero failures/errors/skips, Gradle exit 0 in 27 seconds. The test
requires all four fault-probe markers plus the existing initial publication and
lost CREATE acknowledgement reconciliation markers.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Four cases use fresh operations and a counted real storage adapter:

- Exact verified replay preserves the selection and performs exactly one PUT total.
- Source READ denial commits after the real PUT, before its result returns. The
  transfer returns NOT_FOUND, the object stays unverified, and restoring access
  does not revive the candidate because ACL changes advance mutation_revision.
- Cancellation after PUT prevents verification. Retrying the unverified attempt
  requires reconciliation and performs no additional PUT.
- A lost provider reply after PUT also leaves an unverified attempt. Exact retry
  is rejected without another PUT.

Each case reads back the actual provider version and bytes, checks zero assessment
rows, drains provider activity, returns transfer reservations, then detaches the
owner and releases captures. No provider response is fabricated.

The first fixture run failed because unconditional ACL restoration advanced the
revision between cases. The final fixture restores only actual revocations and
reads the current revision for each new operation. No production code changed.

Still unqualified: successor fault cases, destination-only revocation, execution
expiry or takeover during PUT, provider-worker shutdown barriers, cancellation
after verification, lost SQL verification acknowledgement, and later-callback SQL
lock timeouts. This is not full historical routing qualification.

Sol reviewed the final fixture and found no blocking issue. Failure expectations
separate ACL revision conflict from unverified-attempt fencing.
