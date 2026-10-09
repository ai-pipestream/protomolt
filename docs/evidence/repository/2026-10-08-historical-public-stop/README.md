# Historical public cancellation and shutdown

Test-only qualification on parent `c42ccd4280c6c51e8fe4d1c2c06671e9b37c2972`.
No production implementation changes. Sol reviewed worker ownership, shutdown
ordering, cancellation propagation and transport capacity. The factory remains
package-private pending the other public acceptance cases.

The fixture uses PostgreSQL and real LocalStack S3 PUTs. A proxy holds the first
successful provider reply after delegating the actual write. It never supplies a
fabricated result. Each phase gets its own runtime and provider gate.

- Library cancellation: cancel the accepted request while its PUT reply is held.
  The caller, generation and read lifetime remain active. After release, the call
  reports CANCELLED; its object stays unverified and no assessment or revision
  commit exists.
- Orderly shutdown: close admission while the accepted call is held. New calls
  receive UNAVAILABLE and a zero-wait shutdown step returns false. Release permits
  the accepted uncancelled call to finish with one valid committed receipt.
- Remote cancellation: cancel through `RemoteDocumentPublicationRepository` over
  authenticated in-process gRPC. Wait for server-context cancellation. The client
  returns CANCELLED while the service still holds producer capacity and delivery
  bytes; a second call receives RESOURCE_EXHAUSTED. After actual producer exit,
  bytes return, the old object stays unverified, and no assessment or commit exists.

Every phase retries bounded runtime shutdown, checks zero remaining historical
generations and reader captures, and returns payload reservations to baseline.
The read assertion measures the retained read lifecycle; it does not independently
count SQL pin rows. Transport authentication uses a test token bound to a real
scoped repository caller. No external identity-provider behavior is claimed.

## Verification

The combined run passed in 2m43s: four JUnit cases, 161.106 seconds, zero failures,
errors or skips. It includes initial-owner/public cases and all three public
cold-process restart phases. Its compressed log and XML are named
`combined-before-capacity-adjustment.*` because the subsequent review restored
service capacity two for normal sequential replay tests; only the cancellation
capacity test uses one.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicProcessRestart' \
  --max-workers=2 --console=plain
```

The final focused command after that adjustment passed in 1m8s, with one
aggregate test and zero failures, errors or skips. Its log and XML use the `final-`
prefix. This run requires the library cancellation, orderly shutdown and remote
cancellation markers in addition to the existing initial-owner/public cases.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

These are local correctness checks, not TCP, throughput or horizontal-scaling
qualification. Explicit deadlines, orderly transport closure, current-policy races,
negative intent/identity, lost publication acknowledgement and cleanup failures
remain in the factory-exposure acceptance list.
