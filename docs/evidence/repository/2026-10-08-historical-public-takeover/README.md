# Concurrent public historical takeover

Test-only qualification on parent `4eab777b0a4f856e5a036fcfe2de59703c92f560`.
Production code is unchanged from the dispatcher checkpoint `d0c886a5d`.
Sol reviewed the concurrency, fencing, cleanup and final provider-readback assertions.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Final run passed in 1m10s: one aggregate test, 66.654 seconds, zero failures,
errors or skips. The earlier run also passed, before the additional provider
readback assertions. The archived Gradle log and XML are from the final run.
The aggregate requires `HISTORICAL_PUBLIC_CONCURRENT_TAKEOVER_OK` in the child
process output, alongside the existing initial-owner and public-dispatch cases.

The fixture uses real PostgreSQL and LocalStack S3. A test proxy delegates the
first PUT to the real provider, then holds its successful reply. SQL claim and
owner locks keep renewal waiting while database time crosses both lease deadlines.
No timestamps or successful provider responses are fabricated.

While the library predecessor remains borrowed and its provider worker is held,
an authenticated in-process gRPC request submits the same intent and payloads.
It commits through public repository dispatch, without directly installing a
successor in the test. Assertions establish:

- Two actual PUTs use distinct upload attempts and lease tokens. Only the
  successor object becomes verified.
- Exactly one revision commit and one assessment owner exist for the operation.
- Maintenance retains the held predecessor. After release, its call fails with
  an execution-claim fence; its late reply remains unverified.
- Exact retries preserve the successor receipt without another PUT. Both
  generation slots drain, and a subsequent distinct public operation succeeds.
- Actual GETs of committed provider versions match sizes, checksums and upload
  bytes. Historical parts preserve their physical identity; the original
  historical revision remains readable and unchanged.
- Runtime shutdown completes and payload reservations return to their baseline.

This covers a library predecessor and in-process gRPC successor in one runtime.
It does not establish TCP behavior, cross-host throughput, production authority
provisioning, cleanup fairness or all public cancellation/failure boundaries.
The historical runtime factory remains package-private pending that qualification.
