# Managed historical shutdown with a held provider reply

Real PostgreSQL, Redis and Git qualify cancellation and shutdown during a mixed
historical/new publication. A wrapper invokes the real Redis PUT and holds its
successful reply. The test reads the written bytes back and verifies their checksum;
it does not synthesize a storage success. The host first publishes a typed source,
then reuses that historical revision alongside a new parser payload.

Both a library call and an authenticated in-process gRPC call are exercised. The
client cancels after the PUT reply is held. For gRPC the observer receives the actual
`CANCELLED` status and the server context observes cancellation while the producer
remains active. A 200 ms host close must time out and retain:

- the exact historical source pin IDs;
- the open provider and database;
- the active host execution and accepted worker.

An existing repository handle refuses new work after admission closes. Releasing the
provider reply allows the producer to finish; no assessment owner, success receipt
or revision commit is created. A repeated close releases every hosted source pin,
attests reader `QUIESCED`, fences the host and closes the provider exactly once.
Another close remains idempotent.

The fixture uses package-private hosted composition only to supply the real provider
with a held-reply wrapper. It runs the normal managed historical runtime and transport.
There are no production changes in this checkpoint.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalShutdown' \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalHostBinding' \
  --tests '*DocumentAssessmentStorageRuntimeTest.boundedPublicHostBinding' \
  --max-workers=2 --console=plain
```

Three aggregate host cases passed in 38 seconds, with zero failures, errors or skips.
After adding immediate failure diagnostics for an empty unary RPC completion, the
shutdown case passed again in 13 seconds. Both XML reports are archived here. Sol
reviewed the worker lifetime, cancellation, exact pins and final lifecycle assertions.
Initial fixture corrections fixed a native-query cast and testing a pre-existing
repository handle after close rather than asking a closed host for a new handle.

This covers a held provider PUT with caller cancellation. It does not qualify a held
registry load, held historical GET, uncancelled active RPC, unavailable recovered
upload placement, network listener shutdown, deployment or throughput.
