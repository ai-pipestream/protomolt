# Managed historical shutdown during schema resolution

The hosted shutdown probe covers a real schema registry worker alongside the
existing held Redis GET/PUT cases. A typed source uses StringValue; the mixed
historical candidate adds a Timestamp parser shape. These descriptor closures have
different artifact digests, so the second lookup cannot reuse the source cache entry.
The gate selects the fresh digest, invokes the actual Git registry, verifies the
returned descriptor bytes, then holds the successful reply.

Library and authenticated in-process gRPC cases both assert two registry reads and
one active load before cancellation. A 200 ms close retains the real resolver worker,
provider and database while the host remains ACTIVE. The caller can cancel before
the asynchronous registry worker exits. The held worker owns a fresh descriptor
lookup, so this phase does not require retaining historical source pins after their
I/O has already completed; the provider GET phase separately checks exact pin IDs.

After release, the resolver must drain with zero cached bytes rather than caching
the late result. No assessment owner, revision commit or success receipt is created.
Repeated host close leaves no source pins, attests reader QUIESCED, fences the host
and closes the provider exactly once. Actual gRPC CANCELLED delivery is observed.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalShutdown' \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalHostBinding' \
  --tests '*DocumentAssessmentStorageRuntimeTest.boundedPublicHostBinding' \
  :protomolt-repo-service:test \
  --tests '*ManagedSchemaOwnershipTest' \
  --tests '*ManagedSchemaHostIT' \
  --max-workers=2 --console=plain
```

Passed in 47 seconds: 12 cases, zero failures, errors or skips. Three cases are
aggregate production-JAR host probes; shutdown requires six markers for PUT, GET
and SCHEMA over both call paths. XML is archived here. Sol reviewed the distinct
schema identity, actual registry load, worker ownership and final cleanup. No
production change was required.

These are caller-cancelled operations with controlled real-provider replies. They
do not qualify uncancelled active RPC shutdown, unavailable recovered upload
placements, arbitrary resolver selection code, socket transport, deployment or
performance. Those broader requirements remain separate.
