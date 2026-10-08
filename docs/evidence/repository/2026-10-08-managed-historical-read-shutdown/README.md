# Managed historical shutdown during a provider read

The managed historical shutdown probe now covers both a held PUT reply and a held
historical GET reply, through library and authenticated in-process gRPC calls.
PostgreSQL, Redis and Git are real. The GET wrapper selects one declared historical
source by namespace, object key and provider version, invokes Redis, then holds the
successful reply. Returned length and SHA-256 must equal the retained object identity.
The wrapper is armed only after the source publication and request construction.

For asynchronous reads, cancellation can finish the caller or transport producer
before the actual provider worker exits. Host shutdown must still time out without
releasing the exact source pins, open provider or database, or attesting host
termination. After the gate releases, repeated close must remove the source pins,
attest reader `QUIESCED`, fence the host and close the provider exactly once. Neither
path creates an assessment, revision commit or success receipt for the cancelled
publication. gRPC cancellation is observed through the client's actual `onError`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalShutdown' \
  --max-workers=2 --console=plain
```

The aggregate case passed in 18 seconds, with zero failures, errors or skips. Its
four required markers cover GET/PUT × library/gRPC. XML is archived here. Sol reviewed
the exact-object gate and the distinction between caller and provider-worker lifetime.
No production changes were required.

This qualifies one selected historical source read and one new upload per scenario.
It does not qualify held schema-registry loads, uncancelled active RPCs, unavailable
recovered upload placements, socket transport behavior, performance or deployment.
