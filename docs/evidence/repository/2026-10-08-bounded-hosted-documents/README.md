# Bounded hosted document qualification

Code checkpoint: `bad4b830e` on `agent/host-reader-composition`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --tests '*DocumentAssessmentStorageRuntimeTest.boundedPublicHostBinding' --max-workers=2 --console=plain
```

One aggregate test passed without failures, errors or skips; 20-second build,
11.555-second test. The child JVM uses production JARs plus compiled fixture code,
real PostgreSQL and durable Redis. It executes four cases: local-only and hosted
composition, each through library and authenticated in-process gRPC.

Each case publishes a typed document, checks exact committed receipt replay, and
decodes retained validated history. RPC cases check unauthenticated/wrong-token
rejection and authenticated publication, replay and history. Hosted cases assert
one exact execution/host/boot binding while ACTIVE and a FENCED host plus QUIESCED
LOCAL_DRAIN reader after close. The same markers remain mandatory in the full
storage aggregate. Sol reviewed the test changes with no blocking finding.

This is functional qualification, not a remote termination proof or load test.
The archived XML includes child output and all four success markers. The earlier
full storage run at `731af65ef` is separate and does not test this code checkpoint.
