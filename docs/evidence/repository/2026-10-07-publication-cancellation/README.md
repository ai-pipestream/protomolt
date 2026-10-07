# Publication cancellation and shutdown

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`

The complete production-JAR regression passed using real PostgreSQL, Redis,
LocalStack and Git schema storage. Sol reviewed both added probes.

The library case holds a completed real Redis PUT inside the provider call.
Caller cancellation and timed shutdown leave SQL and the provider open, retain
the active worker, and refuse new publication. Releasing the call delivers the
cancellation; no success receipt or revision commit exists. The physical bytes
remain available for recovery, and final shutdown closes the provider once.

The independent authenticated gRPC case waits for server-context cancellation
while its PUT is held. The transport remains busy, and a second call receives
RESOURCE_EXHAUSTED with a one-call limit. After the provider exits and transport
drains, the cancelled operation remains uncommitted. A distinct operation then
publishes and replays its exact receipt. These calls use a fixture operator token;
they do not qualify scoped-key provisioning or authorization.

The enclosing test requires both probe completion markers and also runs retained
historical decoding, real Redis restart and delayed-write recovery after actual
lease expiry. This evidence does not establish immediate cleanup, remote write
quiescence, or schema-load shutdown for the bounded composition. That last case
remains open. No production code changed in this checkpoint. These are local
test results, not hosted CI, main-branch integration or deployment evidence.
