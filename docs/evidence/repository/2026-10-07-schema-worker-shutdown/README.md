# Bounded document schema-worker shutdown

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`

The complete production-JAR storage regression passed with the added bounded
schema-load case. Sol reviewed the fixture and its lifecycle assertions.

The fixture stages typed content using real Redis PUT and read-back operations.
A fresh resolver fetches the exact descriptor bytes from a real Git registry. A
gate holds that result inside the registry call; resolver counters identify one
active load. Cancelling the publication and attempting timed shutdown lets the
caller finish while the resolver worker remains active. SQL and Redis stay open,
new publication is refused, and the cancelled operation has no successful receipt
or revision commit.

After gate release, the worker drains and the closed resolver does not cache the
late result. Final shutdown closes the provider once, including on repeated close.
The fixture owns Git outside the host; the evidence proves safe draining of a
worker using that borrowed store, not ownership of Git by RepoServices.

The harness requires `BOUNDED_PUBLICATION_SCHEMA_SHUTDOWN_OK` alongside the existing
library/RPC cancellation, retained history, Redis restart and delayed-write
recovery cases. No production code changed. These are local test results, not
hosted CI, a main merge or deployment. Public factory qualification remains open.
