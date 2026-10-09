# Redis document revisions across restart

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 3m40s. Sol reviewed the fixture with no blocker.

A replacement changes the typed payload at the same document address using the
expected mutation revision. The test checks a new revision UUID, a higher mutation
revision and the original receipt on replay. Both documents are compared with
local and authenticated in-process gRPC historical results after live schema
resolution closes. Both requests, receipts and documents are saved and checked
again in a fresh JVM after a graceful Redis restart, without a live registry.
The harness also verifies the initial fixed port mapping.

This proves preservation of both revision contents across replacement and restart.
It does not inspect physical object identifiers or establish zero provider I/O
on replay. Interrupted writes and active-operation shutdown remain separate cases.
