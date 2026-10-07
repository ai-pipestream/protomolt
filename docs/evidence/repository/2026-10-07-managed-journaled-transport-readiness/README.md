# Managed publication transport permit-drain qualification

Candidate base: `6c94d03ef023d7e00f48b4a3b2faef2ed58bfd9b`. The only source
change is in `repo/container/src/test/resources/runtime-inventory/ManagedJournaledDrainProbe.java`.
`source.sha256` records the exact modified probe bytes used by the local run.

## Hosted failure evidence

GitHub run [37696781515](https://github.com/ai-pipestream/protomolt/actions/runs/37696781515)
tested SHA `6c94d03ef023d7e00f48b4a3b2faef2ed58bfd9b`. Console, integration and
conformance passed; `build (25)` failed in the required Repository storage host
tests. The archived XML is from artifact `repository-test-diagnostics-jdk-25-1`
(artifact ID `11517594737`). The child process failed at
`ManagedJournaledDrainProbe.executeTransport`, source line 549: the fixture expected
`INVALID_ARGUMENT` for a malformed request and received `RESOURCE_EXHAUSTED`,
"Publication transport is full." The fixture configures one active call. The
preceding successful publication/replay calls return client completion before the
server's close callback is guaranteed to have released the permit. The status failure
therefore happened at admission, before the malformed request reached validation.

This is a test-ordering diagnosis from the archived failure and service lifecycle
source, not a production transport defect finding. The server marks producer completion
in `finally`; permit release requires both producer completion and a gRPC terminal
callback. The fixture now waits at that boundary, with a five-second bound and a
required successful `awaitIdle`, before retaining the original `INVALID_ARGUMENT`
assertion. The one-call capacity and all saturation assertions are unchanged.

## Local qualification

Exact command and captured terminal result are in `gradle-result.txt`; the complete
JUnit XML is `local-pass.xml`. The filtered production-JAR host run passed one test
with zero failures, errors or skips in 619.119 seconds (Gradle completed in 10m24s).
The hosted failure XML is retained as `hosted-failure.xml.gz`.

This local result qualifies the fixture change only. The new commit still requires
current-head GitHub CI. No target ref was changed by this qualification.
