# Managed publication transport permit-drain qualification

Candidate base: `6c94d03ef023d7e00f48b4a3b2faef2ed58bfd9b`. The first source change
is in `repo/container/src/test/resources/runtime-inventory/ManagedJournaledDrainProbe.java`;
the follow-up adds a test-only retention bound in `AssessmentOperationReplayProbe.java`.
The `source.sha256` files record the exact relevant source bytes for each run.

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

## Follow-up hosted failure

Run [37705064308](https://github.com/ai-pipestream/protomolt/actions/runs/37705064308)
tested candidate `6f2714fc164619f4d4358dbbe8174015b8c8feb9`. The earlier
`ManagedJournaledDrainProbe` assertion no longer fails. The archived JUnit XML
instead records `RejectedAssessmentRestartProbe.main` failing when
`captureRejectedAssessment` reports `Retained rejection evidence is unavailable`.
`ci-source-6f2714fc1.sha256` identifies the relevant test sources at that exact
head.

The rejected writer's operation lease is deliberately 60 seconds. Its assessment
retention deadline is 300 seconds (`AssessmentOperationReplayProbe.java`, scenario 1).
The replay host runs scenarios 0 through 7 before polling the owner lease for up to
65 seconds, then starts the fresh rejected-evidence reader. This run's storage test
lasted 339.239 seconds; its rejected-reader Hibernate log begins at 01:11:42Z, about
5m37 after the JUnit test began at 01:06:05Z. The reader correctly refuses evidence
whose own retention deadline has elapsed, even though the writer lease has expired.
This is a fixture-window mismatch indicated by the source and timestamps, not evidence
that expired evidence should be readable. The reader's availability checks and
writer-lease-expiry assertion must remain intact.

The scenario-1 retained assessment was created with a five-minute deadline, but its
fresh reader is deliberately deferred until after the replay subprocess (90s maximum),
aggregate SQL host (210s), restart acknowledgement host (75s), and rejected-reader
process (30s). These configured child-process limits total 405 seconds after assessment
creation, before allowing cold-start margin. The local filtered run passed, but its
successful host output did not preserve those internal phase timestamps.

The test-only correction gives scenario 1 fifteen minutes of evidence retention. That is
900 seconds, more than twice the explicit 405-second process budget, with cold-start
headroom. It does not extend the 60-second operation-owner lease or permit reading after
assessment evidence expires. The separate 20-second expiry case is unchanged.

The updated filtered production-JAR host test passed locally: one test, zero failures,
errors or skips in 617.621 seconds. See `local-pass-after-retention.xml`,
`source-after-retention.sha256`, and the second result in `gradle-result.txt`. Hosted
checks for the follow-up commit are still required.
