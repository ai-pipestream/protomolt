# Historical CREATE reconciliation

Status: expanded packaged gate and focused regression gate passed locally.
Base: `be1e8ee087720947ba7e8da570b3fc16f7dabde0`, with the implementation and probes
identified by `sources.sha256`. No protobuf changes.
Sol reviewed the production lock order, identity and authorization boundaries, and
reviewed the negative probes. No blocking review findings remain in the current code.

## Behavior under test

The execution retains its exact attempted CREATE tuple before SQL: acknowledged START
identity and deadline, manifest hash and selected upload attempts. Reconciliation
requires the same retained execution, assessment identity, source Work and modes.
It verifies current authority, policy, durable evidence and expiry. Only a verified
committed stage is adopted by the installed owner. Empty observation and failures
preserve the sticky CREATE flag; neither permits another CREATE.

A dedicated transaction retains the established assessment-owner-before-origin lock
order. Current source authorization is rechecked at delivery. This is reconciliation
of one retained attempt, not cold restoration or permission to publish without the
publication path's own checks.

## Required evidence

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --max-workers=2 --console=plain
```

The final expanded packaged run exited 0, BUILD SUCCESSFUL in 9m47s. Its aggregate
JUnit result is one test, zero failures/errors/skips, 585.129 seconds. The complete
initial and supplemental host logs, terminal JUnit XML and Gradle output are archived
beside this record. The source hash check passed after completion.
An earlier packaged run passed in 9m46s,
with one aggregate test and zero failures/errors/skips, before the changed-manifest
and explicit-release cases were added. That earlier result does not qualify the
expanded source. The focused SQL suites exited 0, BUILD SUCCESSFUL in 48 seconds:
28 tests, zero failures/errors/skips (7 installed-owner, 9 capture-disposal and
12 successor-activation tests). Their reports and Gradle output are archived.
Those SQL fixtures use synthetic provider observations for source setup; real
provider evidence comes from the packaged run, not the focused tests.

The packaged probes use real PostgreSQL and versioned provider transfers through
production JARs. JDBC fault injection causes a real rollback or loses a real commit
reply. Assertions cover:

- Direct rollback returns no stage; direct committed reply loss recovers the original.
- The installed owner retains uncertain CREATE across client calls and can reconcile,
  publish and verify exact receipts, provider bytes and provenance afterward.
- Changed selections refuse. A second assessment prepared by the same execution with
  different evaluation time has a different manifest and refuses; the original still
  reconciles after that refusal.
- Revoking the bound credential refuses with UNAUTHENTICATED and publishes nothing.
- A real assessment-row lock blocks reconciliation until the database clock passes
  the deadline. Adoption refuses after the wait, with no publication. The accepted
  retained-row conflict does not identify which expiry check detected the condition.
- Guarded SQL recovery releases that expired assessment. Reconciliation then returns
  empty, another CREATE refuses, and no assessment or publication is recreated.

The existing first host keeps its 210-second cap. New installed-owner negatives run
in a separate 90-second host, using a separate database in the same container, after
all lease-sensitive restart and cleanup checks. The provider bucket is shared; object
identities are independent. Existing scenarios are still required. The split followed
an aggregate timeout after the new cases were appended to the original host; no
operation deadline was increased. The expiry fixture explicitly allows a 35-second
SQL lock wait and 45-second statement window around its deliberate 20-second hold.
Production SQL/provider defaults are unchanged.

The first revocation run failed because its test expected PERMISSION_DENIED; the
production credential boundary correctly returned UNAUTHENTICATED. Only the test
expectation changed. Earlier runs and partial host logs are not final gate evidence.

## Limits

This remains a private installed-plan path. Public historical routing, normal terminal
retirement, ownership before reservation/installation, and complete managed library/
gRPC qualification remain unfinished. This record does not establish hosted CI,
pruning completeness, backup completeness, horizontal scaling or goal completion.
