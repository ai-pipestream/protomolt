# Historical ownership before reservation and installation

Base `b2804af1c14d74a26cd10d472c83684b5f0c891e`; exact changed Java sources and
packaged probes are recorded in `sources.sha256`. No protobuf changes. Sol reviewed
the phase ownership, retention verification, command identity fix, tests and provider
probe cleanup without a remaining blocking finding.

The private historical owner now retains a proposal before reservation and keeps
loaded preparation plus the exact generated plan across uncertain installation replies.
The same entry later owns source Work, execution, START, assessment, CREATE and
publication. A retry never reconstructs an installed plan from a fresh discovery.
Requested modes and complete upload bytes are checked before mutation. The original
historical preparation and initial capture anchor are verified before reservation,
using the same binding check as activation; activation keeps its existing SQL locks.

## Validation

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalPreparationIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 11m55s. The five focused suites contain 49 cases
(11 preparation, 7 installed owner, 10 retirement, 12 activation, 9 disposal), with
zero failures/errors/skips. The packaged gate is one additional aggregate test,
617.569 seconds, zero failures/errors/skips. Reports, four host logs and full Gradle
output are archived here; the final source hash check passed.

Focused tests use real PostgreSQL, with synthetic provider observations only for
archived-source setup. Actual post-commit JDBC `08006` failures plus interrupted
confirmation establish uncertain V97/V93 outcomes; exact retry preserves claim,
incarnation, owner and preparation identities. Other cases qualify fresh-owner V98
recovery, invalid retention/mode refusal before writes, capacity refusal, and local
shutdown at each pre-execution phase without invented capture-drain records.

The provider probe was changed only for the existing installed-owner route. It creates
one owner before a fresh history capture, rejects missing/checksum-corrupt resubmitted
bytes with zero V97/V93 rows, reserves in one call and installs in the next, then
resumes that same entry for the existing multi-call assessment and publication.
Actual provider uploads/readback, exact receipt identity, normal retirement and budget
refunds remain required. Both the initial host and supplemental reconciliation host
require the new preparation marker and successful process completion. The latter also
qualifies CREATE reply-loss reconciliation, revocation, expiry and released assessment
refusal through the new preparation path. No process cap or production timeout was
increased; the deliberate assessment-expiry fixture retains its existing SQL limits.

The tests exposed a real historical source binding defect: Java object equality
rejected a command decoded from the retained journal. The fix compares operation ID
and canonical bytes. A dedicated negative test proves that identical semantic bytes
under a different operation ID still refuse. Proposed-owner retries likewise compare
the encoded retention digest when resubmission reconstructs its Java objects.

Earlier bring-up failures included ambiguous test imports, an incorrect expected
capacity exception, and the command-identity defect. The V98 fixture cleanup was
corrected to detach the owner before releasing its retained history. Those failed
runs are not final qualification. The earlier unchanged-provider regression passed
separately but did not exercise the new preparation entry point; this expanded gate does.

## Remaining work

Public historical routing remains disabled. This checkpoint does not implement the
same owner's pending supersession of its own expired reservation, recovery after an
uncertain attached activation, or complete managed library/gRPC parity. Reservation
preflight authorization and private reservation writes are separate transactions;
loading and activation reauthorize, but the required atomic revocation boundary still
needs an explicit decision and qualification before public dispatch. Conservative
retained preparation reservations remain a memory-capacity optimization to assess.

This is local evidence, not hosted-CI, scaling, pruning, JCR or full-goal completion.
