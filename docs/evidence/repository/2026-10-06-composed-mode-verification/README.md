# Composed scoped mode verification, 2026-10-06

The scoped `requireObservedModes` path now captures preparation and modes together
under the current owner, claim and command checks. It decodes outside SQL locks,
compares the requested modes, and checks live authority again before returning a
successful comparison. This removes nested loader transactions from the scoped
comparison without exposing private preparation or recovery state.

No protobuf, migration, or provider behavior changes. The standalone private
preparation and modes loaders retain their separate delivery checks. The shared
capture helper retains the existing bounded query and exact encoded-size
reservation. Modes reserve their existing maximum allowance only when a
preparation exists. Both reservations close after success or any exception.

## Focused proof

Base: `8e53892d22d663660a937a6d7707b1cfae23d132`.
`tested-source.patch.gz` and `source-sha256.txt` identify the changed production
and test sources for this checkpoint.
The new actual-JDBC commit-count test first failed on that implementation:
expected two commits, observed six. `red.xml.gz` retains that failure.
After the production change, all 52 tests in the following three suites passed,
with zero failures/errors/skips (`focused-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentInitialAdmissionIT' --console=plain
```

Build passed in 1m33s. Mode verification has 22 cases; preparation loading has 23;
initial admission has seven. The exact-match scoped comparison commits twice,
using a budget of one modes allowance plus the actual encoded preparation size.
A mismatching map is refused after its one capture commit. The explicit claim-only
primitive uses one fenced transaction with a one-byte budget and no reservation.
These counts concern mode verification, not entire publication.

New corruption cases deliberately damage actual PostgreSQL rows and disable
immutable-write/owner guards in the isolated fixture so the Java integrity path is
exercised. Missing modes, wrong members, non-string mode values, wrong nonce,
corrupt preparation, and corrupt preparation plus absent modes are refused.
Preparation corruption retains priority over missing modes. Normal SQL write
protection is covered by the existing immutable-write tests.

Real post-COMMIT gates hold the first capture acknowledgment while another
connection transfers the expired claim or the owner expires. Delivery is refused.
Cancellation after capture also refuses delivery. Every case checks released byte
reservations. A budget that admits the modes allowance but cannot admit preparation
also fails and releases the modes reservation. Existing private-loader authority,
claim transfer, mutation protection and initial-admission tests remain green.

## Scope of the performance claim

This proves a reduction from six to two committed transactions in successful
scoped mode comparison. It is not a four-transaction reduction in every repository
operation, a measured latency improvement, or horizontal scaling qualification.
The RustFS callsite trace motivates the change; controlled workload comparisons
remain necessary before making performance claims. Production-JAR and affected
session qualification are tracked separately below.

## Affected session qualification

All 38 cases passed in `DocumentPublicationSessionIT` (17),
`DocumentScopedRegistrationIT` (seven), `DocumentAssessmentStartJournalIT` (nine),
and `DocumentPublicationRegistrationInspectionIT` (five), with no failures/errors/
skips. Their JUnit XML files are retained in `affected-green.tar.gz`. Together with
the focused suites, 90 SQL regression cases passed. Sol reviewed the source and
tests after the focused run and found no blocker.

## Production-JAR and RustFS qualification

The production-JAR `admissionStorageTest` passed with zero failures/errors/skips
(test time 215.427 s; `production-jar-green.xml.gz`). The subsequent journaled,
trace-enabled `nativeReplicaBenchmark` passed all twelve windows (test time
119.698 s; `rustfs-trace-green.xml.gz`). The combined affected-tests/storage/benchmark
build passed in 7m8s. Both runtime tasks use the real packaged runtime and providers.

The RustFS command used the same four clients, eight iterations per client, eight
total read slots and 32 total handles as the preceding trace. The raw directory
`938b8447-33f4-4131-8494-641201b05e94` is retained in `rustfs-trace-raw.tar.gz`.
`measured-totals.csv` uses the same columns and aggregation command as the preceding
[callsite trace](../2026-10-06-journaled-callsite-trace/README.md). Aggregate/detail
conservation and real outcome/history/replay checks passed again.

Across 144 valid publications, scoped mode verification now has 288 connection
usages, all at `requireObservedModes`, versus 864 across that method plus nested
preparation/modes loading before the change. Rejections have 192 usages at
`requireObservedModes`; their standalone preparation and modes loads each dropped
from 288 to 96 usages. Those remaining private-loader calls retain their independent
boundaries. The reduced call counts agree with the focused actual-commit proof.

This run recorded 6,465 aggregate acquisitions, zero reported metric failures, and
about 38.6 ms summed acquisition waiting. It is a short sequential observation on
an unisolated host (startup load averages 14.16, 27.10, 20.82). It does not establish
latency improvement, maximum throughput, or horizontal scalability. Trace overhead
and asynchronous unscoped work retain the limitations recorded in the prior trace.
