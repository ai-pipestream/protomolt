# Journaled callsite tracing, 2026-10-06

The opt-in test-only callsite trace passed all twelve native RustFS windows with
four clients and eight measured iterations per client. Production code is unchanged.
This diagnostic identifies where completed pool/provider callbacks occur; it does
not measure JDBC commit counts or exclusive transaction/statement execution time.

## Reproduction and provenance

Base: `e7229dcf7617a8a6314e1df561d29d87b3cdadee`, plus the compressed
`trace-tested.patch.gz`. The subsequent disabled-scope fix only makes operation
scopes no-ops when tracing is false. The trace run used:

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkJournaled=true -PnativeBenchmarkTrace=true \
  -PnativeBenchmarkClients=4 -PnativeBenchmarkReadSlots=8 \
  -PnativeBenchmarkReadHandles=32 -PnativeBenchmarkIterations=8 \
  --console=plain
```

Build passed in 2m3s. `trace-test.xml.gz` retains the JUnit result.
The follow-up without `nativeBenchmarkTrace` passed all twelve windows in 2m5s,
verifying the disabled-scope change. Its source delta from the same base is
`trace-disabled-tested.patch.gz`, with `trace-disabled-test.xml.gz` and the full
`trace-disabled-raw.tar.gz` directory `4bdc4c7e-c8c2-439f-be83-b8cb5f813624`.
The two source patches are alternatives against the base, not sequential patches.
Disabled scopes now reuse a no-op object without per-operation allocation or
ThreadLocal writes. Other measured-path instrumentation is enabled only by the
explicit trace flag.

`trace-raw.tar.gz` contains the complete raw directory
`733a3c51-fe98-4668-a0db-a308477c4da2`: environment and worker configuration,
primary operation rows, aggregate metrics, detailed trace, SQL snapshots, sampled
waits, and logs. Backend/version/configuration match the adjacent diagnostic:
PostgreSQL 18.6, RustFS beta.11-preview.1, Java 25.0.3, small synthetic fixture,
unisolated Linux amd64 host. This is not a capacity benchmark.

The mode is explicit in parent/worker configuration. Every snapshot verifies that
detailed counts, nanoseconds and failures sum exactly to aggregate metrics under
one short telemetry lock. Both warmup and measured CSV pairs use those same coherent
snapshots. Parent assertions require scoped SQL acquisitions for read, publish,
reject and replay, with zero reported trace failures. Real provider calls and
existing durable-outcome/history/replay checks still execute.
Sol reviewed the trace, disabled-scope follow-up, evidence and proposed mode-read
composition and found no checkpoint blocker.

## What is attributed

The primary and replay calls have separate thread-local scopes. SQL acquisition
and connection-usage callbacks capture the first production ledger stack frame,
excluding `Tx` and native test-harness frames. Provider completion callbacks use
the same rule. A callback on another thread remains `unscoped`; missing ledger
frames are explicitly `outside_ledger`. There is no inferred context propagation.
The label is the callback's completion site, not necessarily the work's origin.

Stack walking and the shared trace-update lock add diagnostic overhead. Do not
compare these latencies against trace-disabled samples as a speedup/regression
measurement. Hikari connection-usage durations can include application work while
a connection is borrowed and are millisecond-resolution observations. Provider and
SQL durations may overlap; summing them does not yield end-to-end latency. A scope's
rows do not include all asynchronous work initiated by that scope.

## Findings that guide the next change

Across 144 measured successful publications, the mode-verification path reported
288 connection usages in `DocumentPublicationModesJournal.requireObservedModes`,
288 in `DocumentPublicationPreparationJournal.load`, and 288 in
`DocumentPublicationModesJournal.loadRetained`: six per successful publication.
Their summed usage durations were 1.093 s, 0.936 s and 0.863 s. Source inspection
confirms nested capture/decode/delivery boundaries. These are not six JDBC commits
proven by this trace; an actual commit-count test is needed for that claim.

A candidate improvement is to capture bounded preparation and modes together under
one live owner/command/claim fence, decode and compare outside SQL locks, and check
live authority again before returning. It must preserve corruption detection,
private state containment, byte reservations, caller/command/predecessor/owner
bindings and cancellation. The explicit claim-only primitive must remain distinct.
No such production optimization is included here.

Other prominent sites include publication commit (144 usages, summed 6.514 s),
selected-attempt renewal (432, 2.705 s), upload staging (144, 1.954 s) and initial
admission (144, 1.728 s). There were also 192 actual provider PUTs, all attributed
to unscoped `DocumentPartTransfer.upload`, with summed 7.805 s duration. Those
numbers justify further attribution, not removing fences or recovery evidence.

## Recompute callsite totals

`measured-totals.csv` has no header. Columns are operation, metric, completion site,
count, summed nanoseconds and failures. From the extracted raw directory:

```sh
awk -F, 'FNR>1 {k=$1 FS $2 FS $3; count[k]+=$4; nanos[k]+=$5; failures[k]+=$6} END {for (k in count) print k FS count[k] FS nanos[k] FS failures[k]}' \
  /scratch/733a3c51-fe98-4668-a0db-a308477c4da2/*-measure-trace.csv \
  | LC_ALL=C sort -t, -k5,5nr > measured-totals.csv
```

These totals pool all topologies for callsite-count investigation. They are not
per-topology scaling statistics. Warmup and measured output remain separate.
