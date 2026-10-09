# Journaled RustFS diagnostic, 2026-10-06

Local tests passed for both runtime modes. This is a short diagnostic to guide
instrumentation, not capacity, horizontal scaling, or an atomic-admission speedup
claim. No production code changed in this checkpoint.

## Source and execution

Base: `dc1ca5dd8ec0b1026dbc3ff20805a5767a50b218`, plus `test-source.patch.gz`.
`source-sha256.txt` identifies the changed test/build sources used by both runs.
The patch adds an explicit opt-in to the existing native benchmark; the default
remains unjournaled. The parent records the mode and checks each worker's recorded
mode. Both paths have the same limits, pools, assessment worker, and payload.

Run each mode sequentially (true first, false second):

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkJournaled=true \
  -PnativeBenchmarkClients=4 -PnativeBenchmarkReadSlots=8 \
  -PnativeBenchmarkReadHandles=32 -PnativeBenchmarkIterations=8 \
  --console=plain
```

Repeat with `-PnativeBenchmarkJournaled=false`. Builds passed in 1m58s and 1m56s,
respectively. The retained JUnit XML files each report one passed test, zero
failures/errors/skips. Each test contains twelve windows over one, two, and four
independent worker JVMs sharing real PostgreSQL and RustFS. It checks operation
outcomes, exact replay, historical bytes and identity, durable counts, provider
counters, and absence of reported metric failures.

The environment was Java 25.0.3, Linux amd64, PostgreSQL 18.6 and
`rustfs/rustfs:1.0.0-beta.11-preview.1`. Worker heap limit: 512 MiB. Four clients
in total, eight iterations per client per window, original small fixture payload,
eight total read slots, 32 total read handles. SQL pools are either eight total
connections divided between workers or eight connections per worker. Host load
was not isolated; no container CPU or memory limits were applied. Exact startup
load averages and worker settings are retained in each raw archive.

## Observations and limits

Each mode measured 384 primary operations: 192 reads, 144 valid publications,
and 48 admission rejections. Warmup is excluded. `latency.csv` records counts,
means and nearest-rank p50/p95 separately by operation and topology. Only 8 or
16 rejection samples exist per topology, so tail estimates are particularly weak.

For the single-worker topology, journaled versus unjournaled means were 203.29
versus 166.39 ms for publication, 328.95 versus 266.22 ms for rejection, and 36.51
versus 37.87 ms for historical reads. These sequential mode runs are not an
interleaved old/new implementation experiment. They neither quantify the effect
of atomic initial admission nor justify weakening journal durability.

No topology with more workers improved aggregate throughput in this low-load
fixture. Rates pooled by topology and pool size, expressed as journaled primary
operations per inclusive window second, ranged from
18.38 to 27.16; unjournaled from 20.91 to 32.56. These rates include test replay,
checks and synchronization in the window denominator, although individual
operation latency ends before replay. They are not maximum service request rates.
The private journaled factory also does not qualify managed-host worker lifecycle,
public transport overhead, authenticated access, or automatic recovery hosting.

`metrics.csv` sums completed provider and connection-pool callbacks over measured
windows. These counters also include replay and maintenance. Journaled mode
recorded 7,425 connection acquisitions, unjournaled 5,449; neither count is a JDBC
commit count. Total acquisition waiting was about 37 ms versus 41 ms, summed over
all workers/windows. Connection usage was about 41.25 s versus 34.45 s, also summed
and not exclusive request wall time. Provider PUT calls were 192 in each mode.
There were no reported metric failures. SQL sampling found no nonempty wait type
in the journaled run; periodic sampling can miss short contention. Query snapshots
identify statements, not causal time attribution.

Sol reviewed the harness, focused commit-count test, analyzer and retained evidence
and found no checkpoint blocker.

Next: measure journaled publication stages and their actual SQL work before
choosing an optimization. The evidence supports investigating repeated database
work, but does not prove which stage dominates or rule out assessment-worker,
provider, scheduler, or host-load effects. Larger controlled RustFS runs remain
required for capacity and horizontal scaling.

## Reproduce the summaries

Extract both `*-raw.tar.gz` archives to a scratch directory. Run:

```sh
java Summarize.java \
  /scratch/f447843c-8731-46de-b695-48996a2a3615 \
  /scratch/fd52aa54-db08-487a-bac2-d907343b8c05 \
  > latency.csv 2> metrics.csv
```

The dependency-free Java analyzer checks per-window primary operation counts,
groups the fixed twelve-window fixture by mode/replicas/pool, and calculates
throughput as total primary operations divided by summed inclusive window time.
It does not average window rates. Raw archives retain operation rows, pool/provider
counters, SQL snapshots, sampled waits, worker logs and configuration. The focused
actual JDBC commit-count proof for `session.admit` is separately retained in
`../2026-10-06-atomic-initial-admission/`.
