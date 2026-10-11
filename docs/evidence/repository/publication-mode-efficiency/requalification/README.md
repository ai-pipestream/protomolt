# Publication-mode validation: requalification with the post-decode recheck

Source under test: branch `agent/publication-mode-efficiency-requalify`, commit
`4be2f0a9333b2a51970acf25ac366092a2a5ed90` (main `98f4e0cbd` plus the reapplied
single-transaction change `a1f332ee3` plus the recheck of claim, owner and command
after the decode). Baseline: main `98f4e0cbd0800bbe6bca61c8426921cbfaa0d02a`, in
which the single-transaction change is reverted. Every tree was clean of tracked
changes at run start (`series.log`). The earlier evidence one directory up is kept
as the record of source `1c9faac60`; it does not describe this source.

Host: Linux 7.0.0-38-generic, 32 CPUs, Java 25, Gradle 9.6.1, Docker with cgroup
v2, no CPU or memory limits; shared with other work. Every step held
`/tmp/protomolt-repository-qualification.lock` (lock wait 0 s for all fourteen).

## The 16 MiB preparation under the lock

`PublicationModeEfficiencyIT.maximumPreparationLockWindowIsMeasured` builds a
canonical preparation of exactly 16,777,216 bytes, the codec and column bound,
through the production record constructor, codec, `DocumentPublicationPreparationJournal.save`
and `DocumentPublicationModesJournal.bind`. Shape: 17 members (one uploading), each
on its own ACTIVE drive of the account; each drive's free-form `metadata` JSON is
padded to at most 1 MiB, the codec's per-text bound, until the encoding lands
on the bound; the drives are read back from PostgreSQL before encoding. Modes:
372 bytes. Constraints that shape the reachable maximum: `drives.metadata` and
`provider_config` are JSONB without a size check, so a placement can carry up to
1 MiB per text field; placements are bounded to 64 drives and the command to
1 MiB; the total is capped by the codec at 16 MiB, so the bound is reachable
with seventeen or more drives carrying large metadata. No integrity guard was
disabled.

Each run makes six consecutive calls; the first is the JVM's first decode of a
preparation this size (`max-cold-*` ran the test alone in a fresh Gradle test
JVM; the fourth row is the same test inside the regression run). Lock window:
from the first claim fence statement to the return after commit. Decode
window: from the rows read to the post-decode claim fence. Allocation: the
calling thread's allocated bytes over the call (JDBC fetch, digest, decode,
canonical re-encode). Every call: 1 acquisition, 1 commit, 8 statements,
reservation 16,777,588 bytes while held and 0 afterwards.

| Run | lock window ms (call 1, calls 2 to 6) | decode ms (call 1, calls 2 to 6) | allocated MiB per call | load at test |
|---|---|---|---|---|
| `max-cold-1` | 110.8; 104.9, 98.6, 102.7, 99.9, 63.6 | 71.4; 66.5, 61.8, 63.3, 61.2, 48.4 | 228 to 230 (196 last) | 3.27 |
| `max-cold-2` | 112.7; 107.3, 99.7, 105.2, 97.9, 61.0 | 73.0; 68.1, 61.3, 63.2, 60.1, 45.1 | 228 to 230 (196 last) | 3.68 |
| `max-cold-3` | 104.4; 102.6, 97.1, 99.8, 94.7, 58.3 | 68.1; 65.8, 60.8, 63.9, 58.5, 44.4 | 229 to 230 (196 last) | 3.46 |
| in `regression-publication-replay` | 88.6; 101.8, 87.0, 107.9, 85.6, 64.8 | 53.6; 65.9, 52.0, 74.0, 51.8, 43.6 | 228 (196 last) | 3.12 |

So at the bound the claim and owner rows of that one operation are held for
about 60 to 115 ms, of which the decode is 44 to 74 ms. At the fixture size
(2,011-byte preparation) the decode window is 0.4 to 1.9 ms
(`decodeWindowInsideTheFenceIsMeasured`). The allocation is the decode's cost
wherever it runs; the baseline paid it outside the lock and was not measured
here.

`heldMaximumValidationBlocksOnlyItsClaimAndTheTakeoverWaitIsBounded` holds the
same 16 MiB shape after the rows read. A comparison on another claim in the same
schema (15,404-byte preparation) completed meanwhile in 16.6 ms (23.1 ms in the
first focused run). The held claim then expired; a takeover was observed waiting
on the claim row lock; after release the 16 MiB decode ran, the post-decode
claim fence refused (`Fenced`, 6 statements, 0 commits, 1 rollback, owner
`write_fence_xid` unchanged) and the takeover completed 60.8 ms after release
(58.5 ms in the first focused run), with epoch 2.

## Round trips per operation (traced run, window t00, one process, 16 clients)

`summary-*-trace/trace-sites.csv`, 192 publications and 64 rejections:

| Per operation | baseline publish | candidate publish | baseline reject | candidate reject |
|---|---|---|---|---|
| scoped `commit()` | 16 | **15** | 35 | **33** |
| pool acquisitions | 16 | **15** | 35 | **33** |
| `executeQuery` | 143 | **141** | 267 | **263** |
| `executeUpdate` | 31 | 31 | 32 | 32 |
| `executeBatch` | 3 | 3 | 1 | 1 |

At the site, per window of 192 publications: `requireObservedModes` commits
384 → 192, commit call time 1682 → 658 ms, acquisition wait 1312 → 661 ms,
connection usage 2001 → 1052 ms. The fence and command statements are 1920 each
in both runs: the baseline's trailing transaction and the candidate's
in-transaction recheck issue the same number. The rows reads moved from
`DocumentPublicationPreparationJournal.capture` (384) plus
`DocumentPublicationModesJournal.readModes` (192) to
`DocumentPublicationModeValidation.capture` (384). Compared with source
`1c9faac60` (one directory up), the recheck adds three statements per call and no
commit or acquisition.

## Throughput and latency, three repetitions each, interleaved

`summary-baseline/configs.csv` and `summary-candidate/configs.csv`, operations per
inclusive window second, mean (min to max); latency nearest-rank, ms:

| Processes × pool | baseline ops/s | candidate ops/s | baseline publish p50/p95/p99 | candidate publish p50/p95/p99 |
|---|---|---|---|---|
| 1 × 8 | 81.4 (76.4 to 86.9) | 83.3 (79.8 to 87.2) | 266 / 317 / 341 | 258 / 304 / 320 |
| 2 × 4 | 76.0 (71.4 to 81.0) | 76.8 (75.0 to 81.0) | 284 / 336 / 355 | 276 / 324 / 344 |
| 2 × 8 | 98.4 (97.4 to 100.4) | 100.8 (98.6 to 103.5) | 231 / 263 / 281 | 224 / 254 / 264 |
| 4 × 2 | 70.5 (68.1 to 72.7) | 70.6 (69.5 to 72.4) | 296 / 346 / 377 | 291 / 346 / 379 |
| 4 × 8 | 90.0 (87.3 to 92.8) | 92.9 (89.3 to 95.2) | 248 / 293 / 323 | 241 / 278 / 297 |

Every candidate mean is at or above its baseline mean, and every range overlaps,
so the throughput and latency change is not distinguishable from run-to-run
variation. The verified claim is the round-trip reduction, not a latency gain.
One-minute load at window start: baseline 4.0 to 13.2, candidate 8.0 to 13.6.

## Suites (source `4be2f0a93`)

| Run | Task | Result |
|---|---|---|
| focused, before archiving | `PublicationModeEfficiencyIT`, `DocumentPublicationModesJournalIT` | 25 + 22 tests, 0 failures, 0 skipped |
| `max-cold-1..3` | `test --rerun --tests PublicationModeEfficiencyIT.maximumPreparationLockWindowIsMeasured` | 1 test each, 0 failures |
| `regression-publication-replay` | `test --rerun --tests '*Publication*' '*Replay*' '*Journal*' '*Successor*' '*ReservedPreparation*' '*ScopedRegistration*'` | 93 classes, 632 tests, 0 failures, 0 errors, 0 skipped |
| `admission-storage` | `:protomolt-repo-container:admissionStorageTest` | `DocumentAssessmentStorageRuntimeTest` 5 tests, 0 failures, 0 skipped, 1144 s |
| `native-replica-gate` | `:protomolt-repo-container:nativeReplicaTest` | `NativeReplicaRuntimeTest` 1 test, 0 failures, 0 skipped, 31 s |
| `baseline-*`, `candidate-*` | `nativeReplicaBenchmark`, journaled, gate first | exit 0 in all eight runs |

The focused run's XML is not archived separately; its printed lines are quoted
above and every case ran again inside `regression-publication-replay`.

## Identity

`source-identity.txt` in every `raw.tar.gz` lists the same artifacts, compiled
probe (`71bda593ce00…`), probe sources, parent sampler and images
(`postgres:18-alpine` `sha256:77f585114c32…`, `rustfs/rustfs:1.0.0-beta.11-preview.1`
`sha256:ea50257bc5e2…`) for both series; the only differing line is the
container jar: baseline `5454a883fb21…` (the same bytes as the earlier baseline at
`5f8acfb41`), candidate `e18a892d8b13…`.

## Real, synthetic and injected

- Real: PostgreSQL and RustFS in containers, every statement, lock, commit,
  rollback, trigger and constraint; the production jar in separate JVMs.
- Synthetic: fixture publications; the 16 MiB shape is legitimate but built for
  the test (large drive metadata), not observed in production.
- Injected: barriers after the rows read, short leases with `pg_sleep`,
  `SQLException` on the rows read, the post-decode fence and the commit.

## Limitations

- CPU was not isolated. Lock-window numbers are wall time on a shared host and
  are not asserted as limits by any test.
- The baseline's decode outside the lock was not timed at 16 MiB.
- The 16 MiB shape combines maximal placements with a small command; a near-1 MiB
  command with a smaller placement share is not separately timed.
