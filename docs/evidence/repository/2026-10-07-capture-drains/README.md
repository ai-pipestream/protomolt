# Durable capture-drain qualification

Base `c9e7ae2b6d67aa1b25f0588c655ebfdba1b3fba2`, branch
`refactor/repository-composition`, plus this checkpoint. `source-sha256.txt`
identifies tested source, migration and test files. Sol reviewed the lifecycle,
SQL lock order, failure handling and final wait-control refinement.

## Focused qualification

Exit 0, 43 seconds, 27 tests, zero failures/errors/skips:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --tests '*DocumentHistoricalSourceDrainIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentPreparationPinOwnerMigrationIT' \
  --tests '*DocumentCapturedPinReuseIT' \
  --max-workers=2 --console=plain
```

The new capture-drain suite has 18 cases. It covers held Work and aliased Uses,
unrelated same-reader handles, expired and transferred SQL claims without renewal,
ACTIVE/FENCED recovery refusal and QUIESCED acceptance, failed actual SQL pin cleanup,
lost marker acknowledgement with ordinary and bounded Tx, public caller refusal,
cancellation after marker commit, concurrent completion, deadline/cancellation while
Work remains held, refused registration versus lost registration acknowledgement,
populated V106-to-V107 migration, wrong owner insertion, and orphan mirror detection.
The other suites contribute 3, 2, 1 and 3 cases respectively. See `focused/`.

Sol found a bounded-Tx error: confirmation initially called `readOnly`, which rejects
timeout-configured views. The bounded lost-acknowledgement regression failed with
that exact error, then passed after confirmation switched to `inTransaction`.
See `bounded-red/`. There were also two test compilation corrections (overloaded
transaction lambda and a non-functional control interface), recorded in `initial/`.
An initial runtime test reached the intentionally gated historical takeover API;
the final test exercises the guarded SQL epoch transition instead. It does not
enable or qualify public historical successor execution.

All SQL is real PostgreSQL 18. The publication fixture supplies synthetic provider
observations; these focused cases do not prove remote provider-effect quiescence.
The orphan-mirror case deliberately disables the mirror trigger in its isolated
database to create corruption, restores it, then verifies refusal and retry after
repair. Normal pin release remains atomic. Local completion covers enrolled work,
not arbitrary external workers or queued tasks that lack lifecycle enrollment.

## Production storage regression

The broader production-JAR storage suite passed, exit 0, in 7 minutes 44 seconds:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

This is one JUnit integration scenario with multiple asserted production-JAR probes,
not one JUnit case per printed marker. It uses actual PostgreSQL, versioned LocalStack
object storage and Redis, including bounded library/gRPC invocation, restart,
revocation, forced-crash recovery and lease-expiry cleanup. See `storage/` for the
command output and JUnit result. Its runtime inventory contains 38 artifacts.
This verifies the existing production storage path against the new migration; it
does not turn the new private capture completion into a public historical API or
replace the focused capture lifecycle tests. No RustFS performance measurement was
run, and these correctness timings are not a latency/throughput claim.

All retained JUnit XML had trailing whitespace normalized without changing test
values. No executable source changed after the final focused or storage runs.

## Scope

V107 records exact captured-source completion and leaves preparation roots intact.
LOCAL is a trusted owning-process lifecycle attestation; QUIESCED requires permanent
reader evidence and absent native pins/mirrors. Confirmation preserves the first
committed evidence kind. No expiry inference, reader-wide fence, lease renewal or
public historical execution is added. The next root-release work must verify full
canonical coverage, all capture drains, terminal/qualified-abandonment binding and
late-capture closure before deleting any retained root.
