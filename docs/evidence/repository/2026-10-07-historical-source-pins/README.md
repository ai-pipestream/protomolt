# Historical source pin projection

Validated the working-tree changes based on
`1b9cdd8a87a4d87ac03525c6019a45122e29dead`. Production changes and these receipts
are committed together. Sol reviewed the changes without a blocker.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalSourcePinsIT' --tests '*DocumentHistoricalMultiRevisionPublicationIT' --tests '*DocumentHistoricalReadCaptureIT' --max-workers=2 --console=plain
```

Exit 0; Gradle completed in 21 seconds. Eight tests passed, with no failures,
errors or skips: source pins (1), multi-revision publication (2), historical
capture (5). XML trailing whitespace was removed for repository hygiene.

These tests run actual PostgreSQL migrations and pin/registration SQL. Provider
observations used to construct native revisions are synthetic fixture data, not
proof of object-store I/O or durability.

The new test verifies the exact reader/pin/object/node/revision/publication tuple
against SQL; two captures of the same object have different pins. Selecting one
entry twice produces one pin and excludes the unselected object. Wrong capture,
forged entry, empty selection, cancellation and ended Use are refused. A closed
capture with a still-active Use remains protected. Projection is immutable and
normal release removes all pins after Uses drain. Existing multi-revision cases
also verify selected identities across revisions and the ended Prepared getter.

The initial run failed test compilation because the cancellation fixture treated
RepositoryReadControl as a functional interface. The fixture now implements its
two required methods. This was a test authoring error, not a production red/green
correctness demonstration; its log is retained separately.

This is a private projection prerequisite. Durable association, capture-batch
recovery, terminal/drain checks and retention release are not implemented by this
change. No public protobuf or transport contract changed. Whole-stack and
performance qualification were not rerun for this bounded change.
