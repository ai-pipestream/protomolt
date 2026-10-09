# Successor coverage lineage checkpoint

Base: `d5ea8f2d5`.

V119 adds immutable successor lineage proofs, distinct from root-owner certificates.
The handler decodes the current preparation and locks claim, current preparation,
then previous preparation. It verifies the exact immutable install edge and requires
an already verified immediate predecessor. Anchor, command, roots and depth are
inherited exactly; depth advances one generation and is bounded to 64. SQL guards
bind the relational identities; canonical protobuf verification remains a trusted
Java handler obligation. Predecessor bytes need not be repeatedly decoded because
the immutable predecessor proof already attests them.

No new history header, capture or execution right is created. Proof insertion and
unresolved deletion are atomic. Missing prerequisite proof remains explicitly
pending only when an unresolved predecessor exists. Contradictory proof/unresolved
state or missing proof and marker produces DATA_LOSS. This is not pruning authority.

Sol reviewed the design and implementation. Two diagnostic gaps found during review
were fixed and regression-tested. No remaining blocker for this checkpoint.

```
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentPreparationCoverageLineageIT' \
 --tests '*DocumentPreparationCoverageReconciliationIT' \
 --tests '*DocumentPreparationCoverageCertificatesIT' \
 --tests '*RepositoryHistoricalSuccessorActivationIT' \
 --tests '*RepositorySuccessorInstallIT' \
 --max-workers=2 --console=plain
```

44 tests, zero failures/errors/skips; 1m17s. JUnit XML is archived here. Tests use real
PostgreSQL 18 containers; existing source-publication fixtures contain synthetic
provider observations and do not establish object-store performance.

Eight lineage cases cover ordinary empty roots, two unactivated historical
successors, V117 migration and pending predecessor, forged SQL proof fields,
cancellation after proof insertion, corrupt install links, absent predecessor proof
and marker, and contradictory verified/unresolved state. The forged-field case
proves refusal and unchanged unresolved state for each mutation; it does not isolate
which individual SQL guard rejects each mutation. Corruption injection is explicit
isolated catalog mutation outside the supported immutable writer.

Still required before pruning: released-anchor lineage qualification, maximum-depth
boundary, lost-ack/partial-batch and simultaneous reconciliation/release/prune races,
operational integration and atomic pruning. Physical cleanup, schema reclamation,
restore, RustFS performance and horizontal-scaling qualification remain part of the
full repository goal. No full-suite, hosted CI, main merge or deployment claim.
