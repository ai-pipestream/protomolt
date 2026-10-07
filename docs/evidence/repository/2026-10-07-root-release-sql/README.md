# Preparation root-release SQL foundation

V111 adds immutable release receipts, checks each retained capture's ownership
and drain evidence, and permits root deletion only with the exact receipt in
the same transaction. A deferred check rejects a receipt committing with any
target roots left. Headers and capture evidence remain permanent. Capture
admission explicitly rejects released operations.

This is an internal SQL foundation, not a callable release API. The future
private Java handler must run canonical terminal and selector validation in
the same transaction before inserting a receipt. SQL verifies retained identities,
hashes, terminal alternatives and all capture rows; it does not decode protobufs.
Existing live-history coverage is not yet release-aware, so the release handler
must remain unavailable until released coverage and exact retry are implemented.

## Executed qualification

Tested changes on base `fd34079daf20754e0f721999a074d8fef6771d7c` with the exact
SQL and test hashes in `source-sha256.txt`. Command exited 0 in 42 seconds:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationRootReleaseIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --max-workers=2 --console=plain
```

`results.tar.gz` contains JUnit XML: eight new root-release cases, 26 capture-drain
cases and 12 capture-admission cases, all with zero failures/errors/skips.
`gradle.log` retains build output. The fixture uses real PostgreSQL 18 and real
repository admission, cancellation, capture and drain logic. Its source-publication
provider observations are fixture-supplied; this is not a storage performance or
provider durability qualification.

The new cases demonstrate:

- Pending and undrained preparations cannot create a release receipt.
- Deletion without a receipt fails; receipt-only commit fails its deferred check.
- Throwing after receipt insertion and root deletion rolls back both.
- Exact abandonment releases the target roots and retains header/capture evidence.
- Receipts cannot be updated or deleted after commit.
- Changed preparation, root digest, capture digest or abandonment token is rejected.
- V110 databases with existing retained rows migrate without synthesizing drainage.
- A canonical cancellation from the real handler releases after drainage, while
  an incorrect terminal receipt hash is rejected. The successful test runs both
  canonical Java inspectors in the release transaction.

An initial test compile failed because the deliberately throwing transaction
lambda matched both Tx overloads; an explicit Consumer type fixed the test.
The six-case initial SQL run then passed before adding cancellation/migration
and running the final regression command above. Only final XML is archived.

Sol reviewed the SQL implementation and found no blocking atomicity bypass.
It checked the complete-batch loop independently of the fingerprint's inner
joins, claim/preparation/header/batch lock order, exact current-XID delete guard,
exclusive terminal alternatives and deferred all-root deletion.

## Remaining qualification and implementation

Not yet proved by this suite: V52 success release, reason-4 release, all other
rejection alternatives, multi-capture release corruption/races, two concurrent
releases, cancellation/lost acknowledgement around the release commit, and retry
after source pruning. The existing terminal-inspector tests do not substitute
for these release tests. Multi-root partial deletion also needs a dedicated case.
Legacy missing coverage remains unknown; this migration makes no backfill claim.

Next implement released-coverage classification and the bounded private handler,
then qualify those paths before exposing or advertising release. This does not
complete the full repository goal, prove hosted CI, merge or deployment, or
enable claimed historical publication.
