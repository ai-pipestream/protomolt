# Archive metadata snapshot checkpoint

Working changes based on `6dfe451703cbb06d6df4133d3aa95300236b8ff9`.
Local verification only; not a main merge, release, or deployment.

Commands completed with exit 0:

```sh
./gradlew :protomolt-repo-container:test --tests '*ArchiveMetadataMigrationIT' --max-workers=2 --console=plain
./gradlew :protomolt-repo-service:test --tests '*ArchiveServiceIT' --tests '*ArchiveManagedUploadIT' --max-workers=2 --console=plain
```

The migration test applies V102 over a populated V101 PostgreSQL database. It
checks legacy snapshot absence, refused backfill/removal/title mutation, refused
version/address mutation, mismatched insert version, allowed rendition updates,
and deletion. Its synthetic ledger rows test persistence constraints, not a
successful repository publication.

The service tests use real PostgreSQL and LocalStack. All 57 tests passed with
zero skips. Added assertions cover frozen unary v1/v2 metadata, current label
edits without new content versions, shared unchanged bytes, and streaming snapshot
identity and retention. Existing managed upload, bridge and lifecycle cases also
passed with the migration installed. The earlier service log predates V102 and is
retained separately; it is not migration evidence.

Subsequent review reproduced and corrected two guard defects: the first draft
prevented legacy owner moves, and accepted matching JSON-null addresses. The
red and green logs preserve that evidence. The guard now validates nonempty
addresses against the parent on insert and freezes identity only for rows with
snapshots. Existing legacy move and rollback tests pass. Added direct SQL cases
also reject a snapshot-bearing owner move, malformed addresses and wrong UUIDs.

Bridge and classification snapshot assertions passed in the later service run.
The response-limit fixture initially chose a label that exceeded the cap even
without a snapshot (1328 bytes, then 1028 bytes versus 1024). Reducing the fixture
label to 200 bytes isolates the intended boundary: removing the snapshot would
fit, but the actual complete response is refused. The same case verifies retained
version listing refuses an oversized snapshot-bearing response. No production
bound was relaxed. That focused case passed.

Final combined command (exit 0, 54 seconds):

```sh
./gradlew :protomolt-repo-proto:test --tests '*ArchiveVersionMetadataContractTest' :protomolt-repo-container:test --tests '*ArchiveMetadataMigrationIT' --tests '*ArchiveRetainedRevisionIT' :protomolt-repo-service:test --tests '*ArchiveServiceIT' --tests '*ArchiveManagedUploadIT' --tests '*ArchiveClassificationIT' --tests '*BoundedArchiveTransportIT' --max-workers=2 --console=plain
```

The attached XML records 89 passing cases, zero skipped: 2 contract, 5 container,
and 82 service tests. The final invocation reran service tests; unchanged proto
and container tasks reused their preceding passing results. Complete proto imports
were compiled during contract generation. `buf lint` on both changed proto paths
and `scripts/check-proto-compatibility.sh 6dfe451703cbb06d6df4133d3aa95300236b8ff9`
also exited 0. Sol's final review found no blocker after the guard corrections.

This checkpoint does not complete archive schema/ownership/admission provenance,
restore or the full repository composition goal.
