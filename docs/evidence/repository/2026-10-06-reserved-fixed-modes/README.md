# Fixed modes restored with reserved preparation

Base: `95c5c7b0b6dc17630896a99b3b9a5fd2390028f1`.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryReservedPreparationIT' --tests '*RepositoryCoordinatorSupersessionIT' --tests '*DocumentPublicationProcessRecoveryIT' --console=plain
```

All 43 cases passed without skips: 13 reserved-load, 18 supersession and 12 process
recovery cases. `affected-green.tar.gz` contains their XML and binary results.

The private reserved loader now captures the immutable fixed-mode row alongside
the preparation under claim/owner/reservation checks. It reserves preparation bytes
plus the existing 1 MiB mode limit before capture; SQL limits mode text delivered to
Java. Both records decode outside the transaction using the existing integrity
checks. Delivery rechecks reservation, owner, preparation hash, exact mode row and
current caller authorization. One closeable result owns the preparation, immutable
mode map and byte reservation. This accounting does not claim to bound all parsed
Java heap overhead.

The fresh-process driver builds the successor from the retained modes instead of
its test scenario flag. Typed/opaque publication and revocation cases continue to
pass. Five corruption cases cover missing modes, unknown enum, wrong member set,
wrong owner and oversized text. They explicitly disable mutation guards in their
isolated test schemas; the oversized fixture also removes the database size check
to reach the Java-side bound. Production schema guards remain unchanged. Existing
installation, cancellation and access-revocation races exercise the combined read.

Sol reviewed the combined loader and found no blocker. This is a recovery input
primitive, not an automatic scheduler. Host retention of proposal/plan identities
before uncertain reservation and installation replies is still required. The
managed integration prerequisites are recorded in the recovery design.

The production-JAR regression also passed:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

`packaged-green.tar.gz` contains its one passing test with no skips. This checks
the existing packaged storage/admission harness; it is not hosted CI or deployment.
