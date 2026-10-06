# Historical and fresh fragments in one member: provider qualification

The dedicated production-JAR harness passed in 2m38s with no skipped cases. Its
Gradle log, JUnit result and changed-source hashes are retained alongside this note.

The production-JAR storage harness now exercises a typed destination member with
historical CORE content and a fresh PARSED fragment using PostgreSQL and versioned
LocalStack storage. This is correctness qualification; RustFS remains the local
performance backend. No production contract or implementation changed in this slice.

The probe uploads the new fragment through `DocumentPartTransfer`, retaining actual
provider observations. Without selected-attempt verification the entire destination
stays absent, no schema admission is published and replay stays pending. With
verification, publication commits one member, replay returns its exact result, and
the source mutation revision stays unchanged.

Historical parts retain physical UUID, provider version, size, content type and the
complete original manifest entry selected by source revision ordinal. Fresh content
is bound to its selected attempt and actual provider version with new producer
provenance. The normal definition resolver is invoked only for the fresh PARSED
occurrence. After publication, actual versioned provider reads recover every part;
the exact fragments and both decoded shapes are checked through retained schemas
without consulting a current registry. Source pins and byte reservations drain.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The ordinary `:test --tests '*DocumentAssessmentStorageRuntimeTest'` task explicitly
excludes this harness and executes no tests. Use the dedicated task above, which
builds the production artifact inventory and requires the same-member success and
unverified-refusal markers alongside the existing recovery matrix.

Sol reviewed the ordinal routing, actual-provider assertions and preservation of
historical metadata without finding a blocker. This complements the earlier SQL
fixture with synthetic provider observations. It does not activate public/claimed
restore, prove backup or pruning completeness, or represent hosted CI or deployment.
