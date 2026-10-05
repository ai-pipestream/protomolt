# Bounded schema artifact byte cache

Local verification on 2026-10-05:

```sh
./gradlew :protomolt-repo-admission:test --tests '*DocumentSchemaArtifactCacheTest' --console=plain
./gradlew :protomolt-repo-admission:check --console=plain
```

Both passed; the final full check after the cancellation regression passed in 7s.
The second command includes the production
runtime dependency boundary check and the complete admission unit suite. The
attached XML records seven focused tests using actual encoded descriptor sets.

The tests cover leased bytes across shutdown, idempotent lease closure, pinned
capacity refusal, idle eviction, temporary-copy byte limits, oversized input,
digest corruption, every insertion cancellation checkpoint, shutdown during an
in-flight copy, cancellation after lookup pinning and concurrent equal-digest insertion. Cancellation and corruption
release pending reservations; concurrent identical bytes converge to one retained
entry without dropping either pin.

This primitive does not fetch schemas, cache authorization or validation results,
coalesce registry calls, or retain archival assets. Hosts still supply authenticated
selection and separate cache instances for distinct security contexts. Parsed
descriptor heap and caller-owned inputs are outside its serialized-byte accounting.
No hosted CI, provider performance or repository adapter qualification is claimed.
