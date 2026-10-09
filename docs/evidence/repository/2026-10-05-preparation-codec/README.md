# Private publication preparation codec

Validation:

```
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationPreparationCodecIT' --tests '*DocumentPublicationSeedsIT' --tests '*DocumentPublicationSessionIT' --console=plain
```

BUILD SUCCESSFUL in 19s. All 19 cases passed (7 codec, 4 seeds, 8 session), with
zero failures/errors/skips. SQL fixtures use real PostgreSQL and synthetic provider
observations. They do not qualify a provider or process-crash recovery.

The round trip retains the full normalized intent, exact operation/owner/upload
identities, seconds/nanos lease, predecessor and nullable/rich drive/backend
snapshots. Reconstructed admission reaches actual SQL attempt rows with the saved
UUIDs and lease tokens; decoding alone creates no operation. Tests reject wrong
journal key/digest, truncation, trailing bytes, unsupported version, malformed
UTF-8, duplicate identities, oversized declared lengths/counts, missing placement,
invalid lease/predecessor, noncanonical equivalent protobuf member order, and
aggregate encoding overflow from individually legal snapshots.

Sol reviewed the record, codec and tests without a blocker. The new record name
is DocumentPublicationPreparationRecord; the existing live coordinator named
DocumentPublicationPreparation remains unchanged. An initial name collision was
caught by compilation and corrected before the passing test run.

The codec verifies structural validity and operation binding, not authenticity of
all private fields. An immutable journal must store and verify a digest of the
entire blob before recovery; the command digest does not cover seeds or placement.
No journal, restored session authority, automatic failover or public API is enabled
by this checkpoint. Encoded-size bounds do not establish a total JVM heap bound;
the host must bound concurrent record loading separately. No performance test ran.
