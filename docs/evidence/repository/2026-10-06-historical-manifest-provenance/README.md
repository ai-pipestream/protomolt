# Historical manifest provenance preparation

The internal writer now has an explicit historical preparation path. It copies
the complete retained manifest entry, preserving its original timestamp and
producer metadata, including an absent producer. The ordinary writer rejects
historical parts instead of treating them as uploads.

Entries are selected by the complete historical selector, including revision and
ordinal. Preparation checks the retained manifest; selection compares the full
declared physical identity with the transaction binding. Source Uses and
cancellation are checked before and after selection. This helper grants no
authorization or transaction lock proof and performs no SQL or provider I/O.

Real PostgreSQL tests cover known and absent producers, a new document version
with original part provenance, object-ID and provider-version substitution,
wrong revision, and a Use ending during selection. Physical provider observations
in this fixture are synthetic; this is not provider publication evidence.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalManifestEntriesIT' \
  --tests '*DocumentCommitWriterTest' \
  --tests '*DocumentSchemaRetentionIT' --console=plain
```

Nine tests passed with no failures, errors or skips in 14 seconds. Log:
`/tmp/protomolt-historical-provenance-final.log`. Sol reviewed the primitive with
no blocker; its suggested final lifetime check and non-ID substitution test were
included before the final run.

Atomic historical publication is not wired yet. Integration must still prove
multiple revisions of the same node and slot, current-policy authorization,
schema retention, rollback and uncertain commit recovery. Public restoration and
ordinary historical execution remain disabled. This checkpoint establishes
neither hosted CI nor deployment.

## Historical schema batch preparation

An explicit historical schema-batch entry now requires the complete live source
preparations before and after strict proof verification. Both ordinary overloads
retain their execution guards. Historical fragment size and hash feed retention;
the retention manifest also checks the historical physical object ID. Full
provider identity and transaction authority remain the commit binders' obligation.

The focused tests additionally exercise real descriptor/proof verification,
ordinary-path refusal, absent or closed source preparations, exact manifest
size/hash/object ID, and substituted object refusal. Descriptor definitions here
come from the fixture, not a registry or historical schema replay. Actual
historical retention writes and sealing still need atomic integration coverage.

The historical manifest, schema batch, retention and admission-binding suites
passed 35 tests with zero failures, errors or skips in 23 seconds. Log:
`/tmp/protomolt-historical-schema-batch.log`. Sol found no blocker and reiterated
that this detached prepared batch cannot substitute for live source Uses and full
physical authorization during commit.
