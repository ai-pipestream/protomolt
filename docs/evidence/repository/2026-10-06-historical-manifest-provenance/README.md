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
