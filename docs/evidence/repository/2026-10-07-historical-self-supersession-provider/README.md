# Historical self-supersession through real providers

Base: `fcb1a9b5653c32ae3d62424d54134d1e9590126e`. The production implementation
is unchanged from that reviewed SQL checkpoint. This change adds a packaged-host
qualification and records the next concurrent-generation design. Sol reviewed the
fixture, scenario selection, inherited byte/receipt checks and lease boundaries.

```
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 11m13s. XML: one aggregate test, 670.386 seconds,
zero failures/errors/skips, timestamp `2026-10-07T20:39:51.299Z`. The inventory uses
38 production artifacts (15,992,702 bytes); child hosts reject an ambient JUnit
classpath. Real PostgreSQL, LocalStack S3 correctness and Redis adapters run in
containers. This is correctness evidence, not a RustFS performance measurement.

The existing aggregate, replay, restart and reconciliation hosts remain mandatory.
A separate `historical_self_supersession` database/JVM adds the new scenario with
its own 90-second limit. Existing host deadlines are unchanged.

## What the new scenario proves

The same scoped-caller Entry reserves and installs a historical successor before
fresh capture. It waits for actual claim/owner lease expiry in PostgreSQL, then
reconciles its own installed claim through V98. A later request installs the
replacement. Assertions bind the advanced epoch and distinct claim token,
incarnation and owner nonce: one V98, two V93 installs and zero activations before
new capture. The Entry remains retained across those request boundaries.

The existing mixed-publication path then obtains a fresh historical capture, rereads
exact retained provider versions, uploads resubmitted fresh bytes, validates and
publishes, checks the current revision and exact receipt, and reads back both reused
and fresh parts. Normal terminal retirement returns the owner's byte budget and
keeps authorized receipt replay working.

Captured markers include:

- `SCOPED_HISTORICAL_SELF_SUPERSESSION_INSTALLED_OK`
- `SCOPED_HISTORICAL_SELF_SUPERSESSION_PUBLICATION_OK`
- `SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK`
- `SCOPED_INSTALLED_HISTORICAL_TERMINAL_RETIRED_OK`

The test driver also requires `HISTORICAL_SELF_SUPERSESSION_HOST_OK` and exit 0.
The full Gradle log and terminal XML are archived. Child logs are periodic snapshots:
JUnit deletes its temporary directory on success, and the final self-supersession
snapshot precedes the host's last output lines. Do not treat the child snapshots as
complete transcripts; the successful driver assertions establish terminal exit and
required markers. Source hashes identify the exact tested fixture and implementation.

## Limits

The replacement has a 30-second lease too; this does not prove recovery from slower
post-installation work. Lost V97/V93/V98 replies and other refusals are qualified by
the preceding real-SQL evidence, not newly injected here. Public historical routing
remains disabled. Concurrent retained generations, attached-generation expiry,
managed transport routing, pruning, JCR and scalability remain separate goal work.
