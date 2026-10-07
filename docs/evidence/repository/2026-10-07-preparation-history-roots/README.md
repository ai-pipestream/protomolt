# Preparation history projection

Base: `93a46ce3e6d99d7dbb16f96507ec63446e446607`, with the V103 projection
changes in this commit. Local PostgreSQL 18 container evidence, not hosted CI or
deployment. Sol reviewed the implementation and tests with no checkpoint blocker.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --max-workers=2 --console=plain
```

Exit 0, 32 seconds. 26 tests, no failures, errors or skips. XML trailing whitespace
was removed for repository hygiene; test content is otherwise unchanged.

The three projection tests cover new explicit empty sets and exact retry,
immutable sealed headers, late child refusal, migration from V102 with unknown
coverage, unsealed commit refusal, wrong count/digest rollback, forged internally
consistent SQL projection rejected by canonical Java reconciliation, and a real
SQL insert fault rolling back claim, preparation and index together. The existing
23 journal tests cover surrounding registration behavior.

No provider is used in this focused SQL suite. Nonempty SQL mechanics are exercised
by a deliberately forged projection, not successful historical registration.
Real prepared historical sources, selector deduplication, permission/pin lifetime,
release, pruning and historical execution activation remain to be qualified.
The SQL seal is not proof that roots match the canonical protobuf selectors.
No protobuf contract or runtime deployment is changed by this checkpoint.
