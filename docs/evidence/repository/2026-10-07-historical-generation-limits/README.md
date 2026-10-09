# Historical generation capacity and replacement

Base: `6dcc7147c4f6f14f18a45cb050ebb096d22fad84`. Test changes reviewed by Sol.

```
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalGenerationLimitsIT' \
  --tests '*RepositoryHistoricalGenerationsIT' \
  --tests '*RepositoryHistoricalPreparationIT' \
  --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 1m44s. 26 cases, zero failures, errors or skips.
PostgreSQL 18 is real; provider observations in source fixtures are synthetic.

Added coverage:

- Byte exhaustion rejects takeover before V97/V93 writes, preserves reservations
  and restores the original retry route. Returning capacity permits recovery.
- V98 replaces an expired reserved or installed successor while an older worker
  remains active. The entry ID persists. Claim epoch/token and owner generation/token
  match the replacement plan, with one additional installation and no new activation.
- Retirement waits for worker completion, preserves the successor route and
  ultimately returns the full byte budget.

Source capture follows installation. These SQL tests do not establish provider I/O,
V52/V97 races, managed public routing, pruning, performance or broader goal completion.
