# Initial registration process-death qualification

`DocumentInitialRegistrationCrashIT` runs separate writer and reader JVMs against
real PostgreSQL. The writer's JDBC hook verifies both target rows on the committing
connection, then halts immediately before COMMIT (exit 81) or immediately after the
actual COMMIT but before acknowledgment (exit 82). The writer is reaped and its
private request file deleted before the reader starts.

The fresh reader receives only a phase and operation ID. Before-commit death leaves
no claim or readable preparation; an explicit retry with the caller's original
input can register afterward. After-commit death leaves the paired registration.
Test-only SQL reads the original claim; production journal APIs load the command
and preparation, inspect PREPARATION_ONLY, and retry with identical authority.
The claim epoch/token/lease are unchanged. A re-encoded record hash must equal the
parent's original bytes, binding seeds, placements, command, owner nonce and lease
duration. The parent also checks the original claim token independently.

Neither reader creates modes, an owner, an admitted command, an assessment start,
upload/selection attempts or terminal results. All reader reservations drain.
This is explicitly test-only SQL capability inspection, not a production recovery
endpoint or proof of automatic claim transfer. Source fixture provider observations
are synthetic; no object-store crash behavior is claimed.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentInitialRegistrationCrashIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationRegistrationInspectionIT' --console=plain
```

Final regression run passed in 28 seconds. An initial worker assumed Timestamp
instead of Hibernate's actual Instant result; that test-only conversion was corrected.
Sol reviewed commit targeting and fresh-process isolation without a blocker. Mode
binding crash recovery, coordinator quiescence, ordinary session activation, hosted
CI and deployment remain separate gates.
