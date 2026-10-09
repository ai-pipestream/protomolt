# Atomic owner and selected-attempt renewal

The coordinator's initial, heartbeat and post-preparation renewal now share one
transaction for the execution claim, operation owner and selected attempts.
The separate renewal that returns VERIFIED attempt states remains separate.
No protobuf or public Java contract changes are included.

The baseline tests failed as intended: a paired renewal committed twice, and a
rejected selected-attempt renewal left the owner lease extended. The combined
helper commits once and rolls back all three leases when the selection is refused.

## Verification

- 24 selected-attempt PostgreSQL cases pass, including exact retry, owner-only
  empty selections, incorrect selection identities, displaced selections, a
  transferred execution claim, and a 64-member batch in reversed input order.
- Real JDBC faults before commit leave all leases unchanged. A fault after the
  actual commit leaves all leases extended; exact retry succeeds in both cases.
- 39 coordinator PostgreSQL/LocalStack cases pass, including slow preparation,
  provider work, cancellation and lifecycle checks.
- 14 local-drain cases pass. The added assertion refuses combined renewal after
  the drain marker and verifies unchanged claim, owner and attempt leases.
- The production-JAR storage-runtime check passes (217.610 seconds). It exercises
  managed publication, authenticated transport/client behavior and shutdown.

The root owns the implementation. Sol reviewed transaction lock order, selection
checks, cancellation checks and coordinator integration. The owner renewal SQL
trigger stamps the transaction write fence before selected attempts are updated.
No provider I/O or arbitrary control callback executes under those SQL locks.

## Performance qualification

The helper's actual JDBC commit counter proves one commit per paired renewal.
The RustFS trace passed twelve windows across one, two and four worker JVMs
in 1 minute 57 seconds. Across 144 foreground publications it records 2,304
commits: 16 per publication versus 18 in the baseline. The two combined renewals
account for 288 commits; the separate selected-state renewal accounts for 144.
There are no failures in the traced publication commit calls. The command and
small-fixture configuration match the preceding JDBC diagnostic.

Raw results are retained in rustfs-raw.tar.gz and aggregated in totals.csv.
This unisolated, traced workload does not establish latency improvement,
saturation capacity or horizontal scalability.
