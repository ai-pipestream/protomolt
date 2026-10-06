# Historical occurrences with equal type URLs

Base: `dd04a5136dec4f878bf8c58ec2eef68f6d7a2a5f`, plus the accompanying test changes.

`./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalSchemasIT' --tests '*DocumentSchemaRetentionIT' --tests '*DocumentHistoricalMaterializationIT' --console=plain`

Passed 21 cases in 19 seconds. The new case uses two dynamic protobuf descriptors
named `archive.Record`, with different field names and descriptor artifact hashes.
CORE and PARSED occurrences share `type.test/archive.Record` but retain their own
definitions. A fresh historical reader replays both from SQL after active policy
changes. Selected materialization verifies each artifact hash, field name and value;
byte reservations and historical read pins are released.

The fixture performs real runtime schema admission and PostgreSQL retention and
replay. Physical provider observations are explicitly synthetic. The historical
reader has no registry dependency; this establishes independence from live schema
discovery, not provider availability or a simulated registry-outage transport test.
No production behavior changed. The first fixture run was refused because its
ownership lacked the required security message; that fixture was corrected.

Sol reviewed the test and found no blocking issue. This result does not enable
public historical publication, safe pruning, or automatic recovery.
