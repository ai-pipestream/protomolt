# Claimed mixed historical publication qualification

Base: `b1de2b0b9287cae887dff37a814e4993f047aa2c`.

The initial-generation fixture combines retained historical parts and a fresh
PARSED upload in one member. It executes the same handle publication with process
authority and with a registered scoped credential. The new probe checks exact
receipt replay and current revision, opens a historical capture of the committed
revision, and reads every exact provider version. Historical objects retain their
object ID, provider version, backend generation, storage realm, namespace and key.
The fresh object's SQL origin must equal the selected upload attempt. Both kinds
must be present, and a repeated publication attempt must require reconciliation.
Outer fixture checks include release of the payload budget.

Sol reviewed the fixture without finding a blocker. Its intended qualification
scope is initial mixed publication only, excluding successor mixed execution, grant-revocation
races, a public recovery endpoint, or performance. No production code or wire
contracts change in this increment.

Resource compilation passed before the final physical-origin assertions. Packaged
run 11366 compiled the final probe sources and passed:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

The SQL/provider host completed with `OBSERVED_SQL_HOST_OK` and both required
mixed-publication markers. Its completed log is archived and source hashes were
reverified. The full driver completed in 8m 23s; archived XML reports one test,
zero failures/errors/skips, including restart and cleanup checks.

For successor qualification, Sol reviewed an explicit resubmission boundary:
historical bytes must be reread through the new capture, while new-upload payloads
must be supplied again by the caller and checked against the declared size/hash.
The successor must use `plan.next()` seeds for new attempt IDs and upload tokens,
perform real provider uploads and verify them under its current owner before
CREATE/publication. This must not be described as automatic payload recovery.
