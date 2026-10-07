# Redis orphan cleanup

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 7m44s, including the real five-minute lease and one-minute recovery scan.
Sol reviewed the fix and test with no blocker.

A fresh JVM waits for host cleanup of the exact lost-ACK object, checks durable
cleanup state and provider absence, then verifies both committed histories.
The long wait now follows the existing restart probes to preserve their leases.

The test exposed repeated cleanup selection of native committed attempts. SQL
correctly blocked deletion of referenced objects. Scanning and the locked claim
check excluded only assessment references; both now exclude all references.
Regression checks require expired native retained attempts to be skipped by scan
and claim, with retention flags unchanged. Cleanup warnings fail the test.

Late writes, cleanup-provider failures and active-work shutdown remain open.
This does not resume publication under the failed operation ID.
