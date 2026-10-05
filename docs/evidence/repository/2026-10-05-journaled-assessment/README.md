# Journaled assessment with actual COMMIT acknowledgment loss

The production-JAR PostgreSQL/LocalStack `admissionStorageTest` passed in 2m14s.
Its separately compiled probe now includes a fifth operation-replay scenario that
uses V81 preparation, V82 fixed modes, a claimed owner and V83 assessment start.

The scenario uploads real versioned provider content using saved attempt seeds.
It validates the deliberately invalid candidate through the real runtime, retains
observed evidence, and performs actual assessment CREATE. A delegating JDBC wrapper
throws SQL state 08006 only after the underlying COMMIT succeeds. A fresh helper
loads the sticky coordinates and discovers the original sealed stage; no CREATE
retry is used. Exact selected attempts and a single sealed assessment are checked.
The existing retained replay and terminal decision path then proves a bound durable
rejection, post-COMMIT cancellation, exact receipt replay and retained terminal reads.
The harness requires both journaled recovery and journaled decision output markers.

Sol reviewed the fixture with no blocker for this scope. The test stays in one JVM
under the original live claim/owner and uses trusted process authority. It does not
prove process-crash failover, claim transfer, scoped-client ACL behavior, full private
preparation restoration, zero extra provider GETs or automatic session activation.
V82-to-observed-mode equality remains a required activation check; this scenario
supplies matching modes and does not establish mismatch refusal.

The same production-JAR correctness gate passed against PostgreSQL/RustFS; complete
output and XML are retained in rustfs.log/xml. This is provider parity, not a
performance measurement.
