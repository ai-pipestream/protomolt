# Journal consumption guards

The targeted PostgreSQL run passed 35 cases across mode choices, preparation,
assessment starts and execution claims. See green.log and the four JUnit XML files.
Two admission assertions initially failed because missing modes and a wrong nonce
were accepted (admission-red.xml). Sol then identified an overbroad owner guard that
blocked expiry cleanup; cleanup-red.xml records the actual SQL failure. The final
guard preserves recovery-only authority and still refuses expired execution writes.

New scoped-caller checks permit internal pass/fail mode comparison while denying
private map retrieval. Missing journal state is injected in a real SQL transaction
to prove it cannot select the legacy assessment path.

The provider fixture uses real observed evidence with saved b=OPAQUE and observed
b=TYPED. It requires the explicit mismatch error and no assessment stage. This does
not measure high-level artifact writes, prove arbitrary direct commit mode binding,
or activate automatic recovery. SQL handles journal presence and identity; Java
compares observed mode semantics. Public protobuf contracts are unchanged.

The final production-JAR PostgreSQL/LocalStack admissionStorageTest passed in 2m16s,
including the new real observed-mode mismatch fixture and the existing journaled
commit recovery flow. Sol reviewed the final cleanup, map ownership and authorization
ordering with no blocker for these bounded paths. Revocation racing the new mode
comparison is not separately fault-injected in this checkpoint; code ordering is
not presented as general post-provider error-reauthorization proof.
