# Restoration reservation and loaded-resource drain

The focused command was
`./gradlew :protomolt-repo-container:test --tests '*DocumentJournaledSessionsIT.failedSameIncarnationRestorationRetainsItsIdentityForDrain' --console=plain`.
Both parameter cases passed in a 20-second build; `barrier.xml.gz` contains the
JUnit result.

One case retains the earlier failed-restoration retry test. The added case holds a
real JDBC commit after the restoration claim fence is stamped, before the manager
reserves its entry. Drain reports registration activity and writes no V90 marker.
After release, the failed restoration retains its identity and a subsequent drain
marks it. The gate checks the current transaction's claim-fence XID, rather than
pausing replay or relying on timing.

The production-JAR probe uses its authentic observed executor. It creates an initial
journaled owner, cancels before provider work, renews the claim while allowing the
owner lease to expire, then persists generation-2 preparation and modes before
takeover. It records an assessment start without committing an assessment stage.
Same-incarnation restoration loads and reserves the preparation, then the real
executor refuses the unresolved stage. Close releases those bytes, retains the
nonterminal identity and capacity, and allows its V90 marker. Fail-fast provider
ports prove this case does not accidentally upload or read content; they do not
pretend to perform a successful provider operation.

Fixture corrections: the commit gate initially paused replay; it now selects the
actual claim-fence transaction. The loaded probe initially followed a successful
fixture mutation and hit the revision guard; it now runs before that mutation.
Sol reviewed both cases. Neither test enables V91 host attestation or public restore.

Final production-JAR command:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
The complete regression, including restart processes, passed in 3 minutes 17 seconds.
`runtime.xml.gz` contains its JUnit result; `host.log.gz` records
`JOURNALED_RESTORATION_DRAIN_OK` and the complete host success marker.
