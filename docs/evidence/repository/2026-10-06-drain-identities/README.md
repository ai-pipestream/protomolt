# Retained coordinator drain identities

Focused command:
`./gradlew :protomolt-repo-container:test --tests '*DocumentJournaledSessionsIT' --tests '*RepositoryCoordinatorDrainIT' --console=plain`.
Result: 39 cases, no failures or skips, build successful in 1 minute 1 second.

Two reproduced failures and their fixes:

- A real owner-creation transaction rolls back after registration commits. An
  accepted abandonment waits while the manager captures its drain identities,
  then commits and evicts the session. Before the fix, the next drain forgot the
  identity (confirmed count changed from one to zero). The retained snapshot now
  requires its exact confirmation on every retry.
- A real claim remains live while its owner expires. Owner takeover and verified
  supersession retire the original session. Same-incarnation restoration fails
  because this fixture has no assessment executor. Previously the manager dropped
  the nonterminal owner; it now retains and retries it, and records its V90 marker
  on drain. No coordinator binding is forged.

`red.xml.gz` and `restoration-red.xml.gz` preserve the failing assertions.
`sessions-green.xml.gz` and `drain-green.xml.gz` preserve the passing suites.
The first abandoned-operation fixture initially exercised the existing prohibition
on abandoning an admitted owner; it was corrected to roll back owner creation.

Sol reviewed the production and test changes. Whole-host V91 activation remains
unfinished: qualify held restoration reservation, loaded journaled restoration
cleanup, and service-owned schema-worker drain before recording local attestation.

The first production-JAR regression hit the harness's 90-second child-host timeout
(`runtime-timeout.xml.gz`). Its temporary log was deleted by the original harness,
so that run establishes no cause or runtime qualification. The harness now retains
temporary artifacts on failure and reports the host-log path; its timeout is unchanged.

A second run at the unchanged cap also timed out. Its retained host log shows
`NATIVE_ASSESSMENT_RUNTIME_OK`, then continued progress through later journaled
recovery probes until the harness killed the aggregate child. Both the JUnit result
and host log are preserved. After Sol's review, the aggregate child cap was raised
to 180 seconds; individual operation deadlines and success assertions are unchanged.
The child then reached `OBSERVED_SQL_HOST_OK`; `runtime-completed-host.log.gz`
preserves that result. This is correctness qualification, not a latency benchmark.

Final command: `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
Result: successful in 3 minutes 10 seconds, including the subsequent restart
processes and every required host marker. `runtime-green.xml.gz` preserves the final
JUnit result. This broader regression does not substitute for the new journaled
restoration lifecycle cases still listed above.
