# Historical START acknowledgement reconciliation

The original execution handle retains a private assessment proposal and retention
duration across START rollback or an uncertain reply. It can reconcile that exact
proposal only after the complete current-authority transaction and delivery check
succeed. Other handles can observe coordinates but gain no CREATE permission.
Repeating START does not clear the sticky CREATE-attempt marker.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalExecutionIT' --tests '*DocumentHistoricalRegistrationAuthorizationIT' --tests '*DocumentAssessmentStartJournalIT' --max-workers=2 --console=plain
```

Passed: 33 tests, zero failures, errors or skips; 47-second build. Real PostgreSQL
tests cover committed START reply loss, rollback, concurrent handles, fixed
retention, expiry, and denied reconciliation after owner/claim expiry or credential
and ACL revocation. Denied retries leave acknowledgement absent and create no
schema claims, assessment owners or revision commits. Shared source, scope and
budget cleanup assertions run for the denial cases too. Provider observations in
these SQL fixtures are synthetic.

Sol reviewed the design and final implementation. Review corrections included
the exact fence exception assertions and preserving common cleanup assertions.
The production-JAR probe now requires CREATE success after original-handle
reconciliation and still refuses another handle before schema claims. Its full
storage qualification is pending at this checkpoint; these SQL results do not
prove that runtime probe, public historical publication or transport behavior.
No protobuf contract or migration changed.
