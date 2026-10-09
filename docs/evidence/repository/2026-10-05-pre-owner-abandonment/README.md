# Pre-owner registration abandonment

Local PostgreSQL 18 Alpine qualification on 2026-10-05:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationAbandonmentIT' \
  --tests '*DocumentPublicationRegistrationInspectionIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentAssessmentStartJournalIT' \
  --tests '*DocumentJournaledSessionsIT' \
  --tests '*DocumentScopedRegistrationIT' --console=plain
```

65 tests passed, zero failures/errors/skips, Gradle 34 seconds. Nine new cases
exercise V85 with real SQL. Sol reviewed the design, SQL guard ordering and test
coverage; no material implementation blocker remained.

- Committed abandonment blocks preparation/mode retries and admission, including
  exact retries. Inspector reports ABANDONED under the original live claim.
- Eight admission/abandonment races each produce exactly one winner.
- Wrong seeds, wrong claim token, expired/transferred claim and absent private
  authority cannot mark the registration. Existing owners cannot be abandoned.
- Cancellation before commit rolls back; a completion failure after commit is
  reconciled by exact retry. Markers cannot be deleted.
- V84 registrations, both admitted and pre-owner, migrate to V85 without changing
  their canonical preparation identity.
- A transient command row without its owner prevents marker insertion. The
  transaction rolls back without command, owner or marker rows.

Initial fixture corrections: the repeated race setup duplicated a drive name;
it now reuses one drive with fresh commands/seeds. The first command-only fixture
attempted to commit an orphan, correctly refused by the existing deferred atomic
owner constraint. The final test exercises that transient state in one transaction.
No production guard was weakened. An ambiguous overloaded Tx lambda was corrected
before the final compile and run.

This is a private SQL primitive, not runtime activation, provider cleanup, process
takeover or session eviction. Exact confirmation through retry still needs a live
original claim. Read-only exact-marker confirmation after lease expiry and manager
capacity release remain to be implemented together. Owner-admitted work still
requires coordinator quiescence and delayed-provider qualification. No performance,
hosted CI, merge or deployment claim follows from this checkpoint.
