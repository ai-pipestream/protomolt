# Abandonment confirmation and retained session cleanup

Local PostgreSQL 18 Alpine verification on 2026-10-05:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationAbandonmentIT' --tests '*DocumentJournaledSessionsIT' \
  --tests '*DocumentPublicationRegistrationInspectionIT' \
  --tests '*DocumentPublicationPreparationJournalIT' --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentAssessmentStartJournalIT' --tests '*DocumentScopedRegistrationIT' \
  --tests '*RepositoryExecutionClaimLedgerIT' --tests '*RepositoryClaimMutationFenceIT' \
  --tests '*DocumentPublicationRecoveryIT' --console=plain

./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationReplayIT' --tests '*DocumentPublicationSessionIT' --console=plain

./gradlew :protomolt-repo-container:test --tests '*DocumentJournaledSessionsIT' --console=plain
```

The broader run passed 109 tests in 1m8s. Existing replay/session regressions passed
25 tests in 25s. After adding two final race cases, the journaled-session suite
passed all 16 tests in 27s. Every run had zero failures, errors or skips; the final
16 include cases already counted in the broader run. Sol reviewed the design and
implementation. Its two material findings were fixed and re-reviewed:

- Public execution now reports typed FAILED_PRECONDITION rather than leaking a
  private exception. Both execute and executeScoped are exercised with storage
  and schema callbacks that fail if opened.
- Marker replay authorizes the bounded, canonical **stored command**, before
  distinguishing a supplied command conflict. A two-document fixture revokes READ
  on the original document while leaving the alternate supplied document readable;
  neither original nor alternate requests can reveal the marker after revocation.

Private exact confirmation remains valid after claim expiry and transfer, checks
the retained preparation/nonce/command/original token, and does not renew the claim.
Absence is explicit and does not authorize deletion of an uncertain registration.
The manager reserves an idle entry during abandonment and evicts only after durable
confirmation. Real lost preparation/modes acknowledgments followed by a lost
abandonment acknowledgment retain capacity until confirmation after lease expiry.
Retries of an authorized abandoned operation do not recreate a session.

Negative cases keep capacity for active calls, admitted owners, unprivileged
cancellation and expired claims without markers. A real JDBC post-commit callback
also writes abandonment after preparation commits, then loses the registration
acknowledgment. Fresh authorized replay evicts; cancellation of confirmation keeps
the entry and preserves the original SQL failure with the confirmation error
suppressed. Explicit later confirmation releases the retained entry.

Initial corrections included Java overload inference and a test command that
incorrectly changed a destination revision without changing its same-address source
condition. The existing command validator correctly refused it. The final ACL test
uses a valid alternate command. No validation or database guard was weakened.

This qualification covers SQL state, real transaction acknowledgments, authorization
and runtime replay. It does not establish provider cleanup, coordinator takeover,
default journaled activation, a public cancellation RPC, throughput, deployment or
hosted CI. Owner-admitted operations still require a separate quiescence and delayed
provider-write protocol. Existing ABI, protobuf names/tags and provider capabilities
are unchanged.
