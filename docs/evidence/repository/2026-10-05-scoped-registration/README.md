# Scoped journal registration

Source base: `850507463678e0e796d1f36377ae726a257f5467`. Run on October 5, 2026,
America/New_York (October 6 UTC).

The private journaled session previously required process authority even for a
caller authorized to update an existing document. The new scoped fixture fails at
that old entry guard (`red.xml`, `red.log.gz`). It uses real PostgreSQL; its retained
object declarations are explicitly synthetic SQL fixtures, not provider evidence.

The session now owns a private nonserializable journal capability, separate from
the actual caller. It binds the initial key, command digest, claim token/epoch and
saved owner nonce. V82/V83 access checks loaded preparation and live owner against
that binding. Legacy direct private mode/load/start and bootstrap access remain
process-only. No caller is copied with elevated authority.

Focused command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentScopedRegistrationIT' \
  --tests '*DocumentPublicationSessionIT' \
  --tests '*DocumentPublicationModesJournalIT' \
  --tests '*DocumentAssessmentStartJournalIT' --console=plain
```

Result: 43 tests passed, none skipped, including seven scoped cases. The fixture
proves authorized registration/staging, direct private-access refusal, source READ
and destination WRITE denial, wrong account/principal refusal and no implicit
creation grant. Those five denied registrations leave no claim/preparation/mode/
owner rows. Source revocation before staging refuses the marker without setting
the local sticky flag; actual upload admission and another registration attempt
also refuse, preserving the already registered owner and preparation.

The production-JAR gate also passes:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
Its PostgreSQL/LocalStack publication, rejection and journaled recovery variants
pass with the additional preflight transactions. This preserves the existing
real-provider integration qualification; those journaled provider variants use
process authority, so scoped end-to-end provider qualification remains outstanding.
`provider-runtime.xml`, `provider.log.gz` and `source.sha256` retain the result
and exact changed Java sources. No performance claim is made for this gate.

Preflight runs against the frozen plan before registration and pre-sticky staging.
It is a separate transaction from the journal write: concurrent ACL changes after
preflight are still possible. Upload, CREATE/decision and publication must retain
their authoritative checks. This does not establish an atomic no-marker guarantee
across concurrent revocation, enable ordinary runtime registration, or qualify
fresh-process interrupted registration and cleanup. No public RPC was mounted.
