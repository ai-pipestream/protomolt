# Initial historical SQL upload admission

Base: `b3390ba66850d350d7528ec98f70e1d59ed54d8c`, branch
`refactor/repository-composition`. Sol reviewed the shared SQL extraction,
authorization/lifetime fence, lock dependencies and encoding reservation.

The private historical execution handle can now admit its fixed initial upload
plan through the same SQL tail as ordinary admission. The ordinary API still
refuses claimed historical commands. Every handle mutation checks current
claim/owner, preparation/modes, credentials and document policy, selected backend,
historical physical witnesses and live captured pins before admitting selections.
No caller-supplied replacement plan, attempt ID or token enters this method.

The method reserves bounded JSON/key scratch before encoding, outside SQL. Closing
the handle serializes with its synchronous mutations. This is SQL admission only;
returned attempt coordinates are not a provider-lifetime or publication capability.

New real PostgreSQL tests cover:

- Reuse-only selection admission and exact repeat with zero attempts.
- Mixed historical/fresh admission with the registered attempt identity.
- Actual JDBC rollback versus commit with lost acknowledgement.
- STAGING retry refusal, preserving seeded attempt ID, token, lease and state.
- Changed retained physical witness and missing native read pin: no attempts or
  selection rows are written. Corruption fixtures explicitly bypass guards.
- Closed-handle refusal and budget exhaustion before admission.
- Current READ/key revocation also refuses upload admission through an open handle.

The retained source fixture has synthetic provider observations. No fresh provider
upload is performed or claimed here, and no fake VERIFIED transition is used.
The extracted ordinary admission suite and existing unclaimed historical publication
suites also pass. A claimed mixed upload reaching VERIFIED through the actual
provider coordinator, its exact VERIFIED retry, held provider work across close,
assessment CREATE/reconciliation and claimed publication remain subsequent work.

Initial run: 116 tests, zero failures/errors/skips, 1m07s.
Final run: **124 tests, zero failures/errors/skips**, 1m09s:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalUploadAdmissionIT' \
  --tests '*DocumentHistoricalRegistrationAuthorizationIT' \
  --tests '*DocumentHistoricalExecutionIT' \
  --tests '*DocumentOperationUploadAdmissionIT' \
  --tests '*DocumentHistoricalMixedMemberPublicationIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --max-workers=2 --console=plain
```

The compressed logs/XML are preserved per run. `source-sha256.txt` identifies the
final tested Java sources. These are local results, not hosted CI or deployment.
No public restore endpoint, schema contract or migration changed.
