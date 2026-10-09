# Historical public replay refusals

Parent: `79bfcc3ed23c5814e77ef5f774fd83c4d623a8e4`.
Sol reviewed the public exception boundary, caller checks, pending mode semantics
and preservation of the retained attempt. The production change is a narrow catch
in `DocumentPublicationFacade`: a private `CommandConflictException` becomes public
`RepositoryException(CONFLICT)`, preserving the cause. Private ledger exceptions
are unchanged. The gRPC adapter maps this to ABORTED, and the client restores
CONFLICT.

The new real PostgreSQL/LocalStack test initially failed because a changed command
leaked the private ledger exception through the facade. The red Gradle log and XML
preserve that failure. The final runtime test passed after the mapping fix and
after correcting two test expectations to the existing contracts:

- Invalid credential generation returns UNAUTHENTICATED; missing account bindings
  return PERMISSION_DENIED.
- Changed modes return CONFLICT for a pending retained attempt, and
  FAILED_PRECONDITION for a completed operation's fixed mode journal.

Five refusal cases run through both library and authenticated in-process gRPC,
first against a committed operation and then against the original retained attempt
after a committed START loses its acknowledgement: 20 asserted refusals total.
The cases change destination revision condition, historical object identity, mode,
account binding or credential generation. The envelopes remain structurally valid.

No refusal adds placement selection, schema resolution or provider PUT. The
subsequent exact request still succeeds and exact-replays, so negative retries do
not discard the retained START identity. The transport fixture binds test tokens
to each deliberately supplied identity; credential-generation fencing is enforced
by the repository, not claimed for an external token issuer.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Final focused run: passed in 1m9s, one aggregate JUnit case, zero failures, errors
or skips. `green-gradle.log.gz` and `green-test.xml.gz` describe the final runtime
qualification. The aggregate requires separate terminal and pending refusal
markers along with the existing historical public cases.

Private-ledger and runtime regressions passed in 35s: 48 tests, zero failures,
errors or skips (33 operation-admission, 12 execution-claim, two scope and one
runtime-configuration cases). Their four XML reports and regression log are
archived alongside the runtime evidence. Command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryOperationAdmissionIT' \
  --tests '*RepositoryExecutionClaimLedgerIT' \
  --tests '*DocumentPublicationScopeCallsTest' \
  --tests '*DocumentPublicationRuntimeConfigTest' \
  --max-workers=2 --console=plain
```

This does not qualify uncertain reservation/install proposals, corrupted persisted
journals, all policy races, cleanup failures or network performance. The historical
factory remains internal pending its remaining acceptance checks.
