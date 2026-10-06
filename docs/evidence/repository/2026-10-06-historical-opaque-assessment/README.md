# Explicit opaque historical assessment

Historical command assessment can preserve a source admitted explicitly as OPAQUE
when the supplied current policy permits opaque mode. The classifier runs under
the exact captured Use and caller's current READ. It checks the sealed native
revision, commit/admission/operation-success identity, command digest, transaction
binding, body/metadata, exact retention manifest and absent typed container role.
It loads no descriptor assets and performs no Any payload decoding.

Every distinct historical source selected by an opaque member must pass this
check before fragment capture. A typed source cannot be downgraded, and a missing
admission is not evidence of opacity. Existing hash checks, source pins, current
READ at inspection/replay and target-policy mode checks remain in force. This
adds no public restore endpoint or publication authority.

The PostgreSQL fixture covers explicit opaque success without a registry resolver,
typed-source refusal, current typed-required policy, revoked READ, cancellation,
and a real unbound native revision published before a policy existed. The latter
has no schema admission and returns FAILED_PRECONDITION. Tests check exact refusal
statuses and zero retained byte reservations. The fixture obtains fragments from
its original content assembly, independent of typed proof availability.

The admission-less regression caught a SQL evaluation error: an AND expression
still invoked the retention manifest function with a null revision argument.
An explicit CASE now guards that call. The function's failure is not caught or
converted into opaque acceptance.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentHistoricalSchemasIT' --console=plain
```

The final run passed in 39 seconds. Log:
`/tmp/protomolt-historical-opaque-qualified.log`. Tests use real PostgreSQL and
synthetic physical-provider observations; they do not establish provider IO,
performance, deployment or hosted CI. Sol reviewed the SQL and lifetime boundaries.

An opaque destination member mixing an opaque and a typed historical source still
needs an explicit two-source negative regression. Observed-runtime CREATE and
atomic publication remain unfinished, and public restore stays disabled.
