# Scoped journaled provider execution

Base `163bbaa8719bd9375811601d9aa559556791c5e5`. October 5, 2026 local,
October 6 UTC. Real PostgreSQL/LocalStack with the production-JAR admission runtime.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Passed in 2m37s. The single wrapper test requires the new
`SCOPED_NATIVE_ASSESSMENT_EXECUTION_OK` child marker in addition to the existing
matrix markers. The five added cases use the real scoped caller for accepted
publication, typed rejection, CREATE acknowledgment loss/rollback, decision
acknowledgment loss and same-command retry. Existing process-authority cases remain.
The initial probe compilation needed a checked-exception functional interface for
denial callbacks; that was corrected before the successful run.

The independent source is genuinely published to versioned provider storage before
schema policy activation and carries READ and WRITE permission. Every scoped case
updates that source, with accepted publication last. Actual uploads, retained reads,
runtime validation, journal identity, receipt binding, no second resolution/upload,
manager retention and budget drain use the existing assertions. No success-shaped
provider fixture or process-authority substitution is used for scoped execution.

Wrong-account calls receive NOT_FOUND before registration and on retry. Initial
denial leaves zero claim/preparation/modes/owner/start rows and zero manager capacity,
upload-backend resolution or schema resolution. The S3 handle was opened already;
these counters do not prove zero provider client construction or every provider call.

Sol reviewed the wiring and found no blocker. This qualifies same-session scoped
updates, not scoped creation, API-key transport, partial-registration process restart,
claim transfer or ordinary host activation. No production or protobuf edits.
