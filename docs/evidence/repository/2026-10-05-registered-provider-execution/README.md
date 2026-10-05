# Registered publication execution over real storage

The native execution fixture now uses the journaled session factory for accepted
publication, retained rejection, CREATE acknowledgment loss, CREATE rollback and
decision acknowledgment loss. Existing unjournaled rejection and fresh-process
recovery scenarios remain; the accepted case runs last because it updates the
shared source revision. Unjournaled acceptance remains covered by the runtime
fixture in the same storage gate.

These cases use actual uploads, retained provider reads, runtime schema checks
and PostgreSQL transactions. Retry supplies no payload bodies and fails if upload
or schema-resolution callbacks are invoked. The fixture checks one claim,
preparation and modes row; sticky V83 markers on rejected/uncertain execution;
no marker for accepted publication; and no second assessment. Rejection receipts
must match the journal's exact assessment UUID and retention deadline. Reader
slots and payload budgets drain.

Command: `:protomolt-repo-container:admissionStorageTest`. The retained wrapper XML
records the production-JAR fixture result. Faults are injected around real SQL
commit; no successful storage path is mocked. This is LocalStack correctness
evidence, not RustFS performance or a horizontal scaling result.

No production code or contracts changed in this checkpoint. Ordinary runtime
session creation remains unjournaled pending scoped journal authority, denial
retention and destination-creation policy. No automatic failover is enabled.
