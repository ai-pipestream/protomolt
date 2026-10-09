# Private coordinator binding

Base: `af91e0aae6be401c81086a54871d86116ba88c9f`, with the adjacent working changes.
Sol reviewed V89, registration, resume and tests. The review found a legacy exact-
token retry bypass; the final guard refuses that path and the focused tests cover it.

The combined focused command is recorded in the adjacent schema-cache-stats evidence.
It passed 52 PostgreSQL repository cases and nine registry cases in 38 seconds.
Attached XML covers claim acquisition, preparation and managed sessions. Five
cancellation checkpoints test atomic claim/binding/preparation persistence. Cases
also refuse retroactive Java API adoption, foreign-incarnation resume, legacy retry
and transferred-epoch fallback while preserving exact retry identity and capacity.

The production-JAR PostgreSQL/LocalStack test includes a fresh journaled manager
replaying both committed success and terminal rejection with unchanged receipts,
no further uploads/schema resolution and no retained session capacity. This is
correctness qualification; RustFS remains the local performance backend.

V89 records an initial identity only. Java claim INSERT evidence prevents adoption
of a pre-existing claim through this API. The SQL trigger checks live fencing and
ordering but does not independently prove claim creation in the same transaction.
Privileged SQL is not the API. No durable drain, provider quiescence, successor
execution or default runtime activation is established by these tests.

Final source passed `./gradlew :protomolt-repo-container:admissionStorageTest
--console=plain`, including the legacy-retry guard and cache statistics. The attached
provider XML is the aggregate production-JAR probe result; its single JUnit case
runs the provider scenario matrix. This does not assert hosted CI, merge or deployment.
