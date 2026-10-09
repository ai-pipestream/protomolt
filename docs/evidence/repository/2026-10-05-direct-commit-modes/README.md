# Direct commit mode binding

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`

The real PostgreSQL/LocalStack production-JAR fixture first failed because direct
commit published a typed candidate despite its saved OPAQUE choice. The red XML
contains the actual assertion after that committed write. After the fix, the gate
passed in 2m15s and requires both mismatch-refusal and matching-success markers.

Two additional promoted-assessment cases use exact saved preparation seeds, claimed
owners, real provider uploads, validation and schema artifact staging. A mode
mismatch must yield FAILED_PRECONDITION, no destination row and no terminal result.
Matching typed modes must produce one success and exact receipt replay, retaining
the original reused source object. Registry resolution remains at the fixture's
single expected call. Previously staged artifact claims are not rolled back by
this later publication refusal.

Sol reviewed the final comparator and cases with no blocker. Mode comparison runs
inside the existing authorized commit transaction before drive/part/revision writes.
It adds one bounded boolean query for claimed operations, not another transaction
or provider call. This is correctness evidence, not a latency or capacity benchmark,
raw-SQL schema enforcement, automatic session recovery or crash-failover proof.
