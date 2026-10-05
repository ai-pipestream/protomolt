# Durable assessment-start prerequisite

Targeted command: `./gradlew :protomolt-repo-container:test --tests '*DocumentAssessmentStartJournalIT' --tests '*DocumentPublicationModesJournalIT' --tests '*DocumentAssessmentRetentionIT' --console=plain`

29 PostgreSQL cases passed: five start-journal, seven mode-journal and seventeen
existing assessment-retention cases. Green log and JUnit XML are retained here.
The same-transaction test first failed because no exception was raised; the red
XML records that behavior. V83 now stamps the marker transaction and refuses
CREATE until a later transaction, preserving the independently committed marker.

The new tests cover exact retries without deadline renewal, fresh-loader recovery
of coordinates, withheld commit acknowledgment, changed identity/retention,
immutability, wrong owner, invalid retention, and direct SQL creation with a missing
or mismatched start. Direct INSERT inputs use synthetic invalid evidence solely to
exercise the earlier identity guard. Correct-binding acceptance is tested through
the real SQL check, not represented as a fully sealed assessment fixture.

Sol reviewed the final implementation with no blocker for this prerequisite.
Automatic session restoration and the full journaled CREATE/decision flow remain
unactivated and unqualified. No throughput, multi-host recovery or deployment claim
is made. Public protobuf contracts are unchanged.

The production-JAR PostgreSQL/LocalStack `admissionStorageTest` also passed in
2m14s, preserving the existing provider-backed assessment, publication, historical
read and transport paths after V83. This is regression evidence for those existing
paths, not proof of a full journaled assessment execution.
