# Scoped mixed successor qualification

Base: `de7083bc1bc5315cbbe58d8dd007fa4954eee3d7`. `source.sha256` identifies the
three changed test sources. No production or protobuf source changed.

`./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`
passed in 8m31s: one aggregate test, zero failures/errors/skips, 509.796 seconds in
JUnit. `gradle.log.gz` and `test.xml.gz` record the complete run. `host.log.gz` is
the complete initial host phase, saved before successful temporary-directory cleanup.
The final lease-expiry cleanup outcome is covered by the complete JUnit result;
no partial cleanup log is presented as its completion evidence.

The production-JAR probe uses real PostgreSQL and LocalStack object storage. After
the original lease expires, a separately supplied process coordinator reserves and
installs a successor. The registered scoped client performs source reads, admission,
START, CREATE, publication and replay. Scoped reservation is explicitly refused.
The expired execution handle cannot perform late CREATE.

Historical bytes are read again from exact retained provider versions. Fresh upload
bytes are explicitly resubmitted and checked against command size/checksum. The
successor's upload ID/token differ from the predecessor and match its own seeds.
The predecessor still has its one admitted object, with no verification or provider
observations. The new revision combines retained physical identities with the selected
successor upload origin; real provider readback, receipt identity, current revision,
replay, two STARTs/one assessment/one revision, sticky publication retry refusal and
capture/payload cleanup are asserted.

Sol reviewed the fixture and authority split. Two archived failed bring-up runs
show the production reservation and capture-drain boundaries correctly rejecting
scoped authority. Both were fixture wiring errors, corrected by using the separate
coordinator only for those private host operations. No production authorization was
weakened to make the test pass.

This qualifies one historical source with one fresh uploaded part through the private
claimed execution path. It does not establish automatic upload-byte recovery, public
historical publication, managed historical orchestration, multi-source recovery or
all policy/revocation race orderings. Hosted CI and merge status are separate.
