# Historical whole-command content preparation

The explicit fragment capture overload validates complete live pinned references
before payload access, preflights the entire command, reserves one aggregate copy
allowance and checks every copied fragment's declared hash. It checks source Uses
again before returning. Failures close the snapshot. Ordinary capture remains gated.

The raw assembly helper has an explicit historical-reference path with checks before
and after assembly. It reports unresolved structured content as not attempted and
refuses typed-required input; it neither chooses admission mode nor resolves schemas.

The PostgreSQL fixture covers historical and new-upload members in one command,
source omission, closed source, closure during copying, later-member hash and size
errors, missing member, capacity exhaustion and cancellation. It also mutates the
original input after success to verify the snapshot owns its bytes. Every refusal
releases the byte budget. The successful case exercises raw structural assembly and
typed-required refusal for both members. Its historically typed source tests the
byte primitive only; it does not authorize a typed-to-opaque downgrade.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentPublicationAssessmentTest' \
 --tests '*DocumentCommandContentTest' --console=plain
```

Result: 47 tests, zero failures/errors/skips, 23 seconds. Local log:
`/tmp/protomolt-historical-content-capture.log`. Sol reviewed the changes with no
blocking finding; `git diff --check` passed.

This fixture uses real SQL/descriptors and explicitly synthetic provider observations.
Whole-command schema assessment, source-classification policy, observed runtime,
CREATE and atomic reference publication remain unfinished. No public restore API or
performance qualification is established here.
