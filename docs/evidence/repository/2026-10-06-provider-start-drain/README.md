# Local provider-start drain

Base: `124297d005d1d3ad5f7e0b8d1a188629f4dcbd6f`, plus the accompanying changes.

`./gradlew :protomolt-repo-container:test --tests '*DocumentUploadCoordinatorIT' --console=plain`

Passed 39 cases in 37 seconds. Compressed XML contains the original outputs. Three
new cases use actual PostgreSQL and versioned LocalStack operations: delayed return
from PUT, delayed return from bounded read-back, and parallel permitted transfers
with a queued third part refused after closure. The tests establish retained byte
ownership and a false transfer-idle result while a return is held, successful
verification of permitted work, exactly two PUTs in the partial case, persisted
observations, explicit incomplete-attempt failure and eventual resource release.
Existing hard interruption, lost-acknowledgment and retry cases also passed.

Sol identified shared-worker failure poisoning during design; gate refusal therefore
returns a distinct not-started result and waits for observation flushing before
failing the operation. Sol's code review also found shutdown ordering could delay
gate closure behind session cleanup. The final runtime closes the provider gate
first. No provider or SQL operation executes under the local permit monitor.

These wrappers delay method return after real provider work; they do not emulate
an in-flight network request. Transfer-only idle excludes SQL settlement, queued
work, retained payloads and provider effects surviving SDK timeout. Per-operation
V90 composition, durable LOCAL_DRAINED and successor execution remain unfinished.
No throughput claim, hosted CI result, merge or deployment is asserted here.

Final source, including close ordering, also passed
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
The production-JAR PostgreSQL/LocalStack regression result is attached as compressed
XML. This is the existing runtime/provider scenario matrix, not a claim that every
retained operation now participates in V90 drain marking. Sol reviewed the final
source and documentation without a remaining blocker.
