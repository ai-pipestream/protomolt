# Redis PUT acknowledgement loss

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 3m40s. Sol reviewed the fault and assertions.

The wrapper invokes the real Redis PUT, then throws before returning the result.
The test preserves that exception as the cause and confirms matching stored bytes
with a bounded read and SHA-256. SQL still names the original document revision.
Immediate retry of the exact operation fails because the selected attempt is not
verified; SQL remains unchanged. A separate operation then publishes the update.
Both committed revisions pass the existing history and restart checks.

The separate operation is not recovery of the failed operation. This checkpoint
does not prove reclamation of the unverified bytes, late-write cleanup, or resumed
publication under the original operation. Those remain required acceptance work.
