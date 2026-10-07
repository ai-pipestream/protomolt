# Redis cleanup error and retry

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 7m44s. Sol reviewed the fault and resource lifetime.

After normal host cleanup, the test recreates original bytes with a real PUT.
An injected cleanup exception must produce RETRY with the original failure,
DELETING in SQL, a bounded diagnostic and no absence timestamp. Bytes remain.
The next call uses the real reclaimer and confirms absence. Both committed
histories and retention filters pass afterward.

This models reappearance with a new PUT. It does not simulate a network request
across process exit or qualify scheduled recheck timing. Direct recovery uses
the original backend identity. No production code changed.

A fixture initially read after provider close. It now clones verified bytes
while the provider is open and saves that snapshot after shutdown.
