# Historical client rejects malformed responses

The production-JAR PostgreSQL/LocalStack harness captures a valid selected-history
response from the real authenticated service. A separate in-process test endpoint
then replays copies with one changed field: revision, Any value bytes, or descriptor
artifact bytes. This endpoint tests client verification only; it does not stand in
for provider success, persistence or authorization.

`HistoricalOccurrenceClient` must report DATA_LOSS for each response and release
all byte reservations. After each refusal, an unmodified captured response on the
same client must decode the expected retained value. Holding that result exhausts
the client's single call slot; closing it releases all reservations. Successful
retry demonstrates that an invalid response does not leak the call slot.

The surrounding probe retains its real-server authentication, cross-account denial,
revocation and selected historical-read checks. Sol reviewed the added verification
and capacity assertions without finding a blocker. No production implementation
change was needed.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The production-JAR harness passed in 2 minutes 38 seconds, including the new
unconditional client-verification probe. Local log:
`/tmp/protomolt-historical-client-malformed-qualified.log`.
This is correctness coverage on LocalStack, not a RustFS performance measurement.
Hosted CI, merge and deployment are separate.
