# Managed archive GetEntry response admission

The bounded managed profile now activates the library read gate and reserves a
maximum GetEntry response allowance at gRPC headers, using the host's shared budget.
Library construction lasts through bounded provider completion and verification;
transport capacity lasts through the terminal listener callback. Other metadata/list
responses, caller-retained protobufs and network buffers are outside this claim.

Validation, 40 tests passed with none skipped in 25 seconds:

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveTransportIT' --tests '*BoundedArchiveOptionsTest' \
  --tests '*UnaryRequestAdmissionTest' --tests '*RepoBoundedArchiveMainTest' \
  --tests '*BoundedArchiveHostIT' --tests '*BoundedArchiveEmbeddingIT' --console=plain
```

Real PostgreSQL/Redis and authenticated Netty cases prove local/remote historical
reads, metadata-envelope refusal before GET, and cancellation during a held actual
Redis GET acknowledgment. Both reservations remain held while the synchronous handler
uses the provider. A shutdown timeout preserves resources until completion; a later
close succeeds. Existing write cancellation, bootstrap and embedding regressions pass.

A separate synthetic protobuf echo service over real Netty verifies the final send
boundary: an oversized protobuf returns RESOURCE_EXHAUSTED without delivery, capacity
drains, and a valid retry succeeds. Its cancellation case checks the extra response
reservation. This fixture is transport coverage, not a substitute for provider tests.

Public options expose `maxResponseBytes`, defaulting to the request cap through the
old constructor. The launcher exposes `DOCUMENT_PLATFORM_ARCHIVE_MAX_RESPONSE_BYTES`.
The configured budget must cover a maximum transport write and a maximum read's
request, construction and transport allowances. Sol reviewed the shared lifetime and
accounting design without a blocker. These are local checks, not hosted CI, merge
or deployment evidence. Process-authority semantics remain unchanged.
