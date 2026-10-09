# Delayed real S3 request after caller timeout

The test-only gateway captures one actual signed PUT from `S3BlobStore` and the
AWS SDK. The SDK times out with no successful response. The inbound handler then
closes and drains. A direct HEAD and physical reclamation pass observe that the
object has not reached LocalStack. Only afterward does the fixture forward the
original signed request and receive the backend's actual 200 response/version.
A version-specific GET verifies the original bytes.

A second `S3ObjectReclaimer` pass removes the late version and checks that no
exact-key versions or delete markers remain. A prefix-sharing neighboring object
survives both passes with its original version and bytes.

The gateway bounds headers, body and response; verifies length and SHA-256;
preserves the original raw request target, Host, Authorization and signed headers;
rejects signed Connection before changing transport close behavior; and permits
only one forwarding attempt. It refuses streaming/Expect framing. No successful
storage response is fabricated. The SDK fixture uses one attempt, non-chunked
bodies and required checksums; production retry/timeout configuration is unchanged.

Qualification commands:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DelayedS3PutGatewayIT' --tests '*DocumentSelectedTransferIT' --console=plain
./gradlew :protomolt-repo-container:test --tests '*DelayedS3PutGatewayIT' --console=plain
```

The first run includes five existing real selected-transfer cases and the new
transport case. The final gateway run also places the neighboring object before
the first cleanup pass and checks its bytes after each pass. Sol reviewed the
transport ordering, signing checks and physical-reclamation assertions.
Both commands passed without failures or skips. The final gateway case took
7.633 seconds; this includes fixture/timeout work and is not a latency benchmark.

This is transport and physical-reclamation evidence only. No SQL tombstone or
successor session is involved. The composed recovery case still needs to maintain
the same registered backend identity across both hosts, preserve the late attempt's
unverified state, and reclaim its exact bytes without affecting published successor
content. LocalStack is a correctness fixture, not a performance result or proof for
every S3-compatible implementation.
