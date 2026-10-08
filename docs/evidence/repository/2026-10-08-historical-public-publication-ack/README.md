# Public historical publication acknowledgement loss

Test-only qualification on parent `30216f4b3539d48d5539ec42396486671f4bbc38`.
Sol reviewed transaction targeting, preservation of the existing generation-bound
fault hook, provider-call counters and direct receipt equality. Production sources
and protobuf contracts are unchanged.

The test uses real PostgreSQL and LocalStack S3 through the package-private
historical runtime factory and public repository facade. A JDBC proxy identifies
the transaction inserting `repository_operation_success` for the exact account,
principal and operation. The row's `creation_xid` must match the current SQL
transaction. The proxy delegates the actual commit before throwing SQLState
`08006`, so the first library call loses an acknowledgement for a durable commit.

Before any public retry, the test reads and parses the persisted `result_bytes`.
It then checks:

- Library and authenticated in-process gRPC replies equal that original SQL
  receipt. Exact retries preserve the same receipt.
- The real provider completed one PUT. All retries add no PUT, placement selection
  or schema resolution.
- The operation has exactly one success row, revision commit and assessment owner.
- Actual committed provider versions match size, checksum and uploaded bytes.
  Historical parts retain their physical identity and the original revision remains
  independently readable.
- Maintenance releases the completed generation and shutdown returns payload
  reservations to baseline.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

The aggregate requires `HISTORICAL_PUBLIC_PUBLICATION_ACK_RECOVERY_OK` alongside
the existing initial-owner and public-dispatch cases. The first version passed
before direct SQL receipt comparisons were added. The first strengthened build
failed compilation because an overloaded protobuf parser could not infer the
generic SQL return type; an explicit byte-array local resolves that test-only error.
Only the final successful run is archived as qualification evidence.
It passed in 1m8s: one aggregate JUnit case, 66.806 seconds, zero failures, errors
or skips. The archived XML and Gradle log describe this final source state.

The injected acknowledgement loss is on the initial library call. gRPC qualifies
authorized replay afterward, not a separate network-disconnect fault. This is
local correctness evidence; it does not establish TCP behavior or performance.
