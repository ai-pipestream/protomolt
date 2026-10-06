# Required repository network authentication

Baseline: `b15a8d6927f74df3c6f7adc2b3c27e9409bb0418`.

Network startup previously allowed missing credentials to select operator access.
The two retained red cases reproduce that behavior on Netty/HTTP startup and the
standalone HTTP constructor. Startup now refuses missing or blank credentials;
standalone and node-module entry points check before constructing resources.
Trusted in-process access is unchanged.

TCP repo-backed storage has an explicit upstream credential, separate from the
host's inbound operator token. The test starts a real Netty upstream backed by
PostgreSQL and LocalStack, checks wrong-key rejection without stored content,
and performs a successful put/get/delete through the downstream composition.
Configuration tests cover blank values, absent values, redaction, and preservation
through configuration copies. No protobuf or database migration changes.

## Local evidence

- `red.tar.gz`: 2 tests, both fail because tokenless startup did not throw.
- `focused-green.tar.gz`: 16 tests pass, no skips. Netty documents/health/reflection,
  HTTP credentials/contract, shutdown and startup refusal.
- `affected-green.tar.gz`: 149 tests pass, no skips. RemoteBlobStoreIT,
  RepoServiceConfigTest, UploadHttpServerIT, ArchiveServiceIT,
  ArchiveClassificationIT, BoundedArchiveHostIT, BoundedArchiveEmbeddingIT,
  ManagedDocumentHostIT and ManagedArchiveHostIT. Real PostgreSQL/LocalStack.
- `final-green.tar.gz`: 129 tests pass, no skips. Full RepoServiceIT/config checks,
  HTTP boundary/shutdown tests, and real archive upload with missing/wrong tokens
  rejected before entry creation, followed by authenticated successful ingestion.

The upstream wrong-key test initially expected a gRPC exception. The adapter
correctly returns provider-neutral `BlobStoreException` with `UNAUTHENTICATED`;
the test was corrected to assert that public boundary, then passed.

Sol reviewed both listener enforcement and upstream credential wiring. Its upstream
credential finding was fixed; its suggested config tests were added. These are
local checks, not hosted CI, merge or deployment evidence.

## Limits

An operator token grants process authority. Key-specific account and creation
permissions remain separate unfinished work. HTTP upload remains operator-only.
TCP repo storage still uses its existing plaintext channel: authentication does
not establish transport confidentiality. In-process embedding trusts its host.

Packaged-host qualification also passed:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
`packaged-green.tar.gz` retains its production-JAR test result. The runtime
inventory contained 38 artifacts. This checks the existing packaged admission,
storage and recovery scenario against PostgreSQL/LocalStack; it is not a RustFS
performance or horizontal-scaling result.
