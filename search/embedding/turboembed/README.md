# Turboembed provider

Implementation slice (base `7aa5f2526`, 2026-11-01): reuse the embedding SPI,
existing KServe protobuf contract and FP32 tensor decoder. Add explicit query
and document purposes, without changing symmetric providers. Index mapping and
chunk derivation use document purpose; search uses query purpose.

Call path: mapping/chunk/query -> EmbeddingProvider -> Turboembed KServe gRPC
ModelInfer -> verified model revision, manifest digest and finite FP32 vectors.
The provider is optional; no repository-service runtime dependency is added.
No protobuf/JSON conversion, generated class loading or new document API.

Acceptance: purpose reaches the actual RPC; exact model/version/manifest and
vector dimensions are enforced; wrong identity, malformed tensor, non-finite
vectors and remote failure refuse; caller-owned channel remains caller-owned;
real Turboembed qualification is separate from an in-process protocol fixture.

This slice does not claim the complete repository -> gRParse -> Rust search
workflow is connected or qualified. That still requires revision-bound writes,
index receipts, ACL/delete propagation and an end-to-end deployment test.

## Mounting

Include `protomolt-search-embedding-turboembed` in the search host. The full document
platform distribution includes this optional provider; the repository-only
assembly does not. Select it using `DOCUMENT_PLATFORM_SEARCH_EMBEDDING_PROVIDER=
turboembed-v1:<manifest-sha256>` (one value with no newline). Supply all six
`PROTOMOLT_TURBOEMBED_*` settings listed in the environment-variable reference.
The manifest-bound provider ID is part of the policy digest: changing the bundle
cannot reuse the old index snapshot identity. The `v1` identifies this client's
purpose/normalization protocol. Indexing calls `embedDocument`; queries call
`embedQuery`; their batched forms `embedAllDocuments` and `embedAllQueries` keep
the purpose. Ordinary `embed`/`embedAll` explicitly select no prefix.

A ModelInfer call sends raw UTF-8 text in the standard BYTES tensor `texts`,
requests no truncation and unquantized vectors, and accepts only finite FP32
`vectors` matching the declared dimension. Calls have 30-second deadlines and
batches of every purpose split at 32 texts. All instances share a process-wide
RPC cap, default 8, configured by `protomolt.embeddings.turboembed.max-in-flight`
or `PROTOMOLT_TURBOEMBED_MAX_IN_FLIGHT`. Admission has a separate interruptible
30-second timeout. Overlapping configurations must agree; see the
[configuration and lifecycle contract](../../../docs/search/embeddings.md#process-wide-tei-and-turboembed-admission).
A server with a smaller capacity refuses;
there is no client retry that changes the input, model, purpose or precision.
The constructor taking a ManagedChannel leaves credentials and channel lifetime
with its caller. Environment-configured plaintext must be chosen explicitly.

## Qualification

The in-process KServe fixture checks protocol, failures and identity. The separate
`TurboembedLiveIT` uses an explicitly configured native server; it requires a
bundle with a query prefix and fails if configuration/server/model is absent.
Run it under `:integrationTest` with all six environment variables set. Use
`--rerun` on that task when changing external servers, since their identity is
not a Gradle file input. These live tests validate the provider only, not search
quality or the complete gRParse/repository/Rust-search workflow.


Qualified on 2026-11-01 against the native `turbo-kserve` binary SHA-256
`e5762a02d268b3a2c9a0ce1ec807cceb37c8c2eabee8a2aee7896270ad5cfe25`
from the local Turboembed `71f772ab5` build. The tiny test bundle was served as
`tiny-bert-bundle`, revision `1`, dimension `32`, with manifest SHA-256
`273ee3dfe946989a27a68e37d58e1dd7bc537c06b404075279f02a1b640b91f7`.
This is a protocol fixture model, not a production retrieval-quality benchmark.
The 8 in-process integration cases and 1 native case passed without skips.
