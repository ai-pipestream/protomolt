# Managed publication transport contract

This is a staged contract; no host mounts the service. PublishDocument reuses the
existing intent, committed result and rejection receipt. Request data contains
member modes and indexed upload bytes, without provider configuration, credentials,
process rights or descriptor implementations.

Protobuf compilation passed with complete imports. All 5 tests passed for generated
and dynamic messages using ProtoMolt's validator. They cover exact mode membership,
unknown/unset modes, payload coordinates and byte limits, required outcomes and
nested receipt validity. Separate fixtures show that missing/duplicate uploads and
incorrect checksums can pass annotation validation: the shared implementation must
check them before execution. JSON Schema tests record list limits and the runtime
CEL metadata; this does not claim complete OpenAPI constraint translation.

Buf lint passed for the new file. The repository compatibility script passed
against e7e760d6b2867d51343ca4a9c158540a42655c93 after the final RPC name change.
Existing protobuf files, tags, service methods and Any URLs are unchanged.

Remaining implementation: provider-neutral shared request validation and receipt
binding, host selection of placements/container definitions, current authentication,
retained mode checks on terminal retries, bounded request/response admission,
cancellation and accepted-call shutdown. Then run the same real PostgreSQL/provider
cases locally and through gRPC before mounting or advertising the service. Large
streaming uploads require their own lifecycle contract; this buffered request does
not imply unlimited upload size.
