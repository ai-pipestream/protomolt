# Shared publication input validation

All 39 repository SPI tests passed, with no failures or skips. The runtime
dependency gate passed without SQL, Kafka or provider implementations. The 7 new
input tests cover upload coverage, unknown fields, modes, lengths, SHA-256,
zero-byte content, mixed-member ordinals, limits and cancellation during hashing.

DocumentPublicationInput retains immutable ByteString content without cloning
payloads. It checks count and total bytes before hashing, with cancellation checks
for each 64 KiB segment. The 10 MiB envelope check occurs after parsing; the host
still needs parser limits and concurrent admission budgets. The 8 MiB aggregate
upload allowance is separate from document assembly limits.

Sol reviewed the class. The requested mixed-member fixture passed in the final
suite. No provider, registry or SQL call occurs here. Validation grants no execution
authority and does not prove typed validity or retained object membership. Execution
must check supported operations, credentials, policies, retained modes, trusted
placements and schema selection. The gRPC service remains unmounted.
