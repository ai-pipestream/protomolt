# Repository schema registry adapter

`protomolt-repo-schema-registry` connects the existing `SchemaRegistryStore` to
`DocumentSchemaAdmission.Resolver`. It caches verified descriptor bytes and owns
leases through an admission attempt. It does not mount a service or authenticate
requests. Repository admission remains usable without this module.

Construct one `RegistrySchemaResolver` for each host-authenticated registry and
security context. Its store is borrowed. The host's `Selector` must authorize each
occurrence and return exact `RepositorySchemaAsset` metadata plus any retained
source bundle. Selection runs again for cache hits and repeated occurrences;
registry IDs and client-supplied metadata are not authorization grants.

The registry store exposes source versions and descriptor artifacts but does not
supply all archival compiler provenance. The selector must provide actual observed
or reported provenance, or an explicit unknown origin where the contract allows it.
The adapter does not compile a new schema or substitute a latest version.

An attempt is thread-confined and must stay open through admission's owned copy:

```java
try (var attempt = resolver.open(authorizedSelector, control);
     var proof = DocumentSchemaAdmission.prepareAndCheck(
             preparation, attempt, admissionLimits, reservations, control)) {
    // Consume the validated proof under the repository's current publication rules.
}
```

Use the overload with reservations when proof assets must outlive the attempt.
The proof owns its copied assets until it is closed. Returned `Definition` values
borrow descriptor leases and host-owned metadata/source; never retain those values
after closing their attempt. Cache presence is not archival retention.

Attempt admission and cache capacity fail without waiting. Bounds cover concurrent
attempts, distinct descriptor artifacts per attempt, cache entries and serialized
cache bytes. Store input allocation, source/metadata ownership, linked descriptor
heap and provider I/O timeouts remain host/provider responsibilities. The built-in
Git store bounds descriptor reads to 16 MiB. Synchronous store calls check control
before and after I/O; this adapter cannot interrupt an arbitrary provider call.

`MissingDescriptor` means the selected artifact was absent. Unsupported descriptor
storage fails construction. Registry I/O, access, corruption and cancellation
propagate as failures without negative caching or opaque-success fallback. An
already cached immutable artifact needs no further artifact read, but the selector
must still authorize its use. Typed admission checks each candidate independently.

Closing the resolver rejects new work and evicts idle entries. Pinned entries remain
until their attempts close. The host must drain active attempts before closing the
borrowed store; resolver close alone does not wait for blocked provider calls.

For native publication, `DocumentPublicationRuntime.executeScoped` accepts a
`SchemaScopes` factory. Return `resolver.open(...)` from that factory, capturing
the supplied authenticated caller and member in the selector. The runtime opens
scopes lazily only when typed selection is needed and closes them after execution.
Terminal replay and opaque members do not open registry attempts. Runtime shutdown
also waits through scope cleanup. A factory that allocates and fails before returning
its scope must release its own partial allocation.

The production dependency gate excludes Git/JGit, repository server/container,
SQL, Kafka and object-store SDKs. Git is used only by the adapter's integration
fixtures. Concurrent registry-load coalescing and managed repository-host mounting
are not implemented by this module.
