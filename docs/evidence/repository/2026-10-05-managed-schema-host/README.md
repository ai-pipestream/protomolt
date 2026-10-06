# Optional managed-host schema scopes

`ManagedSchemaAccess` is a host-supplied scope and lifecycle interface. The new
four-argument `RepoServices.build` overload accepts it alongside independently
optional historical transport. A successful build transfers exclusive lifecycle
ownership; failed construction leaves cleanup with the caller. The component's
registry stores remain borrowed and must outlive successful service shutdown.

The internal publication wrapper forwards the actual caller/member through
`executeScoped`. Shutdown rejects publication and schema admission, drains scope
cleanup, then drains abandoned registry loads before releasing shared resources.
A timeout keeps the database/provider resources for a later close attempt. No
publication RPC or policy-administration API is enabled. The existing native
publication accessor remains package-private.

The service exposes admission types on its Java API but has no production
dependency on `protomolt-repo-schema-registry`. The attached resolved runtime graph
confirms this; the adapter is a test dependency. Admission and registry adapter
runtime boundary checks also passed.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*ManagedSchemaHostIT' --tests '*ManagedDocumentHostIT' \
  --tests '*ManagedArchiveHostIT' --tests '*ManagedArchiveReadShutdownIT' \
  --tests '*LifecycleShutdownTest' \
  :protomolt-repo-admission:check :protomolt-repo-schema-registry:check --console=plain
./gradlew :protomolt-repo-service:dependencies --configuration runtimeClasspath --console=plain
```

The combined check passed in 32s, including 15 service tests. Four new tests use
real PostgreSQL, LocalStack and Git registry artifacts:

- Typed publication resolves the registered descriptor; opaque publication and
  terminal replay perform no schema lookup. An existing-document update preserves
  the exact scoped caller. Revoking schema access denies that update with a warm
  cache; restoring access permits the same command to finish.
- A held real registry read outlives its canceled publication call. Shutdown times
  out while the host database remains usable and new work is refused. Releasing
  the provider allows a repeated close to release the database and cached bytes;
  the borrowed registry remains usable.
- Missing managed-storage qualification fails before acquisition and leaves schema
  lifecycle ownership with the caller.
- A deliberate SQL failure during reader registration in qualified startup also
  leaves schema access open for caller cleanup.

The fixture initializes the real guarded schema-policy catalog directly because
policy administration has no managed public API yet. Timing interception delegates
to the actual Git provider; it never manufactures descriptor results. Sol reviewed
ownership, caller forwarding, startup failure and shutdown ordering.

This is local composition/lifecycle qualification. It does not enable ordinary
durable session registration, automatic claim transfer, a public publication API,
non-S3 archival durability or a managed deployment.
