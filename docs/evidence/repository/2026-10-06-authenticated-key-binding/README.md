# Authenticated key identity

Baseline: `333841109af2ef5bf4fb1c8716083fd49de4d082`.

Optional provisioned credential identity now crosses resolver chaining and the
gRPC context without a second credential lookup. The explicit document repository
host mapper receives the complete authenticated result and must preserve principal,
authority and exact issuer/key ID/generation. Raw tokens are not repository values.

## Evidence

- `authz-green.tar.gz`: 38 tests, no failures or skips. Includes preserved first
  resolution, no upgrade of an unbound match, and no fallback after resolver outage.
- `grpc-green.tar.gz`: 13 tests, no failures or skips. Real in-process gRPC calls
  distinguish two keys for one principal; forged identity metadata has no effect.
  Operator and legacy credentials stay unbound. Inconsistent contexts are refused.
- `spi-green.tar.gz`: 32 existing SPI tests pass, no skips. The SPI runtime dependency
  gate passes; its dependencies did not change.

The new identity seam is additive Java code; no protobuf or SQL migration changes.
The existing principal-only host constructor refuses an identified credential when
its mapper drops identity. The new explicit mapper is required for such callers.
The default adapter preserves identity with no implicit account membership.

Sol reviewed identity preservation and fail-closed binding checks. Credential
provisioning, durable grant state, current-key revocation and publication/recovery
enforcement remain unfinished. Shipped policy and OIDC resolvers have no provisioned
key IDs and remain unbound. This is not an available scoped creation API.

`repository-green.tar.gz` retains all 36 RepoServiceIT cases passing with no skips.
The new case uses real Netty, PostgreSQL and LocalStack: two keys sharing a
principal read the stored document through distinct host bindings. Substituting
another key identity or dropping the binding returns PERMISSION_DENIED.

`packaged-green.tar.gz` retains the passing production-JAR admission/storage/recovery
scenario with PostgreSQL/LocalStack. It passed with no skips. These are local
checks, not hosted CI, deployment or scaling evidence.
