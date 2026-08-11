# Service advertisement and schema exchange

## Objective

Let an authenticated node advertise a gRPC service, reflect its descriptors,
create a TTL-bound registry entry, generate clients, and invoke the service
without a preinstalled client library.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md). Reuse
`ServiceProfile`, service workspaces, reflection loaders, descriptor
fingerprinting, `DynamicGrpcClient`, and stub generation.

## Ownership

Add advertisement contracts and an `AdvertisementRegistrar`. Keep route
selection, general mesh transport, and durable storage behind interfaces.

An advertisement declares:

- node, advertisement, service, and endpoint identifiers;
- direct or advertising-node-relayed reachability;
- TLS mode, authority, trust policy reference, and credential reference;
- reflection service name and expected descriptor fingerprint;
- accepted method and type constraints;
- capability and capacity declarations;
- issue, refresh, and expiry times; and
- registry namespace and publication policy.

The credential reference is opaque and resolves only at connection time.
Advertisement payloads never contain credentials.

## Registration flow

1. Authenticate the node and validate the advertisement.
2. Evaluate outbound scheme, hostname, port, DNS resolution, address range,
   TLS, trust-domain, and relay policy.
3. Reflect the advertised service without following redirects.
4. Canonicalize the descriptor closure and compute its SHA-256 fingerprint.
5. Reject an expected fingerprint mismatch.
6. Build a TTL-bound `ServiceProfile` and add it to the service workspace.
7. Publish the descriptor artifact and optional immutable registry revision.
8. Expose the service through discovery resources and typed actions.
9. Generate a client bundle on demand and support dynamic invocation.
10. Refresh or expire the workspace entry with the advertisement lease.

An expired advertisement must not silently remain callable. A pinned registry
revision can outlive discovery only when local policy explicitly promotes it.

## Security and validation

Protect against SSRF, DNS rebinding, loopback and link-local access, insecure
downgrade, hostname mismatch, descriptor substitution, oversized descriptor
sets, reflection recursion, and advertisement flooding. Validate both the
advertised name and the resolved address at connection time.

Index stable service id, node id, method name, type name, fingerprint, state,
and expiry only where discovery queries require them. Endpoint credentials,
trust material, and full descriptors are not search fields.

## Tests

Use an in-process reflected gRPC service and fake clock. Cover direct
registration, relay registration, client generation, dynamic `Any` invocation,
refresh, expiry, fingerprint mismatch, reflection disabled, address-policy
rejection, descriptor limits, duplicate refresh, and promotion behavior.

## Acceptance criteria

- A custom reflected service becomes discoverable and callable from another
  in-process node without checked-in generated client code.
- The generated client compiles against the reflected descriptor.
- Every failure occurs before registry publication or remote method execution.
- Expiry removes the live workspace entry and records a non-secret reason.

## Exclusions

Do not implement arbitrary Internet crawling, request-supplied endpoints,
service health orchestration, or the complete mesh entity stream here.
