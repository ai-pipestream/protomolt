# gRPC service workspaces

A service workspace gives a live gRPC endpoint a stable identity. ProtoMolt
reflects the endpoint once, persists its service profile, and stores its
descriptor set in the schema registry,
and exposes bounded service and method contracts to MCP clients. An agent can
inspect the service again after either process restarts without carrying a
large base64 descriptor through model context.

## Configure storage

Both runnable servers accept the same option:

```shell
protomolt-serve --service-workspace /srv/protomolt-services
protomolt-mcp --service-workspace /srv/protomolt-services
```

Set `PROTOMOLT_SERVICE_WORKSPACE` instead when the host supplies configuration
through the environment. The directory is created on first use. Profiles are
stored as binary protobuf messages under `profiles/`. When `--registry-git` is
configured, reflected descriptor sets are stored under `descriptors/sha256/`
in that Git repository. Existing artifacts under the service workspace are
migrated into the registry when first read. A standalone service workspace
without a registry retains the legacy artifact store for compatibility.
Profile replacements are atomic.

Without configured storage, `service-register`, `service-list`,
`service-inspect`, `service-refresh`, and `service-invoke` stay discoverable
but return the
structured `unavailable` error and the required launcher option.

## Register a reflected service

This example registers a plaintext development endpoint through the REST
surface. The same JSON is the `service-register` MCP tool input.

```shell
curl -sS -H 'content-type: application/json' \
  -d '{
    "profile": {
      "name": "orders-local",
      "description": "Local order service",
      "endpoints": [{
        "name": "local",
        "host": "127.0.0.1",
        "port": 50051,
        "transport": "TRANSPORT_PLAINTEXT"
      }]
    },
    "endpoint": "local",
    "deadlineMs": 5000
  }' \
  http://localhost:8080/grpc-json/ProtoMoltService/ServiceRegister
```

Registration validates the profile before opening the network connection,
uses gRPC server reflection, fingerprints the returned `FileDescriptorSet`,
and stores it as a content-addressed registry artifact. The response includes service
and method summaries but never the descriptor bytes.

ProtoMolt tries the stable `grpc.reflection.v1` protocol first. It falls back
to `grpc.reflection.v1alpha` only when the endpoint reports that the stable
protocol is unimplemented. Both attempts share the caller's reflection
deadline.

Profile names and endpoint names are stable identifiers. Each endpoint makes
the transport explicit and separates `host` from `port`. TLS uses the JDK's
normal trust configuration when no custom reference is present. Credential,
custom-trust, and client-certificate fields contain only opaque host references,
never secrets. The default host currently refuses those references until a
credential resolver is configured, so it cannot accidentally treat them as
credential material.

## Inspect and refresh

List and inspect profiles without contacting their targets:

```json
{}
```

is the `service-list` input. To filter by name:

```json
{"name": "orders-local"}
```

is the `service-inspect` input. Inspection returns the persisted connection
profile plus each reflected method's fully qualified name, streaming flags,
input and output types, and top-level field shapes.

Refresh only when the deployed schema may have changed:

```json
{"name": "orders-local", "endpoint": "local", "deadlineMs": 5000}
```

`service-refresh` re-runs reflection, advances the stored schema identity, and
returns `changed: true` only when the descriptor fingerprint differs.

## Invoke a registered service

Use the profile identity after registration. The target, TLS mode, and pinned
descriptor are resolved inside ProtoMolt:

```json
{
  "name": "orders-local",
  "method": "orders.v1.Orders/GetOrder",
  "request": {"id": "order-123"},
  "endpoint": "local",
  "deadlineMs": 5000
}
```

This is the `service-invoke` input. The descriptor set is never part of the MCP
or REST request. ProtoMolt verifies the registry artifact against the profile's
SHA-256 fingerprint before parsing the request or opening a channel. The action
supports unary and server-streaming methods and makes one attempt. A configured
method deadline is enforced as a ceiling. Methods marked as requiring approval
are refused because a tool call cannot approve itself.

## MCP resources

Once a workspace is configured, service contracts are available as JSON MCP
resources in addition to tools:

| URI | Contents |
|---|---|
| `protomolt://services` | All registered identities and schema fingerprints |
| `protomolt://services/{profile}` | One profile and all reflected service/method contracts |
| `protomolt://services/{profile}/methods/{full-method}` | One method contract |

Profile and method path segments are URL-encoded. Resources and inspection
responses deliberately omit raw descriptor bytes and credential material.
Resource discovery lists the root and profile URIs only; read a profile to
discover its exact method names, then address an individual method URI.

The intended agent loop is `service-register` once, inspect the stored method
contract, invoke the exact method with `service-invoke`, verify its status, and
refresh only on an explicit schema-change boundary. Use raw `reflect` for
short-lived exploration and `grpc-invoke` for unregistered targets.

## Current boundary

Reflection-enabled services can be registered and invoked. Registry subjects
and uploaded descriptor sources are represented in the profile contract but
are not yet registration inputs. Service profiles also carry health-probe and
per-method operational policy fields. Invocation enforces approval and deadline
policy. Automatic retries and health probes are not executed yet.

Descriptor artifacts are immutable and saved before the profile that points to
them, so a process failure cannot create a profile with a missing artifact. A
failure between those writes can leave an unreferenced artifact. Registry
maintenance may remove unreferenced artifacts later.

See the [gRPC workflow workbench plan](../transform/workflows.md) for the
workflow, run-evidence, structured-inference, and standalone-application phases.
