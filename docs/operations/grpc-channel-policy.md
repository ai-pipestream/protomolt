# Outbound gRPC channel policy

`protomolt-grpc-channel-policy` provides the host-owned guardrail shared by outbound gRPC
actions. `ChannelFactory.standard(policy)` validates a target before schema or network work,
enforces plaintext/TLS and deadline limits, caps message sizes, and leases a bounded channel
slot until shutdown. Validation is syntactic and does not perform DNS resolution.

Every call on a policy channel carries the policy's message limits, 4 MiB in each direction by
default (`maxInboundMessageBytes`, `maxOutboundMessageBytes`). A caller can lower them through
`CallOptions` but cannot raise them. An oversized response fails the call with
`RESOURCE_EXHAUSTED`. An oversized request whose marshaller reports its length, which every
protobuf request does, is refused with `RESOURCE_EXHAUSTED` naming both sizes before any of it
is framed or sent.

```java
OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
        .allowedSchemes(Set.of("dns"))
        .allowedHosts(Set.of("*.internal.example"))
        .allowedPorts(Set.of(443))
        .allowPlaintext(false)
        .maxDeadline(Duration.ofSeconds(20))
        .maxActiveChannels(32)
        .maxInboundMessageBytes(8 * 1024 * 1024)
        .maxOutboundMessageBytes(1024 * 1024)
        .build();

ChannelFactory channels = ChannelFactory.standard(policy);
```

Pass that factory to `ServiceWorkspaceActions.register`, `GrpcInvokeAction`, and
`ReflectAction`, or pass the policy to `WorkflowRunner` and the policy-aware
`ProtoMoltCatalog.full` overload. Existing `standard()` callers receive the permissive host
defaults: DNS/IP targets, either transport, a 60-second maximum deadline, 4 MiB messages, and
64 active channels.

`protomolt-serve` builds one policy per process and shares it with catalog actions and the workflow
job worker. The policy can be configured with command-line options or matching environment
variables (`PROTOMOLT_GRPC_ALLOWED_SCHEMES`, `PROTOMOLT_GRPC_ALLOWED_HOSTS`,
`PROTOMOLT_GRPC_ALLOWED_PORTS`, `PROTOMOLT_GRPC_ALLOW_PLAINTEXT`,
`PROTOMOLT_GRPC_ALLOW_TLS`, `PROTOMOLT_GRPC_MAX_DEADLINE_MS`,
`PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS`, `PROTOMOLT_GRPC_MAX_INBOUND_MESSAGE_BYTES`, and
`PROTOMOLT_GRPC_MAX_OUTBOUND_MESSAGE_BYTES`). Allowlist values are comma-separated; booleans must be
`true` or `false`. Invalid values fail startup before any server is opened.

Credential, trust, and client-certificate references are intentionally opaque and are not
accepted by the standard factory. A host that supports them must resolve references through an
explicit channel factory, keep secret values out of profiles and logs, and retain the policy's
target and deadline validation. Custom factories remain a compatibility seam and are not
automatically bounded; migrate them to the policy-aware factory when outbound traffic is in
scope.
