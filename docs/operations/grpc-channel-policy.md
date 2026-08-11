# Outbound gRPC channel policy

`protomolt-grpc-channel-policy` provides the host-owned guardrail shared by outbound gRPC
actions. `ChannelFactory.standard(policy)` validates a target before schema or network work,
enforces plaintext/TLS and deadline limits, and leases a bounded channel slot until shutdown.
Validation is syntactic and does not perform DNS resolution.

```java
OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
        .allowedSchemes(Set.of("dns"))
        .allowedHosts(Set.of("*.internal.example"))
        .allowedPorts(Set.of(443))
        .allowPlaintext(false)
        .maxDeadline(Duration.ofSeconds(20))
        .maxActiveChannels(32)
        .build();

ChannelFactory channels = ChannelFactory.standard(policy);
```

Pass that factory to `ServiceWorkspaceActions.register`, `GrpcInvokeAction`, and
`ReflectAction`, or pass the policy to `ChainRunner` and the policy-aware
`ProtoMoltCatalog.full` overload. Existing `standard()` callers receive the permissive host
defaults: DNS/IP targets, either transport, a 60-second maximum deadline, and 64 active
channels.

`protomolt-serve` builds one policy per process and shares it with catalog actions and the chain
job worker. The policy can be configured with command-line options or matching environment
variables (`PROTOMOLT_GRPC_ALLOWED_SCHEMES`, `PROTOMOLT_GRPC_ALLOWED_HOSTS`,
`PROTOMOLT_GRPC_ALLOWED_PORTS`, `PROTOMOLT_GRPC_ALLOW_PLAINTEXT`,
`PROTOMOLT_GRPC_ALLOW_TLS`, `PROTOMOLT_GRPC_MAX_DEADLINE_MS`, and
`PROTOMOLT_GRPC_MAX_ACTIVE_CHANNELS`). Allowlist values are comma-separated; booleans must be
`true` or `false`. Invalid values fail startup before any server is opened.

Credential, trust, and client-certificate references are intentionally opaque and are not
accepted by the standard factory. A host that supports them must resolve references through an
explicit channel factory, keep secret values out of profiles and logs, and retain the policy's
target and deadline validation. Custom factories remain a compatibility seam and are not
automatically bounded; migrate them to the policy-aware factory when outbound traffic is in
scope.
