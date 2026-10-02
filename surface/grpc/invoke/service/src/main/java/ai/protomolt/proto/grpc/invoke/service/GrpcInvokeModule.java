package ai.protomolt.proto.grpc.invoke.service;

import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.GrpcInvokeAction;
import ai.protomolt.proto.grpc.invoke.ReflectAction;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;

import java.util.Map;
import java.util.Set;

/** Supplies gRPC actions restricted to an explicitly configured target and transport. */
public final class GrpcInvokeModule implements ServiceModule {
    public static final String TARGET = "PROTOMOLT_GRPC_TARGET";
    public static final String TRANSPORT = "PROTOMOLT_GRPC_TRANSPORT";

    @Override
    public String role() {
        return "grpc-invoke";
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        String target = required(context.environment(), TARGET);
        String transport = required(context.environment(), TRANSPORT);
        boolean tls = switch (transport) {
            case "tls" -> true;
            case "plaintext" -> false;
            default -> throw new IllegalArgumentException(TRANSPORT + " must be tls or plaintext");
        };
        // Parsing validates syntax without DNS resolution or network activity. The serving
        // policy then admits only that host, port, scheme and transport, across both actions.
        var parsed = OutboundChannelPolicy.defaults().validateTarget(target, tls);
        var policy = OutboundChannelPolicy.builder()
                .allowedSchemes(Set.of(parsed.scheme()))
                .allowedHosts(Set.of(parsed.host()))
                .allowedPorts(Set.of(parsed.port()))
                .allowTls(tls)
                .allowPlaintext(!tls)
                .build();
        var channels = ChannelFactory.standard(policy);
        context.contributions().contribute(ProtoAction.class, new ReflectAction(channels));
        context.contributions().contribute(ProtoAction.class, new GrpcInvokeAction(channels));
        // Channels belong to individual calls; wiring starts no background work.
        return ServiceMount.inert(() -> {});
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
