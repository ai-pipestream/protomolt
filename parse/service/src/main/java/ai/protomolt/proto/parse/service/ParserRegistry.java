package ai.protomolt.proto.parse.service;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The coordinator's parser fleet: {@code parser_name → gRPC target}, with a
 * lazily opened, cached {@link ParserClient} per parser. The map is service
 * configuration (like the routing rules) — parsers register by deployment,
 * not by RPC.
 *
 * <p>Targets are {@code host:port} authorities or
 * {@code inprocess:<name>} for same-JVM parsers (tests, all-in-one
 * deployments). An unknown parser is NOT an error here: {@link #lookup}
 * answers empty and the coordinator records a FAILED result for it, because
 * a misconfigured plan must be visible in the stored document, not thrown
 * away.
 */
public final class ParserRegistry implements AutoCloseable {

    private final Map<String, String> targets;
    private final ConcurrentHashMap<String, ParserClient> clients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    private ParserRegistry(Map<String, String> targets) {
        this.targets = Map.copyOf(targets);
        targets.forEach((name, target) -> {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("parser name must not be blank");
            }
            if (target == null || target.isBlank()) {
                throw new IllegalArgumentException(
                        "parser '" + name + "' has a blank target");
            }
        });
    }

    /**
     * Builds the registry from configuration entries.
     *
     * @param targets {@code parser_name → target} ({@code host:port} or
     *        {@code inprocess:<name>})
     * @return the registry; channels open lazily on first use
     */
    public static ParserRegistry of(Map<String, String> targets) {
        if (targets == null) {
            throw new IllegalArgumentException("targets must not be null");
        }
        return new ParserRegistry(targets);
    }

    /**
     * Builds the registry from a service-profile repository: every stored
     * profile registers a parser under the profile's name (THE parser
     * identity), dialing the named endpoint. This is how fleet parsers
     * (gRParse included) register with the coordinator — save a profile,
     * no coordinator restart, no hand-maintained target list.
     *
     * <p>Only {@code TRANSPORT_PLAINTEXT} endpoints are accepted for now:
     * the parser client dials plaintext, and silently downgrading a profile
     * that asked for TLS would be a lie. A TLS endpoint is rejected loudly
     * at construction.
     *
     * @param profiles the profile store (the same store the rest of the
     *        toolkit reads)
     * @param endpointName the endpoint each profile must name (e.g.
     *        {@code "local"})
     * @return the registry; channels open lazily on first use
     */
    public static ParserRegistry fromProfiles(
            ai.protomolt.proto.grpc.profile.ServiceProfileRepository profiles,
            String endpointName) {
        if (profiles == null) {
            throw new IllegalArgumentException("profiles must not be null");
        }
        if (endpointName == null || endpointName.isBlank()) {
            throw new IllegalArgumentException("endpointName must not be blank");
        }
        Map<String, String> targets = new java.util.LinkedHashMap<>();
        java.util.List<ai.protomolt.proto.grpc.profile.v1.ServiceProfile> all;
        try {
            all = profiles.list();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("could not list service profiles", e);
        }
        for (ai.protomolt.proto.grpc.profile.v1.ServiceProfile profile : all) {
            ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint endpoint =
                    profile.getEndpointsList().stream()
                            .filter(e -> endpointName.equals(e.getName()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "service profile '" + profile.getName()
                                            + "' has no endpoint named '" + endpointName + "'"));
            if (endpoint.getTransport()
                    != ai.protomolt.proto.grpc.profile.v1.Transport.TRANSPORT_PLAINTEXT) {
                throw new IllegalArgumentException(
                        "service profile '" + profile.getName() + "' endpoint '" + endpointName
                                + "' is not plaintext; the parser client cannot honor it");
            }
            targets.put(profile.getName(), endpoint.getHost() + ":" + endpoint.getPort());
        }
        return new ParserRegistry(targets);
    }

    /**
     * The client for one parser, opening its channel on first use.
     *
     * @param parserName the parser identity from the plan
     * @return the cached client, or empty when the parser is not registered
     */
    public Optional<ParserClient> lookup(String parserName) {
        String target = targets.get(parserName);
        if (target == null) {
            return Optional.empty();
        }
        return Optional.of(clients.computeIfAbsent(parserName,
                name -> new ParserClient(openChannel(name, target))));
    }

    /** The registered parser names. */
    public java.util.Set<String> parserNames() {
        return targets.keySet();
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.values().forEach(channel -> {
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
        clients.clear();
    }

    private ManagedChannel openChannel(String name, String target) {
        ManagedChannel channel;
        if (target.startsWith(ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX)) {
            String serverName =
                    target.substring(ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX.length());
            channel = InProcessChannelBuilder.forName(serverName).build();
        } else {
            channel = NettyChannelBuilder.forTarget(target).usePlaintext().build();
        }
        channels.put(name, channel);
        return channel;
    }
}
