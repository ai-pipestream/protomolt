package ai.pipestream.proto.composer;

import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Boots a protomolt node from a role list. Modules come from
 * {@link ServiceLoader} (or are injected for tests), are topologically
 * ordered by {@link ServiceModule#requires()}, wired in order, and started
 * in order; shutdown closes everything in reverse. See
 * {@code DESIGN-service-modules.md} for the architecture.
 *
 * <p>The composer carries no transport: remote channels are opened through
 * the injected {@linkplain Builder#remoteOpener(Function) opener} under the
 * validated {@link OutboundChannelPolicy}, so the hosting app decides the
 * wire (Netty in production, in-process in tests).
 */
public final class Composer {

    private static final Logger LOG = LoggerFactory.getLogger(Composer.class);

    /** The environment variable selecting this node's roles. */
    public static final String ENV_ROLES = "PROTOMOLT_ROLES";

    private final Map<String, ServiceModule> modules;
    private final Map<String, String> environment;
    private final Function<String, ManagedChannel> remoteOpener;
    private final OutboundChannelPolicy policy;

    private Composer(Builder builder) {
        this.modules = Map.copyOf(builder.modules);
        this.environment = Map.copyOf(builder.environment);
        this.remoteOpener = builder.remoteOpener;
        this.policy = builder.policy;
    }

    /** Starts a builder over the modules found by {@link ServiceLoader}. */
    public static Builder builder() {
        Builder builder = new Builder();
        for (ServiceModule module : ServiceLoader.load(ServiceModule.class)) {
            builder.module(module);
        }
        return builder;
    }

    /** Starts a builder with no discovered modules (tests inject their own). */
    public static Builder emptyBuilder() {
        return new Builder();
    }

    /**
     * Boots the roles named in {@link #ENV_ROLES} (comma-separated).
     *
     * @return the running node
     * @throws ComposerException when the variable is missing or blank
     */
    public Node bootFromEnvironment() {
        String roles = environment.get(ENV_ROLES);
        if (roles == null || roles.isBlank()) {
            throw new ComposerException(ENV_ROLES + " is required: a comma-separated role list, known roles: "
                    + knownRoles());
        }
        List<String> requested = new ArrayList<>();
        for (String role : roles.split(",")) {
            if (!role.isBlank()) {
                requested.add(role.trim().toLowerCase(Locale.ROOT));
            }
        }
        return boot(requested);
    }

    /**
     * Boots the given roles: resolves modules, orders by requirements,
     * wires, then starts. On any failure everything already created is
     * closed in reverse before the failure propagates.
     *
     * @param roles the roles to mount, in requested order
     * @return the running node
     */
    public Node boot(List<String> roles) {
        List<ServiceModule> ordered = order(roles);
        DefaultContext context = new DefaultContext();
        Deque<AutoCloseable> closeStack = context.closeStack;
        List<ServiceMount> mounts = new ArrayList<>();
        try {
            for (ServiceModule module : ordered) {
                LOG.info("wiring role {}", module.role());
                ServiceMount mount = module.wire(context);
                if (mount == null) {
                    throw new ComposerException("role " + module.role() + " wired to a null mount");
                }
                mounts.add(mount);
                closeStack.push(mount);
            }
            context.wired = true;
            for (int i = 0; i < mounts.size(); i++) {
                LOG.info("starting role {}", ordered.get(i).role());
                mounts.get(i).start();
            }
        } catch (Exception e) {
            closeAll(closeStack);
            // Channels handed out during wiring are node-owned; with no Node
            // returned, the failed boot must close them itself.
            context.channels.close();
            if (e instanceof ComposerException composer) {
                throw composer;
            }
            throw new ComposerException("node boot failed: " + e.getMessage(), e);
        }
        return new Node(context);
    }

    /** The roles this composer can mount, sorted. */
    public Set<String> knownRoles() {
        return new TreeSet<>(modules.keySet());
    }

    private List<ServiceModule> order(List<String> roles) {
        Set<String> selected = new LinkedHashSet<>(roles);
        List<ServiceModule> ordered = new ArrayList<>();
        Set<String> placed = new LinkedHashSet<>();
        Set<String> visiting = new LinkedHashSet<>();
        for (String role : selected) {
            place(role, selected, placed, visiting, ordered);
        }
        return ordered;
    }

    private void place(String role, Set<String> selected, Set<String> placed,
                       Set<String> visiting, List<ServiceModule> ordered) {
        if (placed.contains(role)) {
            return;
        }
        ServiceModule module = modules.get(role);
        if (module == null) {
            throw new ComposerException("unknown role " + role + ", known roles: " + knownRoles());
        }
        if (!visiting.add(role)) {
            throw new ComposerException("role dependency cycle through " + visiting);
        }
        for (String required : module.requires()) {
            // A required role outside the selection is reached remotely via
            // Channels; only co-mounted requirements force wire order.
            if (selected.contains(required)) {
                place(required, selected, placed, visiting, ordered);
            }
        }
        visiting.remove(role);
        placed.add(role);
        ordered.add(module);
    }

    private static void closeAll(Deque<AutoCloseable> closeStack) {
        while (!closeStack.isEmpty()) {
            AutoCloseable resource = closeStack.pop();
            try {
                resource.close();
            } catch (Exception e) {
                LOG.warn("shutdown resource failed to close", e);
            }
        }
    }

    /** A running node; closing it unwinds every mount in reverse order. */
    public static final class Node implements AutoCloseable {

        private final DefaultContext context;

        private Node(DefaultContext context) {
            this.context = context;
        }

        /** The context the node's modules wired against. */
        public NodeContext context() {
            return context;
        }

        @Override
        public void close() {
            closeAll(context.closeStack);
            context.channels.close();
        }
    }

    /** Builds a composer; modules, environment, and transport are injected. */
    public static final class Builder {

        private final Map<String, ServiceModule> modules = new LinkedHashMap<>();
        private Map<String, String> environment = System.getenv();
        private Function<String, ManagedChannel> remoteOpener;
        private OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();

        private Builder() {
        }

        /** Adds one module; a duplicate role is a configuration error. */
        public Builder module(ServiceModule module) {
            ServiceModule previous = modules.putIfAbsent(module.role(), module);
            if (previous != null) {
                throw new ComposerException("two modules claim role " + module.role() + ": "
                        + previous.getClass().getName() + " and " + module.getClass().getName());
            }
            return this;
        }

        /** Replaces the environment (tests inject a map). */
        public Builder environment(Map<String, String> environment) {
            this.environment = environment;
            return this;
        }

        /**
         * The transport for remote role targets: receives the validated
         * canonical target, returns an open channel. Without one, remote
         * resolution fails loudly and only co-mounted roles resolve.
         */
        public Builder remoteOpener(Function<String, ManagedChannel> opener) {
            this.remoteOpener = opener;
            return this;
        }

        /** Replaces the outbound channel policy for remote targets. */
        public Builder policy(OutboundChannelPolicy policy) {
            this.policy = policy;
            return this;
        }

        /** Builds the composer. */
        public Composer build() {
            return new Composer(this);
        }
    }

    private final class DefaultContext implements NodeContext {

        private final String nodeId = UUID.randomUUID().toString().substring(0, 8);
        private final DefaultChannels channels = new DefaultChannels();
        private final DefaultContributions contributions = new DefaultContributions();
        private final Deque<AutoCloseable> closeStack = new ArrayDeque<>();
        private volatile boolean wired;

        @Override
        public Map<String, String> environment() {
            return environment;
        }

        @Override
        public String nodeId() {
            return nodeId;
        }

        @Override
        public Channels channels() {
            return channels;
        }

        @Override
        public Contributions contributions() {
            return contributions;
        }

        @Override
        public void onClose(AutoCloseable resource) {
            if (resource == null) {
                throw new IllegalArgumentException("resource must not be null");
            }
            closeStack.push(resource);
        }

        private final class DefaultChannels implements Channels {

            private final Map<String, String> inProcess = new HashMap<>();
            private final Map<String, ManagedChannel> open = new HashMap<>();

            @Override
            public synchronized void publishInProcess(String role, String inProcessName) {
                String previous = inProcess.putIfAbsent(role, inProcessName);
                if (previous != null) {
                    throw new ComposerException("role " + role + " already published endpoint " + previous);
                }
            }

            @Override
            public synchronized ManagedChannel to(String role) {
                ManagedChannel existing = open.get(role);
                if (existing != null) {
                    return existing;
                }
                ManagedChannel channel = openFor(role);
                open.put(role, channel);
                return channel;
            }

            @Override
            public synchronized boolean isLocal(String role) {
                return inProcess.containsKey(role);
            }

            @Override
            public synchronized String targetOf(String role) {
                String local = inProcess.get(role);
                if (local != null) {
                    return Channels.IN_PROCESS_PREFIX + local;
                }
                String target = remoteTarget(role);
                policy.validateTarget(target, false);
                return target;
            }

            private ManagedChannel openFor(String role) {
                String local = inProcess.get(role);
                if (local != null) {
                    return InProcessChannelBuilder.forName(local).build();
                }
                String target = remoteTarget(role);
                if (remoteOpener == null) {
                    throw new ComposerException("role " + role + " resolves to remote target " + target
                            + " but this node has no remote transport configured");
                }
                return policy.open(target, false, remoteOpener);
            }

            /**
             * The configured remote target for a role: the canonical
             * variable, then the variables of the role's aliases. The
             * refusal names the canonical variable, the one to set.
             */
            private String remoteTarget(String role) {
                for (String variable : Channels.targetVariables(role)) {
                    String target = environment.get(variable);
                    if (target != null && !target.isBlank()) {
                        return target;
                    }
                }
                throw new ComposerException("role " + role
                        + " is not mounted on this node and "
                        + Channels.targetVariable(role) + " is not set");
            }

            private synchronized void close() {
                for (ManagedChannel channel : open.values()) {
                    channel.shutdownNow();
                    try {
                        channel.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                open.clear();
            }
        }

        private final class DefaultContributions implements Contributions {

            private final Map<Class<?>, List<Object>> byKind = new LinkedHashMap<>();

            @Override
            public synchronized <T> void contribute(Class<T> kind, T contribution) {
                if (wired) {
                    throw new ComposerException("contribution of " + kind.getSimpleName()
                            + " after the wire phase completed");
                }
                if (contribution == null) {
                    throw new IllegalArgumentException("contribution must not be null");
                }
                byKind.computeIfAbsent(kind, key -> new ArrayList<>()).add(contribution);
            }

            @Override
            public synchronized <T> List<T> all(Class<T> kind) {
                List<Object> contributions = byKind.getOrDefault(kind, List.of());
                List<T> typed = new ArrayList<>(contributions.size());
                for (Object contribution : contributions) {
                    typed.add(kind.cast(contribution));
                }
                return List.copyOf(typed);
            }
        }
    }
}
