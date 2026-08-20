package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import com.google.protobuf.Descriptors;
import io.grpc.Server;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The parsing coordinator as a mountable role. Wiring builds the parser
 * registry (service profiles, or parser roles resolved through the node's
 * channels), publishes the coordinator's in-process endpoint, registers the
 * {@code parse-document} workflow with a co-mounted registry, and
 * contributes the fleet document descriptor so workflow-run checkpoints
 * transcode. Starting binds the external Netty port.
 */
public final class ParseModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "parse";

    /**
     * Module configuration.
     *
     * @param rules the routing rules
     * @param parserRolesByName parser name to co-mounted parser role
     *        (resolved through channels); empty when profiles are used
     * @param profilesDir the service-profile directory, or null
     * @param profileEndpoint the profile endpoint name, or null
     * @param deadlineSeconds the per-parse deadline
     * @param grpcPort the external port (0 for ephemeral)
     */
    public record Config(RoutingRules rules, Map<String, String> parserRolesByName,
                         Path profilesDir, String profileEndpoint,
                         long deadlineSeconds, int grpcPort) {

        /** Validates the configuration. */
        public Config {
            if (rules == null) {
                throw new IllegalArgumentException("rules must not be null");
            }
            parserRolesByName = parserRolesByName == null
                    ? Map.of() : Map.copyOf(parserRolesByName);
            if (profilesDir == null && parserRolesByName.isEmpty()) {
                throw new IllegalArgumentException(
                        "either profilesDir or parserRolesByName is required");
            }
            if ((profilesDir == null) != (profileEndpoint == null)) {
                throw new IllegalArgumentException(
                        "profilesDir and profileEndpoint come together");
            }
        }
    }

    private final Config config;
    private final java.util.concurrent.atomic.AtomicReference<RoutingRules> liveRules =
            new java.util.concurrent.atomic.AtomicReference<>();
    private ParseCoordinatorServices coordinator;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public ParseModule(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public Set<String> requires() {
        // registry is optional at runtime (workflow registration is skipped
        // when absent) but must wire first when co-mounted.
        return Set.of("repo", "registry");
    }

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        ParserRegistry parsers;
        if (config.profilesDir() != null) {
            parsers = ParserRegistry.fromProfiles(
                    new FileSystemServiceProfileRepository(config.profilesDir()),
                    config.profileEndpoint());
        } else {
            Map<String, String> targets = new LinkedHashMap<>();
            for (Map.Entry<String, String> parser : config.parserRolesByName().entrySet()) {
                targets.put(parser.getKey(), context.channels().targetOf(parser.getValue()));
            }
            parsers = ParserRegistry.of(targets);
        }
        liveRules.set(config.rules());
        coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0,
                        context.channels().targetOf("repo"),
                        "intake",
                        config.deadlineSeconds()),
                liveRules::get,
                parsers);
        String name = ROLE + "-" + context.nodeId();
        inProcess = coordinator.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);

        List<GitSchemaRegistryStore> stores =
                context.contributions().all(GitSchemaRegistryStore.class);
        if (!stores.isEmpty()) {
            stores.getFirst().putWorkflow(
                    ParseWorkflows.PARSE_DOCUMENT_WORKFLOW,
                    ParseWorkflows.parseDocumentWorkflow(
                                    context.channels().targetOf(ROLE),
                                    config.deadlineSeconds() * 1000)
                            .toString());
        }
        context.contributions().contribute(Descriptors.FileDescriptor.class,
                ai.pipestream.proto.parse.document.v1.DocumentProto.getDescriptor());
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = coordinator.startNetty(config.grpcPort());
            }

            @Override
            public void close() throws Exception {
                if (inProcess != null) {
                    inProcess.shutdownNow();
                }
                coordinator.close();
            }
        };
    }

    /**
     * Swaps the live routing rules: every later plan uses the new set.
     * This is distributed config's seam — the "config reload" the routing
     * contract always promised, with no CRUD surface.
     *
     * @param rules the new compiled rule set
     */
    public void swapRules(RoutingRules rules) {
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        liveRules.set(rules);
    }

    /** The currently live routing rules. */
    public RoutingRules currentRules() {
        return liveRules.get();
    }

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("parse module has not started");
        }
        return netty.getPort();
    }
}
