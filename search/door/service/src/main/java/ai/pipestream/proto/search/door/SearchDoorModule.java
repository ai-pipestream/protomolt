package ai.pipestream.proto.search.door;

import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import io.grpc.Server;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The search door as a mountable role. Wiring opens the document fetcher
 * over the repo role's channel, builds the Lucene store (resolving every
 * chunk lane's embedding provider — a subject naming an absent model fails
 * the mount), and publishes the door's in-process endpoint. Starting binds
 * the external Netty port.
 */
public final class SearchDoorModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "search";

    /**
     * Module configuration.
     *
     * @param grpcPort the external port (0 for ephemeral)
     * @param indexDir the root index directory
     * @param subjects the mapping subjects to serve, keyed by subject name
     */
    public record Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {

        /** Validates the configuration. */
        public Config {
            if (indexDir == null) {
                throw new IllegalArgumentException("indexDir must not be null");
            }
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException(
                        "at least one served mapping subject is required");
            }
            subjects = Map.copyOf(subjects);
        }
    }

    private final Config config;
    private SearchDoorServices door;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public SearchDoorModule(Config config) {
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
        // parse, registry, and jobs are optional at runtime (workflow and
        // replay registration are skipped when absent) but must wire first
        // when co-mounted, so their contributions exist by this wire.
        return Set.of("repo", "parse", "registry", "jobs");
    }

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        GrpcDocumentFetcher repo = new GrpcDocumentFetcher(context.channels().targetOf("repo"));
        try {
            door = SearchDoorServices.build(
                    new SearchDoorConfig(config.grpcPort(), config.indexDir(), config.subjects()),
                    repo);
        } catch (RuntimeException e) {
            // A failed mount (for instance a subject naming an absent
            // embedding provider) must not leak the repo channel.
            repo.close();
            throw e;
        }
        String name = ROLE + "-" + context.nodeId();
        inProcess = door.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);

        // With a co-mounted registry, register the door's workflows so
        // operators can submit them by name. Parse-and-index additionally
        // needs a co-mounted parse coordinator; delete-and-unindex rides
        // the repo channel the door already requires. A standalone search
        // node skips both: the workflows span roles it does not have.
        List<GitSchemaRegistryStore> stores =
                context.contributions().all(GitSchemaRegistryStore.class);
        if (!stores.isEmpty()) {
            if (context.channels().isLocal("parse")) {
                stores.getFirst().putWorkflow(
                        SearchWorkflows.PARSE_AND_INDEX_WORKFLOW,
                        SearchWorkflows.parseAndIndexWorkflow(
                                        context.channels().targetOf("parse"),
                                        context.channels().targetOf(ROLE),
                                        60_000)
                                .toString());
            }
            stores.getFirst().putWorkflow(
                    SearchWorkflows.DELETE_AND_UNINDEX_WORKFLOW,
                    SearchWorkflows.deleteAndUnindexWorkflow(
                                    context.channels().targetOf("repo"),
                                    context.channels().targetOf(ROLE),
                                    60_000)
                            .toString());
        }

        // With a co-mounted jobs module, replay rides its submit action: the
        // replay-documents action re-runs a stored workflow over a listing.
        context.contributions().all(ProtoAction.class).stream()
                .filter(action -> action.name().equals("submit-workflow"))
                .findFirst()
                .ifPresent(submit -> context.contributions().contribute(
                        ProtoAction.class, new ReplayAction(repo, submit, door.store())));
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = door.startNetty(config.grpcPort());
            }

            @Override
            public void close() {
                if (inProcess != null) {
                    inProcess.shutdownNow();
                }
                door.close();
            }
        };
    }

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("search module has not started");
        }
        return netty.getPort();
    }
}
