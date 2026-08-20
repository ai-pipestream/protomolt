package ai.pipestream.proto.search.service;

import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.authz.CallerResolver;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.screening.Screener;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.spi.PostalCodeCatalog;
import ai.pipestream.proto.validate.spi.TaxonomyCatalog;
import ai.pipestream.proto.validate.spi.ValidationRuleSources;
import io.grpc.Server;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The search service as a mountable role. Wiring opens the document fetcher
 * over the repo role's channel, builds the Lucene store (resolving every
 * chunk lane's embedding provider — a subject naming an absent model fails
 * the mount), and publishes the service's in-process endpoint. Starting binds
 * the external Netty port.
 */
public final class SearchServiceModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "search";

    /**
     * Module configuration.
     *
     * @param grpcPort the external port (0 for ephemeral)
     * @param indexDir the root index directory
     * @param subjects the mapping subjects to serve, keyed by subject name
     * @param snapshots commit-point snapshots of every subject's index, or
     *        {@code null} for none
     * @param readOnly a reader node: no repo channel, no indexing surface,
     *        no workflow registration, and restore-only snapshots
     * @param refreshSeconds how often a reader pulls newer snapshots into
     *        its live index; {@code 0} means restart-only, and a positive
     *        value demands a read-only node with snapshots
     */
    public record Config(
            int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
            IndexSnapshots snapshots, boolean readOnly, long refreshSeconds,
            TaxonomyCatalog taxonomies,
            Supplier<Screener> screening,
            PostalCodeCatalog postalCodes,
            String apiToken,
            CallerResolver callers) {

        /** Validates the configuration. */
        public Config {
            if (callers != null && apiToken == null) {
                throw new IllegalArgumentException(
                        "an access-policy resolver requires the operator api token");
            }
            if (indexDir == null) {
                throw new IllegalArgumentException("indexDir must not be null");
            }
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException(
                        "at least one served mapping subject is required");
            }
            if (readOnly && snapshots != null && !snapshots.readOnly()) {
                throw new IllegalArgumentException("a read-only node must not write"
                        + " snapshots: construct its IndexSnapshots read-only");
            }
            if (refreshSeconds < 0) {
                throw new IllegalArgumentException("refreshSeconds must not be negative");
            }
            if (refreshSeconds > 0 && !readOnly) {
                throw new IllegalArgumentException("refresh is the reader's pull: a"
                        + " writable node publishes snapshots on its commit cadence"
                        + " instead");
            }
            if (refreshSeconds > 0 && snapshots == null) {
                throw new IllegalArgumentException(
                        "refreshSeconds needs snapshots to refresh from");
            }
            subjects = Map.copyOf(subjects);
        }

        /** An open configuration: no operator token, no access policy. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots, boolean readOnly, long refreshSeconds,
                TaxonomyCatalog taxonomies,
                Supplier<Screener> screening,
                PostalCodeCatalog postalCodes) {
            this(grpcPort, indexDir, subjects, snapshots, readOnly, refreshSeconds,
                    taxonomies, screening, postalCodes, null, null);
        }

        /**
         * The same configuration served behind the operator token, with the
         * resolver's principals scope-checked.
         */
        public Config secured(String token, CallerResolver resolver) {
            return new Config(grpcPort, indexDir, subjects, snapshots, readOnly,
                    refreshSeconds, taxonomies, screening, postalCodes, token, resolver);
        }

        /** A configuration without a postal-code pack. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots, boolean readOnly, long refreshSeconds,
                TaxonomyCatalog taxonomies,
                Supplier<Screener> screening) {
            this(grpcPort, indexDir, subjects, snapshots, readOnly, refreshSeconds,
                    taxonomies, screening, null);
        }

        /** A configuration without a screening mount. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots, boolean readOnly, long refreshSeconds,
                TaxonomyCatalog taxonomies) {
            this(grpcPort, indexDir, subjects, snapshots, readOnly, refreshSeconds,
                    taxonomies, null, null);
        }

        /**
         * A configuration without a taxonomy catalog: no document gate, the
         * service's historical behavior.
         */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots, boolean readOnly, long refreshSeconds) {
            this(grpcPort, indexDir, subjects, snapshots, readOnly, refreshSeconds,
                    null, null, null);
        }

        /** A configuration without periodic refresh. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots, boolean readOnly) {
            this(grpcPort, indexDir, subjects, snapshots, readOnly, 0L, null, null, null);
        }

        /** A writable configuration. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects,
                IndexSnapshots snapshots) {
            this(grpcPort, indexDir, subjects, snapshots, false, 0L, null, null);
        }

        /** A writable configuration without snapshots. */
        public Config(int grpcPort, Path indexDir, Map<String, ServedMapping> subjects) {
            this(grpcPort, indexDir, subjects, null, false, 0L, null, null);
        }
    }

    private final Config config;
    private SearchServices service;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public SearchServiceModule(Config config) {
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
        // when co-mounted, so their contributions exist by this wire. A
        // read-only node requires nothing: it fetches no documents and
        // registers no workflows.
        return config.readOnly()
                ? Set.of()
                : Set.of("repo", "parse", "registry", "jobs");
    }

    /** Per-call deadline on every repo read the service makes. */
    private static final Duration REPO_RPC_TIMEOUT = Duration.ofSeconds(30);

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        if (config.readOnly()) {
            return wireReadOnly(context);
        }
        GrpcDocumentFetcher repo = new GrpcDocumentFetcher(
                context.channels().targetOf("repo"), REPO_RPC_TIMEOUT);
        try {
            service = SearchServices.build(
                    new SearchServiceConfig(config.grpcPort(), config.indexDir(),
                            config.subjects(), config.snapshots()),
                    repo,
                    // A configured catalog turns the document gate on: fetched
                    // documents validate over the live mounts before indexing.
                    config.taxonomies() == null && config.postalCodes() == null
                            ? null
                            : ProtoValidator.create(
                                    ValidationRuleSources.defaults(),
                                    config.taxonomies() == null
                                            ? TaxonomyCatalog.empty()
                                            : config.taxonomies(),
                                    config.postalCodes() == null
                                            ? PostalCodeCatalog.empty()
                                            : config.postalCodes()),
                    // A configured screening supplier turns the mask policy
                    // on: fetched documents screen over the live mount.
                    config.screening());
        } catch (RuntimeException e) {
            // A failed mount (for instance a subject naming an absent
            // embedding provider) must not leak the repo channel.
            repo.close();
            throw e;
        }
        String name = ROLE + "-" + context.nodeId();
        inProcess = service.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);

        // The live store, for co-mounted roles that read the same index in
        // process (the metrics role's executor borrows its searchers).
        context.contributions().contribute(LuceneSearchStore.class, service.store());

        // With a co-mounted registry, register the service's workflows so
        // operators can submit them by name. Parse-and-index additionally
        // needs a co-mounted parse coordinator; delete-and-unindex rides
        // the repo channel the service already requires. A standalone search
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
                        ProtoAction.class, new ReplayAction(repo, submit, service.store())));
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = config.apiToken() == null
                        ? service.startNetty(config.grpcPort())
                        : service.startNetty(config.grpcPort(),
                                config.apiToken(), config.callers());
            }

            @Override
            public void close() {
                if (inProcess != null) {
                    inProcess.shutdownNow();
                }
                service.close();
            }
        };
    }

    /**
     * A reader node: no repo channel, no workflow registration, no replay,
     * no write surface. The restored (or locally present) index serves the
     * query RPCs, and the store still contributes for a co-mounted metrics
     * role.
     */
    private ServiceMount wireReadOnly(NodeContext context) throws Exception {
        service = SearchServices.build(
                new SearchServiceConfig(config.grpcPort(), config.indexDir(),
                        config.subjects(), config.snapshots(), true,
                        config.refreshSeconds()),
                null);
        String name = ROLE + "-" + context.nodeId();
        inProcess = service.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);
        context.contributions().contribute(LuceneSearchStore.class, service.store());
        return new ServiceMount() {
            private java.util.concurrent.ScheduledExecutorService refresher;

            @Override
            public void start() throws Exception {
                netty = config.apiToken() == null
                        ? service.startNetty(config.grpcPort())
                        : service.startNetty(config.grpcPort(),
                                config.apiToken(), config.callers());
                if (config.refreshSeconds() > 0) {
                    refresher = java.util.concurrent.Executors
                            .newSingleThreadScheduledExecutor(runnable -> {
                                Thread thread =
                                        new Thread(runnable, "search-service-refresher");
                                thread.setDaemon(true);
                                return thread;
                            });
                    // The pull never uploads or prunes; a failed tick keeps
                    // the serving commit and the next tick retries.
                    refresher.scheduleWithFixedDelay(
                            () -> service.store().refreshFromSnapshots(),
                            config.refreshSeconds(), config.refreshSeconds(),
                            java.util.concurrent.TimeUnit.SECONDS);
                }
            }

            @Override
            public void close() {
                if (refresher != null) {
                    refresher.shutdownNow();
                }
                if (inProcess != null) {
                    inProcess.shutdownNow();
                }
                service.close();
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
