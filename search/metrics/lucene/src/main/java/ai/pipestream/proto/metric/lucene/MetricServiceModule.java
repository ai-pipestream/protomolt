package ai.pipestream.proto.metric.lucene;

import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.service.MetricActions;
import ai.pipestream.proto.metric.service.MetricServices;
import ai.pipestream.proto.metric.service.MetricWorkflows;
import ai.pipestream.proto.metric.service.ServedMetricSubject;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.search.service.LuceneSearchStore;
import io.grpc.Server;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The metric service as a mountable role, answering over the search role's
 * live Lucene index. Wiring borrows the store the co-mounted search role
 * contributed, builds one Lucene executor over it, publishes the service's
 * in-process endpoint, and contributes the describe-mapping and
 * query-metrics verbs to the action catalog (served by a co-mounted
 * registry). Starting binds the external Netty port.
 *
 * <p>The store is shared in process, never over a wire: a node mounting
 * this role without the search role refuses to wire. The remote metrics
 * node is this role beside a READ-ONLY search role whose index restores
 * from the writer's snapshot, so it needs no repo and writes nothing.
 */
public final class MetricServiceModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "metrics";

    /**
     * One served metric subject: the metric mapping the service answers for
     * and the index mapping the executor reads field shapes from. The
     * subject key must name a mapping subject the search role serves —
     * the executor's reads borrow that subject's searcher.
     *
     * <p>Extra engines (the Iceberg backend over a lake table, built by
     * the host that owns the catalog) mount beside the module's own
     * Lucene executor. Per the design, a subject serving more than one
     * engine refuses a query that leaves the backend unset, naming the
     * mounted set.</p>
     *
     * @param metricMapping the compiled metric member declarations
     * @param indexMapping the search index's field shapes for the subject
     * @param extraExecutors host-built engines by backend; never the
     *        Lucene backend, which this module builds itself
     */
    public record Subject(MetricMapping metricMapping, IndexMapping indexMapping,
            Map<MetricBackend, MetricExecutor> extraExecutors) {

        /** Validates the subject. */
        public Subject {
            if (metricMapping == null) {
                throw new IllegalArgumentException("metricMapping must not be null");
            }
            if (indexMapping == null) {
                throw new IllegalArgumentException("indexMapping must not be null");
            }
            extraExecutors = extraExecutors == null ? Map.of() : Map.copyOf(extraExecutors);
            for (Map.Entry<MetricBackend, MetricExecutor> entry : extraExecutors.entrySet()) {
                if (entry.getKey() == MetricBackend.METRIC_BACKEND_LUCENE
                        || entry.getKey() == MetricBackend.METRIC_BACKEND_UNSPECIFIED) {
                    throw new IllegalArgumentException("extra executors mount under their"
                            + " own named backend; the metrics role builds the Lucene"
                            + " executor itself over the search role's store");
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                            "extra executor for " + entry.getKey() + " must not be null");
                }
                if (entry.getValue().backend() != entry.getKey()) {
                    throw new IllegalArgumentException("extra executor mounted under "
                            + entry.getKey() + " reports backend "
                            + entry.getValue().backend() + "; the key and the executor"
                            + " must agree");
                }
            }
        }

        /** A subject served by the Lucene engine alone. */
        public Subject(MetricMapping metricMapping, IndexMapping indexMapping) {
            this(metricMapping, indexMapping, Map.of());
        }
    }

    /**
     * Module configuration.
     *
     * @param grpcPort the external port (0 for ephemeral)
     * @param subjects the metric subjects to serve, keyed by the search
     *        mapping subject they aggregate over
     * @param rollupSink where rebuilt rollups land, or {@code null} for
     *        none (RebuildRollup refuses with {@code missing-sink})
     * @param subjectResolver resolves subjects beyond the static set
     *        (rollup tables), or {@code null} for none
     */
    public record Config(int grpcPort, Map<String, Subject> subjects,
            ai.pipestream.proto.metric.spi.RollupSink rollupSink,
            ai.pipestream.proto.metric.spi.MetricSubjectResolver subjectResolver) {

        /** Validates the configuration. */
        public Config {
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException(
                        "at least one served metric subject is required");
            }
            subjects = Map.copyOf(subjects);
        }

        /** A configuration without a subject resolver. */
        public Config(int grpcPort, Map<String, Subject> subjects,
                ai.pipestream.proto.metric.spi.RollupSink rollupSink) {
            this(grpcPort, subjects, rollupSink, null);
        }

        /** A configuration without a rollup sink. */
        public Config(int grpcPort, Map<String, Subject> subjects) {
            this(grpcPort, subjects, null, null);
        }
    }

    private final Config config;
    private MetricServices service;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public MetricServiceModule(Config config) {
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
        // The search role must wire first so its store contribution exists
        // by this wire, and a co-mounted registry must wire first so the
        // rebuild-rollup workflow can register; the composer's ordering is
        // the only ordering, and absent roles are simply not required.
        return Set.of("search", "registry");
    }

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        List<LuceneSearchStore> stores =
                context.contributions().all(LuceneSearchStore.class);
        if (stores.isEmpty()) {
            throw new IllegalStateException("the metrics role reads the search index in"
                    + " process: mount the 'search' role on this node (a remote search"
                    + " target cannot share its store)");
        }
        LuceneSearchStore store = stores.getFirst();
        for (String subject : config.subjects().keySet()) {
            if (!store.subjectNames().contains(subject)) {
                throw new IllegalStateException("metric subject '" + subject
                        + "' names no served search mapping subject; the search role"
                        + " serves: " + new TreeSet<>(store.subjectNames()));
            }
        }

        LuceneMetricExecutor executor = new LuceneMetricExecutor(
                new LuceneMetricExecutor.SubjectReader() {
                    @Override
                    public IndexMapping mapping(String subject) {
                        Subject served = config.subjects().get(subject);
                        if (served == null) {
                            throw new IllegalArgumentException("unknown metric subject '"
                                    + subject + "'; served subjects: "
                                    + new TreeSet<>(config.subjects().keySet()));
                        }
                        return served.indexMapping();
                    }

                    @Override
                    public MetricExecutor.Result read(
                            String subject, LuceneMetricExecutor.Aggregation aggregation) {
                        return store.withSearcher(subject, aggregation::run);
                    }
                });
        Map<String, ServedMetricSubject> served = new LinkedHashMap<>();
        config.subjects().forEach((subject, spec) -> {
            Map<MetricBackend, MetricExecutor> engines =
                    new LinkedHashMap<>(spec.extraExecutors());
            engines.put(MetricBackend.METRIC_BACKEND_LUCENE, executor);
            served.put(subject, new ServedMetricSubject(spec.metricMapping(), engines));
        });
        service = MetricServices.build(
                served, config.rollupSink(), config.subjectResolver());
        String name = ROLE + "-" + context.nodeId();
        inProcess = service.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);

        for (ProtoAction action : MetricActions.over(
                served, config.rollupSink(), config.subjectResolver())) {
            context.contributions().contribute(ProtoAction.class, action);
        }

        // With a co-mounted registry, register the rebuild-rollup workflow
        // so operators can submit it by name; without one the RPC still
        // answers, there is just no declared envelope to submit.
        List<ai.pipestream.proto.registry.GitSchemaRegistryStore> registries =
                context.contributions().all(
                        ai.pipestream.proto.registry.GitSchemaRegistryStore.class);
        if (!registries.isEmpty()) {
            registries.getFirst().putWorkflow(
                    MetricWorkflows.REBUILD_ROLLUP_WORKFLOW,
                    MetricWorkflows.rebuildRollupWorkflow(
                            context.channels().targetOf(ROLE), 60_000).toString());
        }
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                netty = service.startNetty(config.grpcPort());
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

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("metrics module has not started");
        }
        return netty.getPort();
    }
}
