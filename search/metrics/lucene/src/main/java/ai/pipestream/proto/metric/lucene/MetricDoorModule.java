package ai.pipestream.proto.metric.lucene;

import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.door.MetricActions;
import ai.pipestream.proto.metric.door.MetricDoorServices;
import ai.pipestream.proto.metric.door.ServedMetricSubject;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.search.door.LuceneSearchStore;
import io.grpc.Server;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The metric door as a mountable role, answering over the search role's
 * live Lucene index. Wiring borrows the store the co-mounted search role
 * contributed, builds one Lucene executor over it, publishes the door's
 * in-process endpoint, and contributes the describe-mapping and
 * query-metrics verbs to the action catalog (served by a co-mounted
 * registry). Starting binds the external Netty port.
 *
 * <p>The store is shared in process, never over a wire: a node mounting
 * this role without the search role refuses to wire. A remote metrics
 * node is a different composition (an index restored from a snapshot) and
 * does not exist yet.
 */
public final class MetricDoorModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "metrics";

    /**
     * One served metric subject: the metric mapping the door answers for
     * and the index mapping the executor reads field shapes from. The
     * subject key must name a mapping subject the search role serves —
     * the executor's reads borrow that subject's searcher.
     *
     * @param metricMapping the compiled metric member declarations
     * @param indexMapping the search index's field shapes for the subject
     */
    public record Subject(MetricMapping metricMapping, IndexMapping indexMapping) {

        /** Validates the subject. */
        public Subject {
            if (metricMapping == null) {
                throw new IllegalArgumentException("metricMapping must not be null");
            }
            if (indexMapping == null) {
                throw new IllegalArgumentException("indexMapping must not be null");
            }
        }
    }

    /**
     * Module configuration.
     *
     * @param grpcPort the external port (0 for ephemeral)
     * @param subjects the metric subjects to serve, keyed by the search
     *        mapping subject they aggregate over
     */
    public record Config(int grpcPort, Map<String, Subject> subjects) {

        /** Validates the configuration. */
        public Config {
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException(
                        "at least one served metric subject is required");
            }
            subjects = Map.copyOf(subjects);
        }
    }

    private final Config config;
    private MetricDoorServices door;
    private Server inProcess;
    private Server netty;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public MetricDoorModule(Config config) {
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
        // by this wire; the composer's ordering is the only ordering.
        return Set.of("search");
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
        config.subjects().forEach((subject, spec) -> served.put(subject,
                new ServedMetricSubject(
                        spec.metricMapping(),
                        Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor))));
        door = MetricDoorServices.build(served);
        String name = ROLE + "-" + context.nodeId();
        inProcess = door.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);

        for (ProtoAction action : MetricActions.over(served)) {
            context.contributions().contribute(ProtoAction.class, action);
        }
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
            throw new IllegalStateException("metrics module has not started");
        }
        return netty.getPort();
    }
}
