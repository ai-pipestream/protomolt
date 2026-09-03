package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.authz.grpc.ScopeServerInterceptor;
import ai.protomolt.proto.grpc.validate.ValidatingServerInterceptor;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * One factory wires the metric service: the MetricService over its served
 * subjects, mounted with the validating server interceptor from day one, so
 * the request protos' validate.v1 rules are what enforces the shape rules
 * and no handler re-implements a range check. A sibling of the search service,
 * never a second RPC on it: retrieval and aggregation stay independently
 * mountable.
 */
public final class MetricServices implements AutoCloseable {

    private final Map<String, ServedMetricSubject> subjects;
    private final MetricGrpcService service;
    private Server server;

    private MetricServices(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
            java.util.function.Supplier<ai.protomolt.proto.authz.AccessPolicy> accessPolicy) {
        this.subjects = Map.copyOf(subjects);
        this.service = new MetricGrpcService(this.subjects, rollups, resolver, accessPolicy);
    }

    /**
     * Builds the stack without a rollup sink: RebuildRollup refuses with
     * {@code missing-sink}.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @return the wired, not-yet-started stack
     */
    public static MetricServices build(Map<String, ServedMetricSubject> subjects) {
        return build(subjects, null);
    }

    /**
     * Builds the stack.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @param rollups where rebuilt rollups land, or {@code null} for none
     * @return the wired, not-yet-started stack
     */
    public static MetricServices build(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.RollupSink rollups) {
        return build(subjects, rollups, null);
    }

    /**
     * Builds the stack.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @param rollups where rebuilt rollups land, or {@code null} for none
     * @param resolver resolves subjects beyond the static set (rollup
     *        tables), or {@code null} for none
     * @return the wired, not-yet-started stack
     */
    public static MetricServices build(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver) {
        return build(subjects, rollups, resolver, null);
    }

    /**
     * Builds the stack with metric access rules: a caller whose access-policy principal
     * carries {@code metric_access} sees denied members dropped from descriptions,
     * refused in queries by name, and its row filters ANDed into every reduction. The
     * supplier is read per request, so a policy swapped on the config lane re-scopes
     * the rewrite with no restart; null (the supplier or its answer) rewrites nothing.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @param rollups where rebuilt rollups land, or {@code null} for none
     * @param resolver resolves subjects beyond the static set, or {@code null} for none
     * @param accessPolicy the live access policy, or {@code null} for no rewrite
     * @return the wired, not-yet-started stack
     */
    public static MetricServices build(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
            java.util.function.Supplier<ai.protomolt.proto.authz.AccessPolicy> accessPolicy) {
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "at least one served metric subject is required");
        }
        return new MetricServices(subjects, rollups, resolver, accessPolicy);
    }

    /** The served subjects, keyed by name. */
    public Map<String, ServedMetricSubject> subjects() {
        return subjects;
    }

    /**
     * Starts the service on an in-process server named {@code name}.
     *
     * @param name the in-process server name
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startInProcess(String name) throws IOException {
        return startInProcess(name, null, null);
    }

    /**
     * Starts in-process with service identity: the operator token holds every scope, a
     * credential the resolver names is scope-checked — describing and querying need
     * {@code metrics-query}, rebuilding rollups needs {@code metrics-rebuild}. Without a
     * token the service stays the open, trusted-network surface it is today.
     *
     * <p>Spends on its own ledger, for a surface that is the node's only enforcement
     * point. A node that also serves another enforcement point passes one shared ledger
     * through the overload that takes {@link ScopeBudgets}, or a principal gets a
     * separate allowance per surface.
     */
    public Server startInProcess(String name, String apiToken, CallerResolver resolver)
            throws IOException {
        return startInProcess(name, apiToken, resolver, new ScopeBudgets());
    }

    /**
     * Starts in-process with service identity, spending on the node's ledger; see
     * {@link #startNetty(int, String, CallerResolver, ScopeBudgets)}.
     */
    public Server startInProcess(String name, String apiToken, CallerResolver resolver,
            ScopeBudgets budgets) throws IOException {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver, budgets);
        server = builder.addService(service)
                .build()
                .start();
        return server;
    }

    /**
     * Starts the service on a Netty server with health and reflection.
     *
     * @param port the port to bind; {@code 0} picks a free port
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startNetty(int port) throws IOException {
        return startNetty(port, null, null);
    }

    /** Starts on Netty with service identity; see
     * {@link #startInProcess(String, String, CallerResolver)}. */
    public Server startNetty(int port, String apiToken, CallerResolver resolver)
            throws IOException {
        return startNetty(port, apiToken, resolver, new ScopeBudgets());
    }

    /**
     * Starts on Netty with service identity, spending on {@code budgets}: a node that also
     * mounts the metric actions passes the ledger it wired into the action catalog, so a
     * principal's metric budget is one allowance across both transports.
     */
    public Server startNetty(int port, String apiToken, CallerResolver resolver,
            ScopeBudgets budgets) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver, budgets);
        server = builder.addService(service)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        return server;
    }

    /**
     * Installs the credential and scope interceptors: added after the validating one so the
     * credential check runs first on the wire, the scope table second, validation third.
     */
    private void identity(Consumer<ServerInterceptor> intercept, String apiToken,
                          CallerResolver resolver, ScopeBudgets budgets) {
        if (apiToken == null) {
            if (resolver != null) {
                throw new IllegalArgumentException(
                        "an access-policy resolver requires the operator api token");
            }
            return;
        }
        String serviceName = service.bindService().getServiceDescriptor().getName();
        intercept.accept(new ScopeServerInterceptor(
                Map.of(serviceName, Scopes.METRICS_QUERY),
                Map.of(serviceName + "/RebuildRollup", Scopes.METRICS_REBUILD),
                Set.of("grpc.health.v1.Health",
                        "grpc.reflection.v1.ServerReflection"),
                budgets));
        intercept.accept(new ApiTokenServerInterceptor(apiToken, resolver));
    }

    /** The bound server, once one of the start methods has run. */
    public Server server() {
        return server;
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
