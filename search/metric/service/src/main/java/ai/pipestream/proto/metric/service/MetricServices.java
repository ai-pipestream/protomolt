package ai.pipestream.proto.metric.service;

import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import ai.pipestream.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.pipestream.proto.authz.grpc.ScopeServerInterceptor;
import ai.pipestream.proto.grpc.validate.ValidatingServerInterceptor;
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
            ai.pipestream.proto.metric.spi.RollupSink rollups,
            ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
        this.subjects = Map.copyOf(subjects);
        this.service = new MetricGrpcService(this.subjects, rollups, resolver);
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
            ai.pipestream.proto.metric.spi.RollupSink rollups) {
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
            ai.pipestream.proto.metric.spi.RollupSink rollups,
            ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "at least one served metric subject is required");
        }
        return new MetricServices(subjects, rollups, resolver);
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
     */
    public Server startInProcess(String name, String apiToken, CallerResolver resolver)
            throws IOException {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver);
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
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver);
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
                          CallerResolver resolver) {
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
                        "grpc.reflection.v1.ServerReflection")));
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
