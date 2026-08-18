package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.grpc.validate.ValidatingServerInterceptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * One factory wires the metric door: the MetricService over its served
 * subjects, mounted with the validating server interceptor from day one, so
 * the request protos' validate.v1 rules are what enforces the shape rules
 * and no handler re-implements a range check. A sibling of the search door,
 * never a second RPC on it: retrieval and aggregation stay independently
 * mountable.
 */
public final class MetricDoorServices implements AutoCloseable {

    private final Map<String, ServedMetricSubject> subjects;
    private final MetricGrpcService service;
    private Server server;

    private MetricDoorServices(
            Map<String, ServedMetricSubject> subjects,
            ai.pipestream.proto.metric.spi.RollupSink rollups) {
        this.subjects = Map.copyOf(subjects);
        this.service = new MetricGrpcService(this.subjects, rollups);
    }

    /**
     * Builds the stack without a rollup sink: RebuildRollup refuses with
     * {@code missing-sink}.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @return the wired, not-yet-started stack
     */
    public static MetricDoorServices build(Map<String, ServedMetricSubject> subjects) {
        return build(subjects, null);
    }

    /**
     * Builds the stack.
     *
     * @param subjects the served subjects, keyed by subject name; non-empty
     * @param rollups where rebuilt rollups land, or {@code null} for none
     * @return the wired, not-yet-started stack
     */
    public static MetricDoorServices build(
            Map<String, ServedMetricSubject> subjects,
            ai.pipestream.proto.metric.spi.RollupSink rollups) {
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "at least one served metric subject is required");
        }
        return new MetricDoorServices(subjects, rollups);
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
        server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create())
                .addService(service)
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
        HealthStatusManager health = new HealthStatusManager();
        server = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create())
                .addService(service)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        return server;
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
