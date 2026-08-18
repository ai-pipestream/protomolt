package ai.pipestream.proto.search.door;

import ai.pipestream.proto.grpc.validate.ValidatingServerInterceptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * One factory wires the door stack: the Lucene store (which resolves every
 * chunk lane's embedding provider at build), the document fetcher, and the
 * two gRPC services mounted together on one server. There is no DI
 * framework; this factory is the SPI.
 */
public final class SearchDoorServices implements AutoCloseable {

    private final SearchDoorConfig config;
    private final DocumentFetcher fetcher;
    private final LuceneSearchStore store;
    private final SearchDoorGrpcServices.Index index;
    private final SearchDoorGrpcServices.Search search;
    private Server server;

    private SearchDoorServices(SearchDoorConfig config, DocumentFetcher fetcher) {
        this.config = config;
        this.fetcher = fetcher;
        this.store = new LuceneSearchStore(config.indexDir(), config.subjects(),
                config.snapshots(), config.readOnly());
        // A read-only door has no write surface: the indexing service is
        // not mounted, so its RPCs answer UNIMPLEMENTED.
        this.index = config.readOnly()
                ? null
                : new SearchDoorGrpcServices.Index(store, fetcher);
        this.search = new SearchDoorGrpcServices.Search(store);
    }

    /**
     * Builds the stack.
     *
     * @param config the door configuration
     * @param fetcher the document fetcher, required unless the
     *        configuration is read-only (a reader indexes nothing and
     *        fetches nothing); closed with this factory when it is
     *        {@link AutoCloseable}
     * @return the wired, not-yet-started stack
     */
    public static SearchDoorServices build(SearchDoorConfig config, DocumentFetcher fetcher) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (fetcher == null && !config.readOnly()) {
            throw new IllegalArgumentException("fetcher must not be null");
        }
        return new SearchDoorServices(config, fetcher);
    }

    /**
     * Starts both services on an in-process server named {@code name}.
     *
     * @param name the in-process server name
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startInProcess(String name) throws IOException {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        if (index != null) {
            builder.addService(index);
        }
        server = builder.addService(search)
                .build()
                .start();
        return server;
    }

    /**
     * Starts both services on a Netty server with health and reflection.
     *
     * @param port the port to bind; {@code 0} picks a free port
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startNetty(int port) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        if (index != null) {
            builder.addService(index);
        }
        server = builder.addService(search)
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

    /** The door's configuration. */
    public SearchDoorConfig config() {
        return config;
    }

    /** The door's Lucene state, for wiring that shares it (replay's prune). */
    LuceneSearchStore store() {
        return store;
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
        }
        store.close();
        if (fetcher instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new IllegalStateException("cannot close the document fetcher", e);
            }
        }
    }
}
