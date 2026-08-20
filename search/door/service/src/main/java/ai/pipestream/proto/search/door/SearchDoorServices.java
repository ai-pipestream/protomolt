package ai.pipestream.proto.search.door;

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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

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

    private SearchDoorServices(SearchDoorConfig config, DocumentFetcher fetcher,
            ai.pipestream.proto.validate.ProtoValidator documentGate,
            java.util.function.Supplier<ai.pipestream.proto.screening.Screener> screening) {
        this.config = config;
        this.fetcher = fetcher;
        this.store = new LuceneSearchStore(config.indexDir(), config.subjects(),
                config.snapshots(), config.readOnly());
        // A read-only door has no write surface: the indexing service is
        // not mounted, so its RPCs answer UNIMPLEMENTED.
        this.index = config.readOnly()
                ? null
                : new SearchDoorGrpcServices.Index(store, fetcher, documentGate, screening);
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
        return build(config, fetcher, null);
    }

    /**
     * As {@link #build(SearchDoorConfig, DocumentFetcher)}, with a document
     * gate: every fetched document validates against {@code documentGate}'s
     * declared rules before anything indexes — typically a validator built
     * over the mounted taxonomy catalog, so membership is enforced as of
     * index time — and a violating document is refused naming its
     * violations. {@code null} keeps the door's historical behavior: no
     * declared-rules validation at index time.
     *
     * @param config the door configuration
     * @param fetcher the document fetcher; see the two-argument overload
     * @param documentGate the validator fetched documents must pass, or
     *        {@code null} for no gate
     * @return the wired, not-yet-started stack
     */
    public static SearchDoorServices build(SearchDoorConfig config, DocumentFetcher fetcher,
            ai.pipestream.proto.validate.ProtoValidator documentGate) {
        return build(config, fetcher, documentGate, null);
    }

    /**
     * As the document-gate overload, with a screening mount: fetched
     * documents screen through the supplied
     * screener before indexing, so a mounted mask policy redacts detected
     * spans on the way in and the response carries the model version and
     * threshold as evidence. The supplier is consulted per request because
     * mounts swap on the config lane; a configured door whose supplier
     * returns {@code null} (no mount live yet) refuses indexing fail-closed,
     * the taxonomy gate's boot stance. {@code null} for the supplier itself
     * means screening was never configured.
     *
     * @param config the door configuration
     * @param fetcher the document fetcher; see the two-argument overload
     * @param documentGate the validator fetched documents must pass, or
     *        {@code null} for no gate
     * @param screening the live screening mount, or {@code null} when
     *        screening is not configured
     * @return the wired, not-yet-started stack
     */
    public static SearchDoorServices build(SearchDoorConfig config, DocumentFetcher fetcher,
            ai.pipestream.proto.validate.ProtoValidator documentGate,
            java.util.function.Supplier<ai.pipestream.proto.screening.Screener> screening) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (fetcher == null && !config.readOnly()) {
            throw new IllegalArgumentException("fetcher must not be null");
        }
        return new SearchDoorServices(config, fetcher, documentGate, screening);
    }

    /**
     * Starts both services on an in-process server named {@code name}.
     *
     * @param name the in-process server name
     * @return the started server
     * @throws IOException when the server fails to bind
     */
    public Server startInProcess(String name) throws IOException {
        return startInProcess(name, null, null);
    }

    /**
     * Starts in-process with door identity: the operator token holds every scope, a
     * credential the resolver names is scope-checked — queries need {@code search-query},
     * the indexing service needs {@code search-index}. Without a token the door stays the
     * open, trusted-network surface it is today.
     */
    public Server startInProcess(String name, String apiToken, CallerResolver resolver)
            throws IOException {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver);
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
        return startNetty(port, null, null);
    }

    /** Starts on Netty with door identity; see
     * {@link #startInProcess(String, String, CallerResolver)}. */
    public Server startNetty(int port, String apiToken, CallerResolver resolver)
            throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        identity(builder::intercept, apiToken, resolver);
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
        Map<String, String> services = new LinkedHashMap<>();
        services.put(search.bindService().getServiceDescriptor().getName(),
                Scopes.SEARCH_QUERY);
        if (index != null) {
            services.put(index.bindService().getServiceDescriptor().getName(),
                    Scopes.SEARCH_INDEX);
        }
        intercept.accept(new ScopeServerInterceptor(
                services, Map.of(),
                Set.of("grpc.health.v1.Health",
                        "grpc.reflection.v1.ServerReflection")));
        intercept.accept(new ApiTokenServerInterceptor(apiToken, resolver));
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
