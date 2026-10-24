package ai.protomolt.proto.search.service;

import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.authz.grpc.ScopeServerInterceptor;
import ai.protomolt.proto.grpc.validate.UnaryRequestAdmission;
import ai.protomolt.proto.grpc.validate.ValidatingServerInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/**
 * One factory wires the service stack: the Lucene store (which resolves every
 * chunk lane's embedding provider at build), the document fetcher, and the
 * two gRPC services mounted together on one server. There is no DI
 * framework; this factory is the SPI.
 */
public final class SearchServices implements AutoCloseable {

    /** Default per-request wire cap (4 MiB). */
    public static final int DEFAULT_MAX_REQUEST_BYTES = 4 * 1024 * 1024;

    /** Default maximum concurrent unary calls holding pre-half-close reservations. */
    public static final int DEFAULT_MAX_ACTIVE = 64;

    private static final Set<String> OPEN_SERVICES = Set.of(
            "grpc.health.v1.Health",
            "grpc.reflection.v1.ServerReflection");

    private final SearchServiceConfig config;
    private final DocumentFetcher fetcher;
    private final LuceneSearchStore store;
    private final SearchGrpcServices.Index index;
    private final SearchGrpcServices.Search search;
    private final Set<String> admittedMethods;
    private final List<Server> servers = new CopyOnWriteArrayList<>();
    private volatile UnaryRequestAdmission admission;
    private volatile Server server;

    private SearchServices(SearchServiceConfig config, DocumentFetcher fetcher,
            ai.protomolt.proto.validate.ProtoValidator documentGate,
            java.util.function.Supplier<ai.protomolt.proto.screening.Screener> screening) {
        this.config = config;
        this.fetcher = fetcher;
        this.store = new LuceneSearchStore(config.indexDir(), config.subjects(),
                config.snapshots(), config.readOnly(), config.vectorization());
        try {
            // A read-only service has no write surface: the indexing service is
            // not mounted, so its RPCs answer UNIMPLEMENTED.
            this.index = config.readOnly()
                    ? null
                    : new SearchGrpcServices.Index(store, fetcher, documentGate, screening);
            this.search = new SearchGrpcServices.Search(store);
            Set<String> methods = new LinkedHashSet<>();
            for (MethodDescriptor<?, ?> method
                    : this.search.bindService().getServiceDescriptor().getMethods()) {
                methods.add(method.getFullMethodName());
            }
            if (this.index != null) {
                for (MethodDescriptor<?, ?> method
                        : this.index.bindService().getServiceDescriptor().getMethods()) {
                    methods.add(method.getFullMethodName());
                }
            }
            this.admittedMethods = Set.copyOf(methods);
            this.admission = new UnaryRequestAdmission(
                    2L * DEFAULT_MAX_REQUEST_BYTES * DEFAULT_MAX_ACTIVE,
                    DEFAULT_MAX_REQUEST_BYTES,
                    DEFAULT_MAX_ACTIVE,
                    this.admittedMethods,
                    Map.of(),
                    OPEN_SERVICES);
        } catch (RuntimeException | Error failure) {
            try {
                store.close();
            } catch (RuntimeException | Error suppressed) {
                failure.addSuppressed(suppressed);
            }
            throw failure;
        }
    }

    /**
     * Builds the stack.
     *
     * @param config the service configuration
     * @param fetcher the document fetcher, required unless the
     *        configuration is read-only (a reader indexes nothing and
     *        fetches nothing); closed with this factory when it is
     *        {@link AutoCloseable}
     * @return the wired, not-yet-started stack
     */
    public static SearchServices build(SearchServiceConfig config, DocumentFetcher fetcher) {
        return build(config, fetcher, null);
    }

    /**
     * As {@link #build(SearchServiceConfig, DocumentFetcher)}, with a document
     * gate: every fetched document validates against {@code documentGate}'s
     * declared rules before anything indexes — typically a validator built
     * over the mounted taxonomy catalog, so membership is enforced as of
     * index time — and a violating document is refused naming its
     * violations. {@code null} keeps the service's historical behavior: no
     * declared-rules validation at index time.
     *
     * @param config the service configuration
     * @param fetcher the document fetcher; see the two-argument overload
     * @param documentGate the validator fetched documents must pass, or
     *        {@code null} for no gate
     * @return the wired, not-yet-started stack
     */
    public static SearchServices build(SearchServiceConfig config, DocumentFetcher fetcher,
            ai.protomolt.proto.validate.ProtoValidator documentGate) {
        return build(config, fetcher, documentGate, null);
    }

    /**
     * As the document-gate overload, with a screening mount: fetched
     * documents screen through the supplied
     * screener before indexing, so a mounted mask policy redacts detected
     * spans on the way in and the response carries the model version and
     * threshold as evidence. The supplier is consulted per request because
     * mounts swap on the config lane; a configured service whose supplier
     * returns {@code null} (no mount live yet) refuses indexing fail-closed,
     * the taxonomy gate's boot stance. {@code null} for the supplier itself
     * means screening was never configured.
     *
     * @param config the service configuration
     * @param fetcher the document fetcher; see the two-argument overload
     * @param documentGate the validator fetched documents must pass, or
     *        {@code null} for no gate
     * @param screening the live screening mount, or {@code null} when
     *        screening is not configured
     * @return the wired, not-yet-started stack
     */
    public static SearchServices build(SearchServiceConfig config, DocumentFetcher fetcher,
            ai.protomolt.proto.validate.ProtoValidator documentGate,
            java.util.function.Supplier<ai.protomolt.proto.screening.Screener> screening) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (fetcher == null && !config.readOnly()) {
            throw new IllegalArgumentException("fetcher must not be null");
        }
        return new SearchServices(config, fetcher, documentGate, screening);
    }

    /**
     * Replaces the pre-half-close unary admission gate before starting a server.
     *
     * @param maxRequestBytes per-request wire cap in bytes
     * @param maxActive maximum concurrent unary calls holding reservations
     * @return this instance, for chaining
     */
    public SearchServices withAdmission(int maxRequestBytes, int maxActive) {
        this.admission = new UnaryRequestAdmission(
                2L * maxRequestBytes * maxActive,
                maxRequestBytes,
                maxActive,
                this.admittedMethods,
                Map.of(),
                OPEN_SERVICES);
        return this;
    }

    /**
     * The unary request admission gate shared across started servers.
     *
     * @return the admission gate
     */
    public UnaryRequestAdmission admission() {
        return admission;
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
     * Starts in-process with service identity: the operator token holds every scope, a
     * credential the resolver names is scope-checked — queries need {@code search-query},
     * the indexing service needs {@code search-index}. Without a token the service stays the
     * open, trusted-network surface it is today.
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
        admission.configure(builder);
        identity(builder, apiToken, resolver, budgets);
        if (index != null) {
            builder.addService(index);
        }
        Server started = builder.addService(search)
                .build()
                .start();
        servers.add(started);
        server = started;
        return started;
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

    /** Starts on Netty with service identity; see
     * {@link #startInProcess(String, String, CallerResolver)}. */
    public Server startNetty(int port, String apiToken, CallerResolver resolver)
            throws IOException {
        return startNetty(port, apiToken, resolver, new ScopeBudgets());
    }

    /**
     * Starts on Netty with service identity, spending on {@code budgets}: a node that also
     * mounts the action catalog (the search action queries the same store) passes the ledger
     * it wired there, so a principal's search budget is one allowance across both transports.
     */
    public Server startNetty(int port, String apiToken, CallerResolver resolver,
            ScopeBudgets budgets) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(ValidatingServerInterceptor.create());
        admission.configure(builder);
        identity(builder, apiToken, resolver, budgets);
        if (index != null) {
            builder.addService(index);
        }
        Server started = builder.addService(search)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        servers.add(started);
        server = started;
        return started;
    }

    /**
     * Installs the credential and scope interceptors: added after the validating and
     * admission interceptors so the credential check runs first on the wire, the scope
     * table second, pre-half-close admission third, and validation fourth.
     */
    private void identity(ServerBuilder<?> builder, String apiToken,
                          CallerResolver resolver, ScopeBudgets budgets) {
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
        ScopeServerInterceptor scopes = new ScopeServerInterceptor(
                services, Map.of(), OPEN_SERVICES, budgets);
        scopes.configure(builder);
        builder.intercept(new ApiTokenServerInterceptor(apiToken, resolver));
    }

    /** The bound server, once one of the start methods has run. */
    public Server server() {
        return server;
    }

    /** The service's configuration. */
    public SearchServiceConfig config() {
        return config;
    }

    /** The service's Lucene state, for wiring that shares it (replay's prune). */
    LuceneSearchStore store() {
        return store;
    }

    @Override
    public void close() {
        admission.close();
        for (Server running : servers) {
            running.shutdownNow();
        }
        servers.clear();
        RuntimeException failure = null;
        try {
            store.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        if (fetcher instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                IllegalStateException closeFailure =
                        new IllegalStateException("cannot close the document fetcher", e);
                if (failure != null) {
                    failure.addSuppressed(closeFailure);
                } else {
                    failure = closeFailure;
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
