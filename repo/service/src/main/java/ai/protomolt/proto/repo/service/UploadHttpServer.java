package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.accession.DocumentUploads;
import ai.protomolt.proto.repo.accession.UploadHttpHandler;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.authz.RequestHeaders;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.repo.archive.DocumentArchive;
import ai.protomolt.proto.repo.archive.RenditionUploads;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.spi.RepositoryCaller;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The repository host's HTTP port: the parts-based document upload route, the
 * archive rendition route and the console bundle at {@link RepositoryConsole#PATH}.
 * HTTP parsing and receipts for the two upload routes live here. The document route belongs to the parts-based
 * {@link UploadHttpHandler}, which owns its receipts, status mapping and
 * credential check. Content-Length is required. Physical keys are immutable
 * per-attempt keys; the receipt returns the committed reference, including on
 * deduplicated uploads.
 */
public final class UploadHttpServer implements AutoCloseable {

    /** The claim-check store's upload route. */
    public static final String UPLOAD_PATH = "/v1/documents:upload";

    /** The archive's upload route (served when the archive surface is wired). */
    public static final String ARCHIVE_UPLOAD_PATH = "/v1/archive:upload";

    /**
     * The path reserved for the Connect gateway; an extra route can neither be it nor sit
     * under it.
     */
    public static final String CONNECT_PATH = "/connect";

    private static final Logger LOG = LoggerFactory.getLogger(UploadHttpServer.class);
    private static final int STREAM_CHUNK_BYTES = 64 * 1024;
    private static final long UPLOAD_READY_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    /** The parts-based document route. */
    private final HttpHandler documentUploads;
    private final byte[] expectedToken;
    private final Supplier<RepositoryCaller> callers;
    private final RenditionUploads v2ArchiveUploads;
    private final RepositoryConsole console;
    private final Map<String, HttpHandler> routes;
    private final ConnectGateway gateway;
    private final RequestCallerResolver requestCallers;
    private final Function<AuthenticatedCaller, RepositoryCaller> bindCaller;

    private HttpServer server;
    private ExecutorService executor;

    /**
     * Serves {@link #UPLOAD_PATH} through the parts-based {@link UploadHttpHandler}
     * over {@code uploads}; its receipts, status mapping and credential check are the
     * handler's own. {@link #ARCHIVE_UPLOAD_PATH} routes to {@code archive} when one is
     * wired. Both routes demand the same operator credential. {@link RepositoryConsole#PATH}
     * serves {@code console}'s static bundle without a credential.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as once its credential matches
     * @param apiToken the operator credential both routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH};
     *        {@link RepositoryConsole#notConfigured()} answers that route with 503
     */
    public UploadHttpServer(DocumentUploads uploads, Supplier<RepositoryCaller> callers,
            String apiToken, DocumentArchive archive, RepositoryConsole console) {
        this(uploads, callers, apiToken, archive, console, Map.of());
    }

    /**
     * Serves the routes of
     * {@link #UploadHttpServer(DocumentUploads, Supplier, String, DocumentArchive, RepositoryConsole)}
     * plus {@code routes}, each mounted as its own context before the server starts. The
     * server matches the longest context path, so a route such as {@code /console/session}
     * takes precedence over the console bundle below it. A route owns its own credential
     * check; none is applied for it here.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as once its credential matches
     * @param apiToken the operator credential both upload routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @param routes extra context path to handler; a path must start with {@code /}, must
     *        not be {@code /}, a built-in route, {@link #CONNECT_PATH} or under it
     * @throws IllegalArgumentException when a route path is refused
     */
    public UploadHttpServer(DocumentUploads uploads, Supplier<RepositoryCaller> callers,
            String apiToken, DocumentArchive archive, RepositoryConsole console,
            Map<String, HttpHandler> routes) {
        this(uploads, callers, apiToken, archive, console, routes, null, null, null);
    }

    /**
     * Serves the routes of
     * {@link #UploadHttpServer(DocumentUploads, Supplier, String, DocumentArchive, RepositoryConsole, Map)}
     * plus {@code gateway} at {@link #CONNECT_PATH}, the console's Connect calls on the same
     * origin as the bundle. The server owns the gateway from here on and closes it with
     * itself; {@code requestCallers} is the resolver the gateway was built with, kept so the
     * host hands the same one to every route that resolves a request's caller.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as once its credential matches
     * @param apiToken the operator credential both upload routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @param routes extra context path to handler, refused as in the six-argument form
     * @param gateway the Connect gateway mounted at {@link #CONNECT_PATH}
     * @param requestCallers the resolver {@code gateway} resolves callers with; the document
     *        and archive upload routes resolve their callers with it too, so a console session
     *        uploads as the scoped caller it is, and the operator token still uploads with
     *        process authority
     * @param bindCaller maps a resolved caller to the repository caller it saves as, the
     *        host's one binding from principal to accounts; required with a gateway
     */
    public UploadHttpServer(DocumentUploads uploads, Supplier<RepositoryCaller> callers,
            String apiToken, DocumentArchive archive, RepositoryConsole console,
            Map<String, HttpHandler> routes, ConnectGateway gateway,
            RequestCallerResolver requestCallers, Function<AuthenticatedCaller, RepositoryCaller> bindCaller) {
        this(uploads, callers, apiToken, archive,
                console, routes, requirePairedGateway(gateway, requestCallers),
                requestCallers, bindCaller, true);
    }

    private static ConnectGateway requirePairedGateway(
            ConnectGateway gateway, RequestCallerResolver requestCallers) {
        if ((gateway == null) != (requestCallers == null)) {
            throw new IllegalArgumentException("a Connect gateway and its request resolver come together");
        }
        return gateway;
    }

    /**
     * Serves {@link #UPLOAD_PATH} over {@code uploads} and {@link #ARCHIVE_UPLOAD_PATH}
     * through the v2 {@link DocumentArchive}, using {@code callers} and {@code apiToken}
     * for operator authentication and {@link RepositoryConsole#notConfigured()} for the
     * console bundle.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as once its credential matches
     * @param apiToken the operator credential both routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     */
    public UploadHttpServer(DocumentUploads uploads, Supplier<RepositoryCaller> callers,
            String apiToken, DocumentArchive archive) {
        this(uploads, callers, apiToken, archive,
                RepositoryConsole.notConfigured(), Map.of(), null, null, null, false);
    }

    /**
     * Serves {@link #UPLOAD_PATH} over {@code uploads} and {@link #ARCHIVE_UPLOAD_PATH}
     * through the v2 {@link DocumentArchive}, resolving every request's caller through
     * {@code requestCallers} and {@code bindCaller} when provided.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param apiToken the operator credential required by {@link RepositoryNetworkAuthentication}
     * @param requestCallers the request caller resolver; every upload resolves its caller
     *        through this resolver and maps it via {@code bindCaller}
     * @param bindCaller maps a resolved caller to the {@link RepositoryCaller} it acts as
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     */
    public UploadHttpServer(DocumentUploads uploads, String apiToken,
            RequestCallerResolver requestCallers,
            Function<AuthenticatedCaller, RepositoryCaller> bindCaller,
            DocumentArchive archive, RepositoryConsole console) {
        this(uploads, null, apiToken, archive,
                console, Map.of(), null,
                Objects.requireNonNull(requestCallers, "requestCallers"),
                Objects.requireNonNull(bindCaller, "bindCaller"), false);
    }

    /**
     * Creates an {@link UploadHttpServer} routing {@link #ARCHIVE_UPLOAD_PATH} through
     * the v2 {@link DocumentArchive}.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as once its credential matches
     * @param apiToken the operator credential both routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @return the configured HTTP server
     */
    public static UploadHttpServer withArchive(DocumentUploads uploads,
            Supplier<RepositoryCaller> callers, String apiToken,
            DocumentArchive archive, RepositoryConsole console) {
        return new UploadHttpServer(uploads, callers, apiToken, archive,
                console, Map.of(), null, null, null, false);
    }

    /**
     * Creates an {@link UploadHttpServer} routing {@link #ARCHIVE_UPLOAD_PATH} through
     * the v2 {@link DocumentArchive}, with extra routes, a mounted {@link ConnectGateway},
     * and unified caller resolution via {@code requestCallers} and {@code bindCaller}.
     *
     * @param uploads the parts-based streaming upload coordinator, or null to leave
     *        the document route unmounted (archive-only host)
     * @param callers supplies the caller a request acts as when {@code requestCallers} is null
     * @param apiToken the operator credential both upload routes require
     * @param archive the v2 document archive rendition port, or null to leave
     *        {@link #ARCHIVE_UPLOAD_PATH} unmounted
     * @param console the console bundle served at {@link RepositoryConsole#PATH}
     * @param routes extra context path to handler
     * @param gateway the Connect gateway mounted at {@link #CONNECT_PATH}, or null
     * @param requestCallers the resolver for HTTP requests, or null when no gateway/resolver is wired
     * @param bindCaller maps a resolved caller to the {@link RepositoryCaller} it acts as
     * @return the configured HTTP server
     */
    public static UploadHttpServer withArchive(DocumentUploads uploads,
            Supplier<RepositoryCaller> callers, String apiToken,
            DocumentArchive archive, RepositoryConsole console,
            Map<String, HttpHandler> routes, ConnectGateway gateway,
            RequestCallerResolver requestCallers,
            Function<AuthenticatedCaller, RepositoryCaller> bindCaller) {
        return new UploadHttpServer(uploads, callers, apiToken, archive,
                console, routes, gateway, requestCallers, bindCaller, false);
    }

    private UploadHttpServer(DocumentUploads uploads, Supplier<RepositoryCaller> callers,
            String apiToken, DocumentArchive v2Archive,
            RepositoryConsole console, Map<String, HttpHandler> routes,
            ConnectGateway gateway, RequestCallerResolver requestCallers,
            Function<AuthenticatedCaller, RepositoryCaller> bindCaller,
            boolean unusedDisambiguator) {
        if (gateway != null && requestCallers == null) {
            throw new IllegalArgumentException("a Connect gateway and its request resolver come together");
        }
        if (requestCallers != null) {
            Objects.requireNonNull(bindCaller, "bindCaller");
        }
        String token = RepositoryNetworkAuthentication.requireOperatorToken(apiToken);
        this.documentUploads = uploads == null ? null
                : requestCallers == null
                        ? new UploadHttpHandler(uploads, token, Objects.requireNonNull(callers, "callers"))
                        : new UploadHttpHandler(uploads,
                                headers -> requestCallers.resolve(RequestHeaders.of(headers)).map(bindCaller));
        this.expectedToken = token.getBytes(StandardCharsets.UTF_8);
        this.callers = callers;
        if (v2Archive == null) {
            this.v2ArchiveUploads = null;
        } else {
            RenditionUploads baseUploads = new RenditionUploads(v2Archive);
            this.v2ArchiveUploads = new RenditionUploads(
                    v2Archive,
                    ArchiveCallerHandoff.interceptChannel(baseUploads.channel()));
        }
        this.console = Objects.requireNonNull(console, "console");
        this.routes = extraRoutes(routes);
        this.gateway = gateway;
        this.requestCallers = requestCallers;
        this.bindCaller = bindCaller;
    }

    /**
     * The request resolver the mounted Connect gateway uses, for routes that resolve a
     * request's caller the same way.
     *
     * @return the resolver
     * @throws IllegalStateException when this server mounts no gateway
     */
    public RequestCallerResolver callers() {
        if (requestCallers == null) {
            throw new IllegalStateException("this repository HTTP port mounts no Connect gateway");
        }
        return requestCallers;
    }

    private static Map<String, HttpHandler> extraRoutes(Map<String, HttpHandler> routes) {
        Objects.requireNonNull(routes, "routes");
        Map<String, HttpHandler> accepted = new LinkedHashMap<>();
        routes.forEach((path, handler) -> {
            Objects.requireNonNull(path, "route path");
            Objects.requireNonNull(handler, "handler for route " + path);
            if (!path.startsWith("/") || path.equals("/")) {
                throw new IllegalArgumentException("route path must start with / and name a route: "
                        + path);
            }
            if (path.equals(UPLOAD_PATH) || path.equals(ARCHIVE_UPLOAD_PATH)
                    || path.equals(RepositoryConsole.PATH)) {
                throw new IllegalArgumentException("route path " + path + " is a built-in route");
            }
            if (path.equals(CONNECT_PATH) || path.startsWith(CONNECT_PATH + "/")) {
                throw new IllegalArgumentException("route path " + path
                        + " is reserved for the Connect gateway");
            }
            accepted.put(path, handler);
        });
        return Collections.unmodifiableMap(accepted);
    }

    /**
     * Refuses a request that does not present the configured credential. Compared in
     * constant time, so a wrong credential costs the same as a right one and the
     * comparison leaks nothing about how much of it matched. The refusal names the
     * missing header and never echoes what was presented.
     */
    private void requireCredential(HttpExchange exchange) throws HttpError {
        String presented = exchange.getRequestHeaders().getFirst("api_token");
        if (presented == null) {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization != null
                    && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        if (presented == null || presented.isBlank()
                || !MessageDigest.isEqual(presented.getBytes(StandardCharsets.UTF_8),
                        expectedToken)) {
            throw new HttpError(401, "missing or invalid credential 'api_token'");
        }
    }

    /**
     * Binds and starts the server.
     *
     * @param port the listen port (0 = ephemeral, read it back via {@link #port()})
     * @return the bound port
     */
    public synchronized int start(int port) {
        if (server != null || executor != null) {
            throw new IllegalStateException("HTTP upload server already started");
        }
        HttpServer created;
        try {
            created = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind HTTP upload server on port " + port, e);
        }
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = created;
        this.executor = pool;
        try {
            if (documentUploads != null) {
                created.createContext(UPLOAD_PATH, documentUploads);
            }
            if (v2ArchiveUploads != null) {
                created.createContext(ARCHIVE_UPLOAD_PATH, this::handleArchive);
            }
            created.createContext(RepositoryConsole.PATH, console.handler());
            routes.forEach(created::createContext);
            if (gateway != null) {
                created.createContext(CONNECT_PATH, gateway);
            }
            created.setExecutor(pool);
            created.start();
        } catch (RuntimeException | Error e) {
            try { close(); }
            catch (RuntimeException | Error cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
        int bound = created.getAddress().getPort();
        LOG.info("repository HTTP port {} serving {}{}{}{}{}", bound,
                documentUploads != null ? UPLOAD_PATH + " " : "",
                v2ArchiveUploads != null ? ARCHIVE_UPLOAD_PATH + " " : "",
                console.configured() ? RepositoryConsole.PATH : RepositoryConsole.PATH + " (not configured)",
                routes.isEmpty() ? "" : " " + String.join(" ", routes.keySet()),
                gateway != null ? " " + CONNECT_PATH + " (" + gateway.procedures().size() + " procedures)" : "");
        return bound;
    }

    /**
     * The bound port.
     *
     * @return the port the server is listening on
     */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("HTTP upload server not started");
        }
        return server.getAddress().getPort();
    }

    @Override
    public synchronized void close() {
        close(java.time.Duration.ofSeconds(10));
    }

    synchronized void close(java.time.Duration timeout) {
        if (server != null) {
            server.stop(1);
            server = null;
        }
        if (gateway != null) {
            gateway.close();
        }
        if (executor != null) {
            try { ExecutorShutdown.stop(executor, timeout); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("HTTP shutdown interrupted; resources retained", interrupted);
            }
            executor = null;
        }
    }

    // ------------------------------------------------------------------ archive route

    private record ResolvedCaller(RepositoryCaller caller, AuthenticatedCaller authenticated) {}

    /**
     * Resolves the caller for {@link #ARCHIVE_UPLOAD_PATH} using the same path as
     * {@link #UPLOAD_PATH}: when {@code requestCallers} is wired, resolves the request
     * headers through {@code requestCallers} and maps the authenticated principal via
     * {@code bindCaller}; otherwise verifies the operator credential and asks
     * {@code callers}.
     */
    private ResolvedCaller resolveArchiveCaller(HttpExchange exchange) {
        if (requestCallers != null) {
            Optional<AuthenticatedCaller> resolved;
            try {
                resolved = requestCallers.resolve(RequestHeaders.of(exchange.getRequestHeaders()));
            } catch (IllegalStateException outage) {
                throw new HttpError(503, outage.getMessage());
            }
            if (resolved.isEmpty()) {
                throw new HttpError(401, "missing or invalid credential 'api_token'");
            }
            AuthenticatedCaller authenticated = resolved.get();
            RepositoryCaller caller = Objects.requireNonNull(
                    bindCaller.apply(authenticated), "caller");
            return new ResolvedCaller(caller, authenticated);
        }
        requireCredential(exchange);
        RepositoryCaller caller = callers != null
                ? Objects.requireNonNull(callers.get(), "caller")
                : new RepositoryCaller("http-upload", true);
        return new ResolvedCaller(caller, null);
    }

    private void handleArchive(HttpExchange exchange) throws IOException {
        try {
            ResolvedCaller resolved = resolveArchiveCaller(exchange);
            if (!ARCHIVE_UPLOAD_PATH.equals(exchange.getRequestURI().getPath())) {
                throw new HttpError(404, "unknown route " + exchange.getRequestURI().getPath());
            }
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new HttpError(405, "method not allowed: " + exchange.getRequestMethod());
            }
            archiveUpload(exchange, resolved);
        } catch (HttpError e) {
            writeError(exchange, e.status, e.getMessage());
        } catch (ai.protomolt.proto.repo.spi.RepositoryException e) {
            int status = switch (e.code()) {
                case INVALID_ARGUMENT -> 400;
                case NOT_FOUND -> 404;
                case PERMISSION_DENIED -> 403;
                case FAILED_PRECONDITION, CONFLICT -> 409;
                default -> 502;
            };
            writeError(exchange, status, e.getMessage());
        } catch (StatusRuntimeException e) {
            // The archive flows' gRPC status vocabulary, flattened onto HTTP.
            int status = switch (e.getStatus().getCode()) {
                case INVALID_ARGUMENT -> 400;
                case UNAUTHENTICATED -> 401;
                case PERMISSION_DENIED -> 403;
                case NOT_FOUND -> 404;
                case ALREADY_EXISTS, FAILED_PRECONDITION, ABORTED -> 409;
                case RESOURCE_EXHAUSTED -> 429;
                case UNAVAILABLE -> 503;
                default -> 502;
            };
            exchange.getResponseHeaders().set("Connection", "close");
            writeProtobuf(exchange, status, io.grpc.protobuf.StatusProto.fromThrowable(e));
        } catch (RuntimeException | IOException e) {
            LOG.warn("archive upload failed: {}", e.getMessage());
            writeError(exchange, 502, "backing store failure: " + e.getMessage());
        } finally {
            exchange.close();
        }
    }

    /**
     * The archive's zero-dependency door: the body is one rendition's bytes,
     * identity rides query parameters or headers (query wins) — {@code account_id},
     * {@code archive}, {@code entry_id}, {@code rendition} (default {@code original}),
     * {@code sub_key}, {@code schema_subject}, {@code module}, {@code operation_id},
     * {@code expected_version}, {@code Content-Type}, and optional {@code X-Content-Sha256}
     * ({@code expected_sha256}) — streamed through the same verified, content-addressed
     * landing as the gRPC streaming door, one new entry version per upload.
     *
     * <p>The response is the binary protobuf {@link UploadRenditionResponse}
     * ({@code Content-Type: application/protobuf}) with {@code X-Operation-Id} and
     * {@code X-Replayed} response headers.
     */
    private void archiveUpload(HttpExchange exchange, ResolvedCaller resolved) throws IOException {
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String accountId = required(identity(exchange, query, "account_id", "x-account-id"),
                "account_id");
        String archive = required(identity(exchange, query, "archive", "x-archive"), "archive");
        String entryId = required(identity(exchange, query, "entry_id", "x-entry-id"),
                "entry_id");
        String rendition = identity(exchange, query, "rendition", "x-rendition");
        String subKey = identity(exchange, query, "sub_key", "x-sub-key");
        String schemaSubject = identity(exchange, query, "schema_subject", "x-schema-subject");
        String module = identity(exchange, query, "module", "x-module");
        String operationId = identity(exchange, query, "operation_id", "x-operation-id");
        long expectedVersion = parseExpectedVersion(
                identity(exchange, query, "expected_version", "x-expected-version"));
        String contentType = first(query.get("content_type"),
                exchange.getRequestHeaders().getFirst("Content-Type"));
        String declaredSha = identity(exchange, query, "expected_sha256", "X-Content-Sha256");
        long contentLength = contentLength(exchange);
        if (contentLength <= 0) {
            throw new HttpError(411, "Content-Length must be positive for an archive upload");
        }

        EntryAddress address = EntryAddress.newBuilder()
                .setAccountId(accountId)
                .setArchive(archive)
                .setEntryId(entryId)
                .build();
        RenditionDescriptor.Builder descriptorBuilder = RenditionDescriptor.newBuilder()
                .setName(rendition == null ? "original" : rendition)
                .setMediaType(contentType);
        if (subKey != null) {
            descriptorBuilder.setSubKey(subKey);
        }
        if (schemaSubject != null) {
            descriptorBuilder.setSchemaSubject(schemaSubject);
        }
        RenditionDescriptor descriptor = descriptorBuilder.build();

        if (identity(exchange, query, "filename", "x-filename") != null
                || identity(exchange, query, "title", "x-title") != null
                || identity(exchange, query, "source_uri", "x-source-uri") != null) {
            throw new HttpError(400, "filename, title and source_uri are not supported by v2 archive upload; "
                    + "set entry metadata through PutEntry");
        }
        writeProto(exchange, 200, uploadV2Archive(exchange, resolved, address, descriptor,
                contentLength, declaredSha, module, operationId, expectedVersion));
    }

    private UploadRenditionResponse uploadV2Archive(HttpExchange exchange, ResolvedCaller resolved,
            EntryAddress address, RenditionDescriptor descriptor, long contentLength,
            String declaredSha, String module, String operationId, long expectedVersion) throws IOException {
        WriteAttribution.Builder attribution = WriteAttribution.newBuilder()
                .setActor(resolved.caller().principalName());
        if (module != null) {
            attribution.setModule(module);
        }
        UploadRenditionHeader.Builder header = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(descriptor)
                .setSizeBytes(contentLength)
                .setExpectedVersion(expectedVersion)
                .setWrittenBy(attribution.build());
        if (declaredSha != null) {
            header.setExpectedSha256(declaredSha);
        }
        if (operationId != null) {
            header.setOperationId(operationId);
        }

        CompletableFuture<UploadRenditionResponse> responseFuture = new CompletableFuture<>();
        StreamObserver<UploadRenditionResponse> responseObserver =
                new StreamObserver<>() {
                    private UploadRenditionResponse value;

                    @Override
                    public void onNext(UploadRenditionResponse response) {
                        this.value = response;
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        if (value == null) {
                            responseFuture.completeExceptionally(Status.INTERNAL
                                    .withDescription("upload completed without a response")
                                    .asRuntimeException());
                        } else {
                            responseFuture.complete(value);
                        }
                    }
                };

        UploadRenditionResponse result = ArchiveCallerHandoff.callWithCaller(
                resolved.caller(), resolved.authenticated(), () -> {
                    RenditionUploads.FlowControlledUpload stream =
                            v2ArchiveUploads.uploadFlowControlled(responseObserver);
                    InputStream body = exchange.getRequestBody();
                    long bytesRead = 0L;
                    try {
                        stream.onNext(UploadRenditionRequest.newBuilder()
                                .setHeader(header.build())
                                .build());
                        if (!responseFuture.isDone()
                                && !resolved.caller().processAuthority()
                                && resolved.caller().credentialBinding().isEmpty()) {
                            exchange.getResponseHeaders().set("Connection", "close");
                            StatusRuntimeException denied = Status.PERMISSION_DENIED
                                    .withDescription("Saving requires a credential-bound caller or process authority")
                                    .asRuntimeException();
                            stream.onError(denied);
                            throw denied;
                        }
                        byte[] buffer = new byte[STREAM_CHUNK_BYTES];
                        while (bytesRead < contentLength && !responseFuture.isDone()) {
                            if (!stream.awaitReady(UPLOAD_READY_TIMEOUT_NANOS)) {
                                StatusRuntimeException timeout = Status.DEADLINE_EXCEEDED
                                        .withDescription("timed out waiting for archive upload stream readiness")
                                        .asRuntimeException();
                                stream.onError(timeout);
                                throw timeout;
                            }
                            if (responseFuture.isDone()) {
                                break;
                            }
                            int read = body.read(buffer, 0, (int) Math.min(buffer.length, contentLength - bytesRead));
                            if (read == -1) {
                                break;
                            }
                            if (read > 0) {
                                bytesRead += read;
                                stream.onNext(UploadRenditionRequest.newBuilder()
                                        .setChunk(ByteString.copyFrom(buffer, 0, read))
                                        .build());
                            }
                        }
                        if (!responseFuture.isDone()) {
                            if (bytesRead < contentLength) {
                                exchange.getResponseHeaders().set("Connection", "close");
                                StatusRuntimeException truncated = Status.INVALID_ARGUMENT
                                        .withDescription("truncated HTTP upload stream: expected "
                                                + contentLength + " bytes, got " + bytesRead)
                                        .asRuntimeException();
                                stream.onError(truncated);
                                throw new HttpError(400, truncated.getStatus().getDescription());
                            }
                            stream.onCompleted();
                            body.close();
                        } else if (bytesRead < contentLength) {
                            // The call terminated early (for example on an immediate idempotent replay
                            // or header-time refusal). Mark Connection: close instead of draining the
                            // remaining request body so large refused or replayed bodies are not read.
                            exchange.getResponseHeaders().set("Connection", "close");
                        } else {
                            body.close();
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        exchange.getResponseHeaders().set("Connection", "close");
                        StatusRuntimeException cancelled = Status.CANCELLED
                                .withDescription("archive upload interrupted")
                                .withCause(interrupted)
                                .asRuntimeException();
                        stream.onError(cancelled);
                        throw cancelled;
                    } catch (IOException ioFailure) {
                        exchange.getResponseHeaders().set("Connection", "close");
                        if (bytesRead < contentLength) {
                            StatusRuntimeException truncated = Status.INVALID_ARGUMENT
                                    .withDescription("truncated HTTP upload stream after "
                                            + bytesRead + " of " + contentLength + " bytes: "
                                            + ioFailure.getMessage())
                                    .withCause(ioFailure)
                                    .asRuntimeException();
                            stream.onError(truncated);
                            throw new HttpError(400, truncated.getStatus().getDescription());
                        }
                        stream.onError(Status.INTERNAL
                                .withDescription("failed to read HTTP upload stream: " + ioFailure.getMessage())
                                .withCause(ioFailure)
                                .asRuntimeException());
                        throw ioFailure;
                    } catch (RuntimeException failure) {
                        if (bytesRead < contentLength) {
                            exchange.getResponseHeaders().set("Connection", "close");
                        }
                        stream.onError(failure);
                        throw failure;
                    }
                    try {
                        return responseFuture.get();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw Status.CANCELLED
                                .withDescription("archive upload interrupted")
                                .withCause(interrupted)
                                .asRuntimeException();
                    } catch (ExecutionException failed) {
                        Throwable cause = failed.getCause();
                        if (cause instanceof StatusRuntimeException sre) {
                            throw sre;
                        }
                        if (cause instanceof ai.protomolt.proto.repo.spi.RepositoryException re) {
                            throw re;
                        }
                        if (cause instanceof HttpError httpError) {
                            throw httpError;
                        }
                        if (cause instanceof IOException io) {
                            throw io;
                        }
                        if (cause instanceof RuntimeException runtime) {
                            throw runtime;
                        }
                        throw new IllegalStateException("archive upload failed", cause);
                    }
                });

        return result;
    }

    private static long parseExpectedVersion(String raw) {
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        try {
            long parsed = Long.parseLong(raw.trim());
            if (parsed < 0) {
                throw new NumberFormatException("negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new HttpError(400, "expected_version must be a non-negative integer (got \""
                    + raw + "\")");
        }
    }

    // ------------------------------------------------------------------ plumbing

    /** A failure carrying its HTTP status; thrown at the decision point. */
    private static final class HttpError extends RuntimeException {
        private final int status;

        private HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private static long contentLength(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Content-Length");
        if (header == null || header.isBlank()) {
            throw new HttpError(411, "Content-Length header is required"
                    + " (chunked uploads are not accepted: the store streams with a known length)");
        }
        try {
            long length = Long.parseLong(header.trim());
            if (length < 0) {
                throw new NumberFormatException("negative");
            }
            return length;
        } catch (NumberFormatException e) {
            throw new HttpError(411, "Content-Length must be a non-negative byte count (got \""
                    + header + "\")");
        }
    }

    /** Query parameter wins over the header; either may be absent. */
    private static String identity(HttpExchange exchange, Map<String, String> query,
            String queryName, String headerName) {
        String value = query.get(queryName);
        if (value == null || value.isBlank()) {
            value = exchange.getRequestHeaders().getFirst(headerName);
        }
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static String required(String value, String name) {
        if (value == null) {
            throw new HttpError(400, name + " is required (query parameter or header)");
        }
        return value;
    }

    private static String first(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate.trim();
            }
        }
        return "application/octet-stream";
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return out;
        }
        int from = 0;
        while (from <= rawQuery.length()) {
            int amp = rawQuery.indexOf('&', from);
            String pair = amp < 0 ? rawQuery.substring(from) : rawQuery.substring(from, amp);
            int eq = pair.indexOf('=');
            if (eq > 0) {
                out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
            if (amp < 0) {
                break;
            }
            from = amp + 1;
        }
        return out;
    }

    private static void writeProto(HttpExchange exchange, int status, UploadRenditionResponse response)
            throws IOException {
        exchange.getResponseHeaders().set("X-Operation-Id", response.getOperationId());
        exchange.getResponseHeaders().set("X-Replayed", Boolean.toString(response.getReplayed()));
        writeProtobuf(exchange, status, response);
    }

    private static void writeProtobuf(HttpExchange exchange, int status,
            com.google.protobuf.MessageLite response) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/protobuf");
        exchange.sendResponseHeaders(status, response.getSerializedSize());
        try (OutputStream body = exchange.getResponseBody()) {
            response.writeTo(body);
        }
    }

    private static void writeError(HttpExchange exchange, int status, String message) throws IOException {
        // Refuse from headers without waiting for an untrusted request body.
        exchange.getResponseHeaders().set("Connection", "close");
        int code = switch (status) {
            case 400, 411 -> Status.Code.INVALID_ARGUMENT.value();
            case 401 -> Status.Code.UNAUTHENTICATED.value();
            case 403 -> Status.Code.PERMISSION_DENIED.value();
            case 404 -> Status.Code.NOT_FOUND.value();
            case 405 -> Status.Code.UNIMPLEMENTED.value();
            case 409 -> Status.Code.FAILED_PRECONDITION.value();
            case 429 -> Status.Code.RESOURCE_EXHAUSTED.value();
            case 503 -> Status.Code.UNAVAILABLE.value();
            default -> Status.Code.INTERNAL.value();
        };
        writeProtobuf(exchange, status, com.google.rpc.Status.newBuilder()
                .setCode(code).setMessage(message == null ? "archive upload failed" : message).build());
    }

}
