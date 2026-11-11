package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.registry.ConfigSupport;
import ai.protomolt.proto.registry.InvalidConfigException;
import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.WorkflowDocumentStore;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.registry.CompatibilityModes;
import ai.protomolt.proto.registry.IncompatibleRegistrationException;
import ai.protomolt.proto.registry.InvalidSchemaException;
import ai.protomolt.proto.registry.ReferenceNotFoundException;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.registry.StoredSchema;
import ai.protomolt.proto.registry.StoredSchemaSources;
import ai.protomolt.proto.emit.parquet.ProtoParquetSchemas;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Confluent-subjects-protocol facade over a {@link SchemaRegistryStore}: JDK
 * {@link HttpServer} on virtual threads, zero extra dependencies — the same idiom as the
 * {@code servers/jdk} REST host.
 *
 * <h2>Endpoints (content type {@code application/vnd.schemaregistry.v1+json})</h2>
 * <ul>
 *   <li>{@code GET /subjects}, {@code GET /subjects/{subject}/versions},
 *       {@code GET /subjects/{subject}/versions/{version|latest}}</li>
 *   <li>{@code POST /subjects/{subject}/versions} (register, response {@code {id}}),
 *       {@code POST /subjects/{subject}} (lookup by content)</li>
 *   <li>{@code GET /schemas/ids/{id}}</li>
 *   <li>{@code GET|PUT /config} and {@code GET|PUT /config/{subject}} — including the
 *       Confluent key quirk: PUT bodies/responses use {@code compatibility}, GET responses use
 *       {@code compatibilityLevel}</li>
 *   <li>native extras: {@code GET {nativePrefix}/subjects/{subject}/descriptor-set} (binary
 *       {@code FileDescriptorSet} of the subject's latest schema plus transitive references),
 *       {@code GET {nativePrefix}/subjects/{subject}/parquet-schema?message={fqn}[&version={n}]}
 *       (the Parquet schema of one message of the subject as canonical schema text — a pure
 *       function of the descriptor, derived on read and never stored) and
 *       {@code GET /health}</li>
 * </ul>
 *
 * <p>Errors are Confluent-style {@code {error_code, message}} JSON: 40401 unknown subject,
 * 40402 unknown version, 40403 schema not found, 42201 invalid schema (also unknown
 * references and non-PROTOBUF schema types), 42202 invalid version, 42203 invalid
 * compatibility level, 409 incompatible registration. Subject path segments are URL-decoded,
 * so import-path subjects containing slashes round-trip.</p>
 *
 * <p>With {@link SchemaRegistryServerConfig#apiToken()} set, every request except the health
 * endpoint must present the shared secret as an {@code api_token} header or bearer
 * credential — the registry carries writes and action execution, so it sits behind the same
 * boundary as the other operational surfaces.</p>
 */
@ApiStability(StabilityTier.STABLE)
public final class SchemaRegistryServer implements AutoCloseable {

    /** Canonical route ID for {@code GET {healthPath}}. */
    public static final String ROUTE_HEALTH = "schema-registry:health";
    /** Canonical route ID for {@code GET /subjects}. */
    public static final String ROUTE_SUBJECTS_LIST = "schema-registry:subjects.list";
    /** Canonical route ID for {@code POST /subjects/{subject}}. */
    public static final String ROUTE_SUBJECT_LOOKUP = "schema-registry:subject.lookup";
    /** Canonical route ID for {@code GET|POST /subjects/{subject}/versions}. */
    public static final String ROUTE_SUBJECT_VERSIONS = "schema-registry:subject.versions";
    /** Canonical route ID for {@code GET /subjects/{subject}/versions/{version}}. */
    public static final String ROUTE_SUBJECT_VERSION_GET = "schema-registry:subject.version.get";
    /** Canonical route ID for {@code GET /schemas/ids/{id}}. */
    public static final String ROUTE_SCHEMAS_BY_ID = "schema-registry:schemas.by-id";
    /** Canonical route ID for {@code GET|PUT /config}. */
    public static final String ROUTE_CONFIG_GLOBAL = "schema-registry:config.global";
    /** Canonical route ID for {@code GET|PUT /config/{subject}}. */
    public static final String ROUTE_CONFIG_SUBJECT = "schema-registry:config.subject";
    /** Canonical route ID for {@code GET {nativePrefix}/subjects/{subject}/descriptor-set}. */
    public static final String ROUTE_NATIVE_DESCRIPTOR_SET = "schema-registry:native.descriptor-set";
    /** Canonical route ID for {@code GET {nativePrefix}/subjects/{subject}/parquet-schema}. */
    public static final String ROUTE_NATIVE_PARQUET_SCHEMA = "schema-registry:native.parquet-schema";
    /** Canonical route ID for {@code GET {nativePrefix}/configs}. */
    public static final String ROUTE_NATIVE_CONFIGS_LIST = "schema-registry:native.configs.list";
    /** Canonical route ID for {@code GET|PUT {nativePrefix}/configs/{name}}. */
    public static final String ROUTE_NATIVE_CONFIGS_ITEM = "schema-registry:native.configs.item";
    /** Canonical route ID for {@code GET {nativePrefix}/workflows}. */
    public static final String ROUTE_NATIVE_WORKFLOWS_LIST = "schema-registry:native.workflows.list";
    /** Canonical route ID for {@code GET|PUT {nativePrefix}/workflows/{name}}. */
    public static final String ROUTE_NATIVE_WORKFLOWS_ITEM = "schema-registry:native.workflows.item";
    /** Canonical route ID for {@code GET {nativePrefix}/actions}. */
    public static final String ROUTE_NATIVE_ACTIONS_LIST = "schema-registry:native.actions.list";
    /** Canonical route ID for {@code POST {nativePrefix}/actions/{name}}. */
    public static final String ROUTE_NATIVE_ACTIONS_EXECUTE = "schema-registry:native.actions.execute";

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryServer.class);

    private static final String JSON_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    private static final String WORKFLOW_JSON_CONTENT_TYPE = "application/json";
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
    private static final String TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

    /**
     * Declarative HTTP route contract served by {@link SchemaRegistryServer}, shared between
     * live request dispatch and release-baseline compatibility snapshots.
     *
     * @param routeId                 canonical route identifier
     * @param pathTemplate            URI path template
     * @param httpMethods             allowed uppercase HTTP methods
     * @param pathParameters          ordered path parameter names
     * @param requiredQueryParameters required query parameter names
     * @param requestMediaType        expected request media type (or empty when bodyless)
     * @param responseMediaType       produced response media type
     * @param requestType             logical request payload contract name
     * @param responseType            logical response payload contract name
     * @param successStatusCodes      documented HTTP status codes for the endpoint
     * @param authRequired            whether authentication is required on this server configuration
     * @param stability               declared stability tier
     */
    @ApiStability(StabilityTier.STABLE)
    public record RouteContract(
            String routeId,
            String pathTemplate,
            List<String> httpMethods,
            List<String> pathParameters,
            List<String> requiredQueryParameters,
            String requestMediaType,
            String responseMediaType,
            String requestType,
            String responseType,
            List<Integer> successStatusCodes,
            boolean authRequired,
            StabilityTier stability) {

        public RouteContract {
            Objects.requireNonNull(routeId, "routeId");
            Objects.requireNonNull(pathTemplate, "pathTemplate");
            httpMethods = List.copyOf(Objects.requireNonNull(httpMethods, "httpMethods"));
            pathParameters = List.copyOf(Objects.requireNonNull(pathParameters, "pathParameters"));
            requiredQueryParameters = List.copyOf(
                    Objects.requireNonNull(requiredQueryParameters, "requiredQueryParameters"));
            requestMediaType = Objects.requireNonNull(requestMediaType, "requestMediaType");
            responseMediaType = Objects.requireNonNull(responseMediaType, "responseMediaType");
            requestType = Objects.requireNonNull(requestType, "requestType");
            responseType = Objects.requireNonNull(responseType, "responseType");
            successStatusCodes = List.copyOf(
                    Objects.requireNonNull(successStatusCodes, "successStatusCodes"));
            Objects.requireNonNull(stability, "stability");
        }

        boolean matchesSegments(List<String> requestSegments) {
            List<String> templateSegments = templateSegments(pathTemplate);
            if (templateSegments.size() != requestSegments.size()) {
                return false;
            }
            for (int i = 0; i < templateSegments.size(); i++) {
                String segment = templateSegments.get(i);
                if (isTemplateParameter(segment)) {
                    continue;
                }
                if (!segment.equals(requestSegments.get(i))) {
                    return false;
                }
            }
            return true;
        }

        boolean allowsMethod(String method) {
            return httpMethods.contains(method);
        }

        String allowHeader() {
            return String.join(", ", httpMethods);
        }

        private static List<String> templateSegments(String template) {
            List<String> segments = new ArrayList<>();
            for (String segment : splitOn(template, '/')) {
                if (!segment.isEmpty()) {
                    segments.add(segment);
                }
            }
            return segments;
        }

        private static boolean isTemplateParameter(String segment) {
            return segment.length() > 2
                    && segment.charAt(0) == '{'
                    && segment.charAt(segment.length() - 1) == '}';
        }
    }

    /**
     * Returns the immutable list of {@link RouteContract}s served by a
     * {@link SchemaRegistryServer} with the given configuration and action-mount setting.
     *
     * @param config              server configuration supplying {@code healthPath}, {@code nativePathPrefix}, and {@code apiToken}
     * @param includeActionRoutes whether an {@link ActionCatalog} is mounted under {@code {nativePathPrefix}/actions}
     * @return immutable ordered route contracts
     */
    public static List<RouteContract> routeContracts(
            SchemaRegistryServerConfig config,
            boolean includeActionRoutes) {
        Objects.requireNonNull(config, "config");
        boolean authRequired = config.apiToken() != null;
        String nativePrefix = config.nativePathPrefix();

        List<RouteContract> routes = new ArrayList<>(16);
        routes.add(new RouteContract(
                ROUTE_HEALTH,
                config.healthPath(),
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "HealthStatusResponse",
                List.of(200),
                false,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_SUBJECTS_LIST,
                "/subjects",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "SubjectList",
                List.of(200),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_SUBJECT_LOOKUP,
                "/subjects/{subject}",
                List.of("POST"),
                List.of("subject"),
                List.of(),
                JSON_CONTENT_TYPE,
                JSON_CONTENT_TYPE,
                "LookupSchemaRequest",
                "RegisteredSchemaResponse",
                List.of(200, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_SUBJECT_VERSIONS,
                "/subjects/{subject}/versions",
                List.of("GET", "POST"),
                List.of("subject"),
                List.of(),
                JSON_CONTENT_TYPE,
                JSON_CONTENT_TYPE,
                "RegisterSchemaRequest",
                "SubjectVersionsOrIdResponse",
                List.of(200, 404, 409, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_SUBJECT_VERSION_GET,
                "/subjects/{subject}/versions/{version}",
                List.of("GET"),
                List.of("subject", "version"),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "RegisteredSchemaResponse",
                List.of(200, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_SCHEMAS_BY_ID,
                "/schemas/ids/{id}",
                List.of("GET"),
                List.of("id"),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "SchemaByIdResponse",
                List.of(200, 404),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_CONFIG_GLOBAL,
                "/config",
                List.of("GET", "PUT"),
                List.of(),
                List.of(),
                JSON_CONTENT_TYPE,
                JSON_CONTENT_TYPE,
                "CompatibilityConfigRequest",
                "CompatibilityConfigResponse",
                List.of(200, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_CONFIG_SUBJECT,
                "/config/{subject}",
                List.of("GET", "PUT"),
                List.of("subject"),
                List.of(),
                JSON_CONTENT_TYPE,
                JSON_CONTENT_TYPE,
                "CompatibilityConfigRequest",
                "CompatibilityConfigResponse",
                List.of(200, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_DESCRIPTOR_SET,
                nativePrefix + "/subjects/{subject}/descriptor-set",
                List.of("GET"),
                List.of("subject"),
                List.of(),
                "",
                PROTOBUF_CONTENT_TYPE,
                "",
                "google.protobuf.FileDescriptorSet",
                List.of(200, 404),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_PARQUET_SCHEMA,
                nativePrefix + "/subjects/{subject}/parquet-schema",
                List.of("GET"),
                List.of("subject"),
                List.of("message"),
                "",
                TEXT_CONTENT_TYPE,
                "",
                "ParquetMessageTypeText",
                List.of(200, 400, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_CONFIGS_LIST,
                nativePrefix + "/configs",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "ConfigNameList",
                List.of(200, 404),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_CONFIGS_ITEM,
                nativePrefix + "/configs/{name}",
                List.of("GET", "PUT"),
                List.of("name"),
                List.of(),
                JSON_CONTENT_TYPE,
                JSON_CONTENT_TYPE,
                "ConfigEnvelopeRequest",
                "ConfigEnvelopeResponse",
                List.of(200, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_WORKFLOWS_LIST,
                nativePrefix + "/workflows",
                List.of("GET"),
                List.of(),
                List.of(),
                "",
                JSON_CONTENT_TYPE,
                "",
                "WorkflowNameList",
                List.of(200, 404),
                authRequired,
                StabilityTier.STABLE));
        routes.add(new RouteContract(
                ROUTE_NATIVE_WORKFLOWS_ITEM,
                nativePrefix + "/workflows/{name}",
                List.of("GET", "PUT"),
                List.of("name"),
                List.of(),
                JSON_CONTENT_TYPE,
                WORKFLOW_JSON_CONTENT_TYPE,
                "WorkflowDefinitionRequest",
                "WorkflowDefinitionResponse",
                List.of(200, 404, 422),
                authRequired,
                StabilityTier.STABLE));
        if (includeActionRoutes) {
            routes.add(new RouteContract(
                    ROUTE_NATIVE_ACTIONS_LIST,
                    nativePrefix + "/actions",
                    List.of("GET"),
                    List.of(),
                    List.of(),
                    "",
                    JSON_CONTENT_TYPE,
                    "",
                    "ActionListResponse",
                    List.of(200),
                    authRequired,
                    StabilityTier.STABLE));
            routes.add(new RouteContract(
                    ROUTE_NATIVE_ACTIONS_EXECUTE,
                    nativePrefix + "/actions/{name}",
                    List.of("POST"),
                    List.of("name"),
                    List.of(),
                    JSON_CONTENT_TYPE,
                    JSON_CONTENT_TYPE,
                    "ActionExecuteRequest",
                    "ActionExecuteResponse",
                    List.of(200, 400, 403, 404, 422, 429),
                    authRequired,
                    StabilityTier.STABLE));
        }
        return List.copyOf(routes);
    }

    private final SchemaRegistryServerConfig config;
    private final SchemaRegistryStore store;
    private final ActionCatalog actions;
    private final CallerResolver resolver;
    private final RouteContract healthRoute;
    private final List<RouteContract> dispatchRoutes;
    private final ObjectMapper json = new ObjectMapper();
    private final ScopeBudgets budgets;
    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final AtomicReference<HttpServer> httpServer = new AtomicReference<>();
    private volatile ExecutorService executor;

    /**
     * Creates a server with default configuration over {@code store}.
     *
     * @param store backing schema registry store
     */
    public SchemaRegistryServer(SchemaRegistryStore store) {
        this(SchemaRegistryServerConfig.defaults(), store);
    }

    /**
     * Creates a server with the given configuration and backing store.
     *
     * @param config server configuration
     * @param store backing schema registry store
     */
    public SchemaRegistryServer(SchemaRegistryServerConfig config, SchemaRegistryStore store) {
        this(config, store, null);
    }

    /**
     * A server that additionally mounts the given action catalog under
     * {@code {nativePathPrefix}/actions}: {@code GET .../actions} lists the actions with their
     * input schemas; {@code POST .../actions/{name}} executes one with a JSON body. Action
     * failures map to the action error envelope with {@code unknown-action} as 404,
     * {@code invalid-input} as 400, {@code invalid-response} as 500, and other action errors by code.
     *
     * @param config server configuration
     * @param store backing schema registry store
     * @param actions optional action catalog to mount
     */
    public SchemaRegistryServer(SchemaRegistryServerConfig config, SchemaRegistryStore store,
                                ActionCatalog actions) {
        this(config, store, actions, null);
    }

    /**
     * With a resolver, a credential a mounted access policy names authenticates as its
     * principal: reads require {@code schema-read}, writes require {@code schema-write},
     * and the action endpoint dispatches through the scoped catalog so each verb's own
     * declaration applies.
     *
     * @param config server configuration
     * @param store backing schema registry store
     * @param actions optional action catalog to mount
     * @param resolver optional access-policy caller resolver
     */
    public SchemaRegistryServer(SchemaRegistryServerConfig config, SchemaRegistryStore store,
                                ActionCatalog actions,
                                CallerResolver resolver) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.actions = actions;
        List<RouteContract> allRoutes = routeContracts(config, actions != null);
        this.healthRoute = allRoutes.get(0);
        this.dispatchRoutes = allRoutes.subList(1, allRoutes.size());
        // The route table and the mounted catalog are one enforcement boundary: they
        // spend on the catalog's ledger so a caller cannot take its per-scope budget
        // once on a schema route and again on an action.
        this.budgets = actions == null ? new ScopeBudgets() : actions.budgets();
        if (resolver != null && config.apiToken() == null) {
            throw new IllegalArgumentException(
                    "an access-policy resolver requires the operator api token");
        }
        this.resolver = resolver;
    }

    /**
     * Returns the immutable list of {@link RouteContract}s served by this server instance.
     *
     * @return immutable ordered route contracts
     */
    public List<RouteContract> routeContracts() {
        List<RouteContract> all = new ArrayList<>(1 + dispatchRoutes.size());
        all.add(healthRoute);
        all.addAll(dispatchRoutes);
        return List.copyOf(all);
    }

    /**
     * Starts the server and returns the bound port.
     *
     * @return the bound TCP port
     */
    public int start() {
        lifecycleLock.lock();
        try {
            if (httpServer.get() != null) {
                throw new IllegalStateException("Server already started");
            }
            HttpServer server;
            try {
                server = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to start schema registry server", e);
            }
            ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
            try {
                server.createContext("/", this::handle);
                server.setExecutor(workerPool);
                server.start();
            } catch (Throwable t) {
                // The socket is bound after create(); release it before rethrowing.
                server.stop(0);
                workerPool.shutdownNow();
                throw new IllegalStateException("Failed to start schema registry server", t);
            }
            this.executor = workerPool;
            httpServer.set(server);
            return server.getAddress().getPort();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * The bound port (useful with {@code port = 0}).
     *
     * @return the bound TCP port
     */
    public int actualPort() {
        HttpServer server = httpServer.get();
        if (server == null) {
            throw new IllegalStateException("Server not started");
        }
        return server.getAddress().getPort();
    }

    /**
     * Returns the server configuration.
     *
     * @return server configuration
     */
    public SchemaRegistryServerConfig config() {
        return config;
    }

    @Override
    public void close() {
        HttpServer server;
        ExecutorService workerPool;
        lifecycleLock.lock();
        try {
            server = httpServer.getAndSet(null);
            workerPool = executor;
            executor = null;
        } finally {
            lifecycleLock.unlock();
        }
        if (server != null) {
            // Small grace period so in-flight exchanges can finish.
            server.stop(1);
        }
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }

    // ---------------------------------------------------------------- dispatch

    private void handle(HttpExchange exchange) throws IOException {
        try {
            route(exchange);
        } catch (RequestTooLargeException e) {
            writeError(exchange, 413, 413, e.getMessage());
        } catch (Exception e) {
            internalError(exchange, "request handling failed", e);
        }
    }

    /**
     * A 500 never carries backend detail — exception text can name filesystem paths, Git
     * remotes, or upstream credentials. Clients get a correlation id; the id plus the full
     * stack trace go to the server log.
     */
    private void internalError(HttpExchange exchange, String where, Exception e)
            throws IOException {
        String correlationId = java.util.UUID.randomUUID().toString();
        LOG.error("Internal error ({}), correlation id {}", where, correlationId, e);
        writeError(exchange, 500, 50001,
                "Error in the backend datastore (correlation id " + correlationId + ")");
    }

    private void route(HttpExchange exchange) throws Exception {
        String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
        String rawPath = exchange.getRequestURI().getRawPath();
        if (healthRoute.pathTemplate().equals(rawPath)) {
            if (!healthRoute.allowsMethod(method)) {
                methodNotAllowed(exchange, healthRoute.allowHeader());
            } else {
                writeJson(exchange, 200, json.createObjectNode().put("status", "UP"));
            }
            return;
        }
        // The registry holds schema, config, and workflow writes plus action execution:
        // the same credentials that guard gRPC/REST/MCP guard it, health excepted.
        Caller resolved = Caller.operator();
        if (config.apiToken() != null) {
            resolved = callerFor(exchange);
            if (resolved == null) {
                writeError(exchange, 401, 401, "Missing or invalid API token 'api_token'");
                return;
            }
        }
        Caller caller = resolved;
        List<String> segments = decodeSegments(rawPath);
        String nativePrefix = config.nativePathPrefix().substring(1);

        String required = requiredScope(segments, method, nativePrefix);
        if (required != null && !caller.holds(required)) {
            writeError(exchange, 403, 403, "caller '" + caller.name() + "' does not hold '"
                    + required + "', which " + method + " " + rawPath + " requires");
            return;
        }
        if (required != null && caller.budgets().containsKey(required)) {
            // The payload size is the declared body length; a chunked request
            // without one skips the payload cap, never the rate.
            long declared = -1;
            String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
            if (contentLength != null) {
                try {
                    declared = Long.parseLong(contentLength.trim());
                } catch (NumberFormatException e) {
                    declared = -1;
                }
            }
            Optional<String> refusal = budgets.refuse(caller, required, declared);
            if (refusal.isPresent()) {
                writeError(exchange, 429, 429, refusal.get());
                return;
            }
        }

        RouteContract matched = findMatchingRoute(segments);
        if (matched == null) {
            writeError(exchange, 404, 404, "HTTP 404 Not Found");
            return;
        }
        if (!matched.allowsMethod(method)) {
            methodNotAllowed(exchange, matched.allowHeader());
            return;
        }
        switch (matched.routeId()) {
            case ROUTE_SUBJECTS_LIST -> listSubjects(exchange);
            case ROUTE_SUBJECT_LOOKUP -> lookupByContent(exchange, segments.get(1));
            case ROUTE_SUBJECT_VERSIONS -> {
                if ("GET".equals(method)) {
                    listVersions(exchange, segments.get(1));
                } else {
                    register(exchange, segments.get(1));
                }
            }
            case ROUTE_SUBJECT_VERSION_GET -> getVersion(exchange, segments.get(1), segments.get(3));
            case ROUTE_SCHEMAS_BY_ID -> getById(exchange, segments.get(2));
            case ROUTE_CONFIG_GLOBAL -> {
                if ("GET".equals(method)) {
                    writeJson(exchange, 200, json.createObjectNode()
                            .put("compatibilityLevel", store.globalCompatibilityMode()));
                } else {
                    putGlobalConfig(exchange);
                }
            }
            case ROUTE_CONFIG_SUBJECT -> {
                if ("GET".equals(method)) {
                    getSubjectConfig(exchange, segments.get(1));
                } else {
                    putSubjectConfig(exchange, segments.get(1));
                }
            }
            case ROUTE_NATIVE_DESCRIPTOR_SET -> descriptorSet(exchange, segments.get(2));
            case ROUTE_NATIVE_PARQUET_SCHEMA -> parquetSchema(exchange, segments.get(2));
            case ROUTE_NATIVE_CONFIGS_LIST -> listConfigs(exchange);
            case ROUTE_NATIVE_CONFIGS_ITEM -> {
                if ("GET".equals(method)) {
                    getConfig(exchange, segments.get(2));
                } else {
                    putConfig(exchange, segments.get(2));
                }
            }
            case ROUTE_NATIVE_WORKFLOWS_LIST -> listWorkflows(exchange);
            case ROUTE_NATIVE_WORKFLOWS_ITEM -> {
                if ("GET".equals(method)) {
                    getWorkflow(exchange, segments.get(2));
                } else {
                    putWorkflow(exchange, segments.get(2));
                }
            }
            case ROUTE_NATIVE_ACTIONS_LIST -> writeJson(exchange, 200, actions.list(caller));
            case ROUTE_NATIVE_ACTIONS_EXECUTE -> executeAction(exchange, segments.get(2), caller);
            default -> writeError(exchange, 404, 404, "HTTP 404 Not Found");
        }
    }

    private RouteContract findMatchingRoute(List<String> segments) {
        for (RouteContract candidate : dispatchRoutes) {
            if (candidate.matchesSegments(segments)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * The scope a route requires, or null when the dispatched action's own declaration
     * applies (the action endpoint). Reads are {@code schema-read}, including the
     * POST-shaped content lookup; every mutation is {@code schema-write}.
     */
    private static String requiredScope(List<String> segments, String method,
                                        String nativePrefix) {
        if (segments.size() >= 2 && segments.get(0).equals(nativePrefix)
                && segments.get(1).equals("actions")) {
            return null;
        }
        if ("GET".equals(method)) {
            return Scopes.SCHEMA_READ;
        }
        if ("POST".equals(method) && segments.size() == 2 && segments.get(0).equals("subjects")) {
            return Scopes.SCHEMA_READ;
        }
        return Scopes.SCHEMA_WRITE;
    }

    /**
     * The caller the request authenticates as: the operator token with process authority,
     * a credential the access policy names as its principal, anything else null (the
     * refusal carries nothing an attacker could use to recover a credential).
     */
    private Caller callerFor(HttpExchange exchange) {
        String presented = exchange.getRequestHeaders().getFirst("api_token");
        if (presented == null) {
            String authorization = exchange.getRequestHeaders().getFirst("authorization");
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        if (presented == null || presented.isBlank()) {
            return null;
        }
        if (java.security.MessageDigest.isEqual(
                config.apiToken().getBytes(StandardCharsets.UTF_8),
                presented.getBytes(StandardCharsets.UTF_8))) {
            return Caller.operator();
        }
        return resolver == null ? null : resolver.resolve(presented).orElse(null);
    }

    // ---------------------------------------------------------------- subjects protocol

    private void listSubjects(HttpExchange exchange) throws IOException {
        ArrayNode array = json.createArrayNode();
        store.subjects().forEach(array::add);
        writeJson(exchange, 200, array);
    }

    private void listVersions(HttpExchange exchange, String subject) throws IOException {
        List<Integer> versions = store.versions(subject);
        if (versions.isEmpty()) {
            subjectNotFound(exchange, subject);
            return;
        }
        ArrayNode array = json.createArrayNode();
        versions.forEach(array::add);
        writeJson(exchange, 200, array);
    }

    private void getVersion(HttpExchange exchange, String subject, String versionSpec)
            throws IOException {
        if (store.versions(subject).isEmpty()) {
            subjectNotFound(exchange, subject);
            return;
        }
        Optional<StoredSchema> schema;
        if ("latest".equals(versionSpec)) {
            schema = store.latest(subject);
        } else {
            int version;
            try {
                version = Integer.parseInt(versionSpec);
            } catch (NumberFormatException e) {
                writeError(exchange, 422, 42202, "The specified version '" + versionSpec
                        + "' is not a valid version id.");
                return;
            }
            schema = store.version(subject, version);
        }
        if (schema.isEmpty()) {
            writeError(exchange, 404, 40402, "Version " + versionSpec + " not found.");
            return;
        }
        writeJson(exchange, 200, versionEnvelope(schema.get()));
    }

    private void register(HttpExchange exchange, String subject) throws IOException {
        JsonNode body = readJsonBody(exchange);
        if (body == null) {
            writeError(exchange, 422, 42201, "Invalid schema: request body is not JSON");
            return;
        }
        String schemaType = body.path("schemaType").asText("PROTOBUF");
        if (!"PROTOBUF".equals(schemaType)) {
            writeError(exchange, 422, 42201,
                    "Invalid schema type " + schemaType + "; this registry serves only PROTOBUF");
            return;
        }
        String schema = body.path("schema").asText("");
        if (schema.isBlank()) {
            writeError(exchange, 422, 42201, "Invalid schema: empty schema");
            return;
        }
        List<SchemaReference> references;
        try {
            references = parseReferences(body);
        } catch (IllegalArgumentException e) {
            writeError(exchange, 422, 42201, "Invalid schema references: " + e.getMessage());
            return;
        }
        try {
            StoredSchema stored = store.register(subject, schema, references);
            writeJson(exchange, 200, json.createObjectNode().put("id", stored.globalId()));
        } catch (IncompatibleRegistrationException e) {
            writeError(exchange, 409, 409,
                    "Schema being registered is incompatible with an earlier schema: "
                            + String.join("; ", e.violations()));
        } catch (ReferenceNotFoundException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        } catch (ai.protomolt.proto.registry.ReferenceConflictException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        } catch (InvalidSchemaException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        }
    }

    private void lookupByContent(HttpExchange exchange, String subject) throws IOException {
        if (store.versions(subject).isEmpty()) {
            subjectNotFound(exchange, subject);
            return;
        }
        JsonNode body = readJsonBody(exchange);
        if (body == null) {
            writeError(exchange, 422, 42201, "Invalid schema: request body is not JSON");
            return;
        }
        List<SchemaReference> references;
        try {
            references = parseReferences(body);
        } catch (IllegalArgumentException e) {
            writeError(exchange, 422, 42201, "Invalid schema references: " + e.getMessage());
            return;
        }
        Optional<StoredSchema> match =
                store.findByContent(subject, body.path("schema").asText(""), references);
        if (match.isEmpty()) {
            writeError(exchange, 404, 40403, "Schema not found");
            return;
        }
        writeJson(exchange, 200, versionEnvelope(match.get()));
    }

    private void getById(HttpExchange exchange, String idSpec) throws IOException {
        Optional<StoredSchema> schema;
        try {
            schema = store.byGlobalId(Integer.parseInt(idSpec));
        } catch (NumberFormatException e) {
            schema = Optional.empty();
        }
        if (schema.isEmpty()) {
            writeError(exchange, 404, 40403, "Schema " + idSpec + " not found");
            return;
        }
        ObjectNode node = json.createObjectNode()
                .put("schema", schema.get().schemaText())
                .put("schemaType", "PROTOBUF");
        appendReferences(node, schema.get());
        writeJson(exchange, 200, node);
    }

    // ---------------------------------------------------------------- config protocol

    private void putGlobalConfig(HttpExchange exchange) throws IOException {
        String mode = compatibilityFromBody(exchange);
        if (mode == null) {
            return;
        }
        store.setGlobalCompatibilityMode(mode);
        // Confluent quirk: PUT echoes "compatibility"; GET responds with "compatibilityLevel".
        writeJson(exchange, 200, json.createObjectNode().put("compatibility", mode));
    }

    private void getSubjectConfig(HttpExchange exchange, String subject) throws IOException {
        Optional<String> mode = store.compatibilityMode(subject);
        if (mode.isEmpty()) {
            writeError(exchange, 404, 40408,
                    "Subject '" + subject + "' does not have subject-level compatibility configured");
            return;
        }
        writeJson(exchange, 200, json.createObjectNode().put("compatibilityLevel", mode.get()));
    }

    private void putSubjectConfig(HttpExchange exchange, String subject) throws IOException {
        String mode = compatibilityFromBody(exchange);
        if (mode == null) {
            return;
        }
        store.setCompatibilityMode(subject, mode);
        writeJson(exchange, 200, json.createObjectNode().put("compatibility", mode));
    }

    /** Reads and validates the {@code {"compatibility": …}} PUT body; null = error written. */
    private String compatibilityFromBody(HttpExchange exchange) throws IOException {
        JsonNode body = readJsonBody(exchange);
        String mode = body == null ? null : body.path("compatibility").asText(null);
        if (!CompatibilityModes.isValid(mode)) {
            writeError(exchange, 422, 42203, "Invalid compatibility level: " + mode);
            return null;
        }
        return mode;
    }

    // ---------------------------------------------------------------- native extras

    private void listWorkflows(HttpExchange exchange) throws IOException {
        if (!(store instanceof WorkflowDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold workflows");
            return;
        }
        try {
            writeJson(exchange, 200, json.valueToTree(documentStore.workflows()));
        } catch (Exception e) {
            internalError(exchange, "listing workflows", e);
        }
    }

    // ---------------------------------------------------------------- config documents

    private void listConfigs(HttpExchange exchange) throws IOException {
        if (!(store instanceof ConfigDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold configs");
            return;
        }
        ArrayNode array = json.createArrayNode();
        documentStore.configs().forEach(array::add);
        writeJson(exchange, 200, array);
    }

    /**
     * One config document, gated on the way out too: the envelope is
     * re-parsed and re-checked against the registered type's declared
     * rules, so even a hand-edited repository never serves an invalid
     * document — it serves a refusal an operator can read.
     */
    private void getConfig(HttpExchange exchange, String name) throws IOException {
        if (!(store instanceof ConfigDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold configs");
            return;
        }
        try {
            var envelope = documentStore.config(name);
            if (envelope.isEmpty()) {
                writeError(exchange, 404, 40401, "Config not found: " + name);
                return;
            }
            ConfigSupport.Gated gated;
            try {
                gated = ConfigSupport.gate(store, envelope.get());
            } catch (InvalidConfigException e) {
                writeError(exchange, 422, 42202,
                        "Stored config '" + name + "' no longer gates: " + e.getMessage());
                return;
            }
            ObjectNode body = json.createObjectNode();
            body.put("name", name);
            body.put("messageType", gated.messageType());
            body.put("version", documentStore.configVersion(name).orElse(""));
            body.put("payloadBase64", java.util.Base64.getEncoder()
                    .encodeToString(gated.message().toByteArray()));
            body.set("config", json.readTree(envelope.get()).path(ConfigSupport.CONFIG));
            writeJson(exchange, 200, body);
        } catch (IllegalArgumentException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        }
    }

    private void putConfig(HttpExchange exchange, String name) throws IOException {
        if (!(store instanceof ConfigDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold configs");
            return;
        }
        JsonNode body = readJsonBody(exchange);
        if (!(body instanceof ObjectNode envelope)) {
            writeError(exchange, 422, 42201, "The body must be a config envelope object");
            return;
        }
        try {
            ConfigSupport.gate(store, envelope.toString());
            String version = documentStore.putConfig(name, envelope.toString());
            ObjectNode response = json.createObjectNode();
            response.put("name", name);
            response.put("version", version);
            writeJson(exchange, 200, response);
        } catch (InvalidConfigException e) {
            writeError(exchange, 422, 42202, e.getMessage());
        } catch (IllegalArgumentException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        }
    }

    private void getWorkflow(HttpExchange exchange, String name) throws IOException {
        if (!(store instanceof WorkflowDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold workflows");
            return;
        }
        try {
            var workflow = documentStore.workflow(name);
            if (workflow.isEmpty()) {
                writeError(exchange, 404, 40401, "Workflow not found: " + name);
                return;
            }
            // The stored workflow is already JSON; it is written verbatim rather than reparsed.
            writeBytes(exchange, 200, "application/json",
                    workflow.get().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            internalError(exchange, "reading a workflow", e);
        }
    }

    /**
     * Stores a workflow, gated the way schema writes are compatibility-gated: when the action
     * catalog is mounted, {@code check-workflow} must pass before anything is committed.
     */
    private void putWorkflow(HttpExchange exchange, String name) throws IOException {
        if (!(store instanceof WorkflowDocumentStore documentStore)) {
            writeError(exchange, 404, 40401, "This store does not hold workflows");
            return;
        }
        JsonNode body = readJsonBody(exchange);
        if (!(body instanceof ObjectNode workflow)) {
            writeError(exchange, 422, 42201, "The body must be a workflow definition object");
            return;
        }
        if (actions != null) {
            try {
                ObjectNode request = json.createObjectNode();
                request.set("workflow", workflow);
                ObjectNode checked = actions.execute("check-workflow", request);
                if (!checked.path("ok").asBoolean()) {
                    ObjectNode error = json.createObjectNode();
                    error.put("error_code", 42202);
                    error.put("message", "Workflow does not verify");
                    error.set("findings", checked.get("findings"));
                    writeJson(exchange, 422, error);
                    return;
                }
            } catch (ActionException e) {
                writeError(exchange, 422, 42202, "Workflow does not verify: " + e.getMessage());
                return;
            }
        }
        try {
            documentStore.putWorkflow(name, workflow.toString());
            writeJson(exchange, 200, json.createObjectNode().put("name", name));
        } catch (IllegalArgumentException e) {
            writeError(exchange, 422, 42201, e.getMessage());
        } catch (Exception e) {
            internalError(exchange, "storing a workflow", e);
        }
    }

    private void descriptorSet(HttpExchange exchange, String subject) throws IOException {
        Optional<StoredSchema> latest = store.latest(subject);
        if (latest.isEmpty()) {
            subjectNotFound(exchange, subject);
            return;
        }
        CompiledProtos compiled;
        try {
            compiled = compiler.compile(StoredSchemaSources.resolve(store, latest.get()).sources());
        } catch (Exception e) {
            // Registered schemas are compile-verified; failure here is a store inconsistency.
            internalError(exchange, "compiling subject " + subject, e);
            return;
        }
        writeBytes(exchange, 200, PROTOBUF_CONTENT_TYPE,
                topologicallyOrdered(compiled.descriptorSet()).toByteArray());
    }

    /**
     * The Parquet schema of one message of the subject, derived on read from the compiled
     * descriptors and never stored. {@code message} (a fully qualified message name) is
     * required because a subject's descriptor set can hold many messages; {@code version}
     * pins the schema version and defaults to latest.
     */
    private void parquetSchema(HttpExchange exchange, String subject) throws IOException {
        Map<String, String> query = queryParams(exchange);
        String message = query.get("message");
        if (message == null || message.isBlank()) {
            writeError(exchange, 400, 400,
                    "The 'message' query parameter is required (a fully qualified message name)");
            return;
        }
        if (store.versions(subject).isEmpty()) {
            subjectNotFound(exchange, subject);
            return;
        }
        String versionSpec = query.getOrDefault("version", "latest");
        Optional<StoredSchema> schema;
        if ("latest".equals(versionSpec)) {
            schema = store.latest(subject);
        } else {
            int version;
            try {
                version = Integer.parseInt(versionSpec);
            } catch (NumberFormatException e) {
                writeError(exchange, 422, 42202, "The specified version '" + versionSpec
                        + "' is not a valid version id.");
                return;
            }
            schema = store.version(subject, version);
        }
        if (schema.isEmpty()) {
            writeError(exchange, 404, 40402, "Version " + versionSpec + " not found.");
            return;
        }
        CompiledProtos compiled;
        try {
            compiled = compiler.compile(StoredSchemaSources.resolve(store, schema.get()).sources());
        } catch (Exception e) {
            // Registered schemas are compile-verified; failure here is a store inconsistency.
            internalError(exchange, "compiling subject " + subject, e);
            return;
        }
        Descriptor descriptor = findMessage(compiled, message);
        if (descriptor == null) {
            writeError(exchange, 404, 40403, "Message '" + message
                    + "' is not defined by subject '" + subject + "'.");
            return;
        }
        MessageType parquet;
        try {
            parquet = ProtoParquetSchemas.schema(descriptor);
        } catch (IllegalArgumentException e) {
            // A valid protobuf schema can still refuse columnar form (recursive types).
            writeError(exchange, 422, 42201, e.getMessage());
            return;
        }
        writeBytes(exchange, 200, TEXT_CONTENT_TYPE,
                parquet.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** The message with the given fully qualified name, nested types included. */
    private static Descriptor findMessage(CompiledProtos compiled, String fullName) {
        for (FileDescriptor file : compiled.fileDescriptors()) {
            Descriptor found = findMessage(file.getMessageTypes(), fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor findMessage(List<Descriptor> messages, String fullName) {
        for (Descriptor message : messages) {
            if (message.getFullName().equals(fullName)) {
                return message;
            }
            Descriptor nested = findMessage(message.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    /** Decoded query parameters; the last value wins on a repeated name. */
    private static Map<String, String> queryParams(HttpExchange exchange) {
        Map<String, String> params = new LinkedHashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null) {
            return params;
        }
        for (String pair : splitOn(raw, '&')) {
            int equals = pair.indexOf('=');
            String name = equals < 0 ? pair : pair.substring(0, equals);
            String value = equals < 0 ? "" : pair.substring(equals + 1);
            params.put(URLDecoder.decode(name, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return params;
    }

    /**
     * Dependencies before dependents, so consumers can link the set in one forward pass
     * (bufbuild, protoc, and most descriptor tooling expect this).
     */
    static com.google.protobuf.DescriptorProtos.FileDescriptorSet topologicallyOrdered(
            com.google.protobuf.DescriptorProtos.FileDescriptorSet set) {
        java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> byName =
                new java.util.LinkedHashMap<>();
        set.getFileList().forEach(file -> byName.put(file.getName(), file));
        java.util.LinkedHashSet<String> ordered = new java.util.LinkedHashSet<>();
        byName.keySet().forEach(name -> visitFile(name, byName, ordered, new java.util.HashSet<>()));
        com.google.protobuf.DescriptorProtos.FileDescriptorSet.Builder out =
                com.google.protobuf.DescriptorProtos.FileDescriptorSet.newBuilder();
        ordered.forEach(name -> out.addFile(byName.get(name)));
        return out.build();
    }

    private static void visitFile(String name,
                                  java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> byName,
                                  java.util.LinkedHashSet<String> ordered,
                                  java.util.Set<String> visiting) {
        if (ordered.contains(name) || !byName.containsKey(name) || !visiting.add(name)) {
            return;
        }
        for (String dependency : byName.get(name).getDependencyList()) {
            visitFile(dependency, byName, ordered, visiting);
        }
        ordered.add(name);
    }

    // ---------------------------------------------------------------- protocol JSON

    private ObjectNode versionEnvelope(StoredSchema schema) {
        ObjectNode node = json.createObjectNode()
                .put("subject", schema.subject())
                .put("id", schema.globalId())
                .put("version", schema.version())
                .put("schemaType", "PROTOBUF");
        appendReferences(node, schema);
        node.put("schema", schema.schemaText());
        return node;
    }

    /** Adds the references array — omitted entirely when empty, matching Confluent. */
    private void appendReferences(ObjectNode node, StoredSchema schema) {
        if (schema.references().isEmpty()) {
            return;
        }
        ArrayNode array = node.putArray("references");
        for (SchemaReference reference : schema.references()) {
            array.addObject()
                    .put("name", reference.name())
                    .put("subject", reference.subject())
                    .put("version", reference.version());
        }
    }

    private static List<SchemaReference> parseReferences(JsonNode body) {
        List<SchemaReference> references = new ArrayList<>();
        for (JsonNode reference : body.path("references")) {
            references.add(new SchemaReference(
                    reference.path("name").asText(),
                    reference.path("subject").asText(),
                    reference.path("version").asInt()));
        }
        return references;
    }

    // ---------------------------------------------------------------- actions mount

    private void executeAction(HttpExchange exchange, String name, Caller caller)
            throws IOException {
        JsonNode body = readJsonBody(exchange);
        if (body == null || !body.isObject()) {
            writeError(exchange, 400, 400, "Action input must be a JSON object");
            return;
        }
        try {
            writeJson(exchange, 200, actions.execute(name, (ObjectNode) body, caller));
        } catch (ActionException e) {
            int status = switch (e.code()) {
                case "unknown-action" -> 404;
                case "invalid-input" -> 400;
                case "invalid-response" -> 500;
                case "permission-denied" -> 403;
                case "resource-exhausted" -> 429;
                default -> 422;
            };
            writeJson(exchange, status, e.toJson(json));
        }
    }

    // ---------------------------------------------------------------- HTTP plumbing

    private static final class RequestTooLargeException extends RuntimeException {
        RequestTooLargeException(int limit) {
            super("Request body exceeds " + limit + " bytes");
        }
    }

    private JsonNode readJsonBody(HttpExchange exchange) throws IOException {
        byte[] body = readBody(exchange);
        if (body.length == 0) {
            return null;
        }
        try {
            return json.readTree(body);
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (out.size() + read > config.maxRequestBytes()) {
                    throw new RequestTooLargeException(config.maxRequestBytes());
                }
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    private static List<String> decodeSegments(String rawPath) {
        List<String> segments = new ArrayList<>();
        for (String segment : splitOn(rawPath, '/')) {
            if (!segment.isEmpty()) {
                segments.add(URLDecoder.decode(segment, StandardCharsets.UTF_8));
            }
        }
        return segments;
    }

    /**
     * {@link String#split(String)} (limit 0) on one literal separator, without regex: an
     * empty input is one empty token, trailing empty tokens are dropped, leading and
     * interior empty tokens are kept.
     */
    static List<String> splitOn(String text, char separator) {
        if (text.isEmpty()) {
            return List.of("");
        }
        List<String> tokens = new ArrayList<>();
        int start = 0;
        int next;
        while ((next = text.indexOf(separator, start)) >= 0) {
            tokens.add(text.substring(start, next));
            start = next + 1;
        }
        tokens.add(text.substring(start));
        int end = tokens.size();
        while (end > 0 && tokens.get(end - 1).isEmpty()) {
            end--;
        }
        return tokens.subList(0, end);
    }

    private void methodNotAllowed(HttpExchange exchange, String allow) throws IOException {
        exchange.getResponseHeaders().set("Allow", allow);
        writeError(exchange, 405, 405, "HTTP 405 Method Not Allowed");
    }

    private void subjectNotFound(HttpExchange exchange, String subject) throws IOException {
        writeError(exchange, 404, 40401, "Subject '" + subject + "' not found.");
    }

    private void writeError(HttpExchange exchange, int status, int errorCode, String message)
            throws IOException {
        writeJson(exchange, status, json.createObjectNode()
                .put("error_code", errorCode)
                .put("message", message));
    }

    private void writeJson(HttpExchange exchange, int status, JsonNode body) throws IOException {
        writeBytes(exchange, status, JSON_CONTENT_TYPE,
                body.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
