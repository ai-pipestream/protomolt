package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.registry.CompatibilityModes;
import ai.pipestream.proto.registry.IncompatibleRegistrationException;
import ai.pipestream.proto.registry.InvalidSchemaException;
import ai.pipestream.proto.registry.ReferenceNotFoundException;
import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import ai.pipestream.proto.registry.StoredSchemaSources;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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
 *       {@code FileDescriptorSet} of the subject's latest schema plus transitive references)
 *       and {@code GET /health}</li>
 * </ul>
 *
 * <p>Errors are Confluent-style {@code {error_code, message}} JSON: 40401 unknown subject,
 * 40402 unknown version, 40403 schema not found, 42201 invalid schema (also unknown
 * references and non-PROTOBUF schema types), 42202 invalid version, 42203 invalid
 * compatibility level, 409 incompatible registration. Subject path segments are URL-decoded,
 * so import-path subjects containing slashes round-trip.</p>
 */
public final class SchemaRegistryServer implements AutoCloseable {

    private static final String JSON_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    private final SchemaRegistryServerConfig config;
    private final SchemaRegistryStore store;
    private final ObjectMapper json = new ObjectMapper();
    private final ProtoSourceCompiler compiler = new ProtoSourceCompiler();
    private final AtomicReference<HttpServer> httpServer = new AtomicReference<>();
    private volatile ExecutorService executor;

    public SchemaRegistryServer(SchemaRegistryStore store) {
        this(SchemaRegistryServerConfig.defaults(), store);
    }

    public SchemaRegistryServer(SchemaRegistryServerConfig config, SchemaRegistryStore store) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Starts the server and returns the bound port. */
    public int start() {
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
    }

    /** The bound port (useful with {@code port = 0}). */
    public int actualPort() {
        HttpServer server = httpServer.get();
        if (server == null) {
            throw new IllegalStateException("Server not started");
        }
        return server.getAddress().getPort();
    }

    public SchemaRegistryServerConfig config() {
        return config;
    }

    @Override
    public void close() {
        HttpServer server = httpServer.getAndSet(null);
        if (server != null) {
            // Small grace period so in-flight exchanges can finish.
            server.stop(1);
        }
        ExecutorService workerPool = executor;
        executor = null;
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
            writeError(exchange, 500, 50001, "Error in the backend datastore: " + e.getMessage());
        }
    }

    private void route(HttpExchange exchange) throws Exception {
        String method = exchange.getRequestMethod().toUpperCase();
        String rawPath = exchange.getRequestURI().getRawPath();
        if (config.healthPath().equals(rawPath)) {
            requireMethod(exchange, method, "GET", () -> writeJson(exchange, 200,
                    json.createObjectNode().put("status", "UP")));
            return;
        }
        List<String> segments = decodeSegments(rawPath);
        String nativePrefix = config.nativePathPrefix().substring(1);

        if (matches(segments, "subjects")) {
            requireMethod(exchange, method, "GET", () -> listSubjects(exchange));
        } else if (segments.size() == 2 && segments.get(0).equals("subjects")) {
            requireMethod(exchange, method, "POST",
                    () -> lookupByContent(exchange, segments.get(1)));
        } else if (segments.size() == 3 && segments.get(0).equals("subjects")
                && segments.get(2).equals("versions")) {
            switch (method) {
                case "GET" -> listVersions(exchange, segments.get(1));
                case "POST" -> register(exchange, segments.get(1));
                default -> methodNotAllowed(exchange, "GET, POST");
            }
        } else if (segments.size() == 4 && segments.get(0).equals("subjects")
                && segments.get(2).equals("versions")) {
            requireMethod(exchange, method, "GET",
                    () -> getVersion(exchange, segments.get(1), segments.get(3)));
        } else if (segments.size() == 3 && segments.get(0).equals("schemas")
                && segments.get(1).equals("ids")) {
            requireMethod(exchange, method, "GET", () -> getById(exchange, segments.get(2)));
        } else if (matches(segments, "config")) {
            switch (method) {
                case "GET" -> writeJson(exchange, 200, json.createObjectNode()
                        .put("compatibilityLevel", store.globalCompatibilityMode()));
                case "PUT" -> putGlobalConfig(exchange);
                default -> methodNotAllowed(exchange, "GET, PUT");
            }
        } else if (segments.size() == 2 && segments.get(0).equals("config")) {
            switch (method) {
                case "GET" -> getSubjectConfig(exchange, segments.get(1));
                case "PUT" -> putSubjectConfig(exchange, segments.get(1));
                default -> methodNotAllowed(exchange, "GET, PUT");
            }
        } else if (segments.size() == 4 && segments.get(0).equals(nativePrefix)
                && segments.get(1).equals("subjects") && segments.get(3).equals("descriptor-set")) {
            requireMethod(exchange, method, "GET", () -> descriptorSet(exchange, segments.get(2)));
        } else {
            writeError(exchange, 404, 404, "HTTP 404 Not Found");
        }
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
            writeError(exchange, 500, 50001, "Failed to compile subject " + subject
                    + ": " + e.getMessage());
            return;
        }
        writeBytes(exchange, 200, PROTOBUF_CONTENT_TYPE, compiled.descriptorSet().toByteArray());
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

    private static boolean matches(List<String> segments, String only) {
        return segments.size() == 1 && segments.get(0).equals(only);
    }

    private static List<String> decodeSegments(String rawPath) {
        List<String> segments = new ArrayList<>();
        for (String segment : rawPath.split("/")) {
            if (!segment.isEmpty()) {
                segments.add(URLDecoder.decode(segment, StandardCharsets.UTF_8));
            }
        }
        return segments;
    }

    @FunctionalInterface
    private interface Handler {
        void run() throws IOException;
    }

    private void requireMethod(HttpExchange exchange, String actual, String expected, Handler handler)
            throws IOException {
        if (expected.equals(actual)) {
            handler.run();
        } else {
            methodNotAllowed(exchange, expected);
        }
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
