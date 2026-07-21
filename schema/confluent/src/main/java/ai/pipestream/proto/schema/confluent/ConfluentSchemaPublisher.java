package ai.pipestream.proto.schema.confluent;

import ai.pipestream.proto.sources.ProtoImports;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.sources.publish.SchemaPublishException;
import ai.pipestream.proto.sources.publish.SchemaPublisher;
import ai.pipestream.proto.sources.publish.SubjectNamingStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Publishes a {@link ProtoSourceSet} to a Confluent Schema Registry (or any compatible
 * endpoint, such as Apicurio Registry's ccompat facade) by speaking the subjects REST API —
 * the write-side counterpart of {@link ConfluentSchemaRegistryLoader}.
 *
 * <h2>Protocol</h2>
 * <p>Files are registered in reverse-topological import order
 * ({@link ProtoSourceSet#topologicalOrder()}), so every schema reference exists before the
 * file that declares the import. For each file:</p>
 * <ol>
 *   <li>A references array is built from the file's {@code import} statements
 *       ({@code google/protobuf/*} well-known imports are skipped — registries do not hold
 *       WKTs). Each reference's {@code name} is the import path, its {@code subject} comes
 *       from the {@link SubjectNamingStrategy}, and its {@code version} is the version
 *       registered for that import earlier in the run (or the registry's current latest when
 *       the import is not part of the source set).</li>
 *   <li>Idempotency check: {@code POST /subjects/{subject}} (the schema lookup endpoint) with
 *       the candidate schema. HTTP 200 means identical content is already registered —
 *       {@link Action#UNCHANGED}, no write.</li>
 *   <li>Otherwise {@code POST /subjects/{subject}/versions} registers the schema
 *       ({@link Action#CREATED} for a brand-new subject, {@link Action#UPDATED} for a new
 *       version). In {@link PublishOptions#dryRun() dry-run} mode the write is skipped and the
 *       file is reported as {@link Action#WOULD_WRITE}.</li>
 * </ol>
 *
 * <h2>Failure handling</h2>
 * <p>Per-schema rejections (HTTP 409 incompatible, HTTP 422 invalid) become
 * {@link Action#FAILED} outcomes carrying the registry's response body, and the remaining
 * files are still attempted; a file whose import failed earlier in the run fails too, naming
 * the import. Registry-level failures (HTTP 401/403, 5xx, connection failures) abort the whole
 * publish with a {@link SchemaPublishException}, mirroring
 * {@code ConfluentSchemaRegistryLoader}'s failure classification.</p>
 *
 * <p>Like the loader, the publisher talks anonymous HTTP via {@link HttpClient} with
 * configurable timeouts and owns its client; call {@link #close()} when done.</p>
 */
public final class ConfluentSchemaPublisher implements SchemaPublisher, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaPublisher.class);

    private static final String SCHEMA_REGISTRY_ACCEPT =
            "application/vnd.schemaregistry.v1+json, application/json";
    private static final String SCHEMA_REGISTRY_CONTENT_TYPE =
            "application/vnd.schemaregistry.v1+json";
    private static final String WELL_KNOWN_PREFIX = "google/protobuf/";

    /** Confluent error code for "subject not found" on a 404 response. */
    private static final int ERROR_SUBJECT_NOT_FOUND = 40401;

    /** Default connect timeout for the HTTP client. */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    /** Default per-request timeout. */
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final URI baseUrl;
    private final HttpClient client;
    private final Duration requestTimeout;
    private final ObjectMapper json = new ObjectMapper();

    /**
     * Creates a publisher for the given registry base URL, e.g. {@code http://localhost:8081}
     * or {@code http://localhost:8080/apis/ccompat/v7} (no trailing slash required), with
     * default timeouts ({@link #DEFAULT_CONNECT_TIMEOUT} / {@link #DEFAULT_REQUEST_TIMEOUT}).
     *
     * @param baseUrl base URL of the Confluent-compatible registry
     */
    public ConfluentSchemaPublisher(URI baseUrl) {
        this(baseUrl, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Creates a publisher with explicit HTTP timeouts.
     *
     * @param baseUrl base URL of the Confluent-compatible registry
     * @param connectTimeout TCP connect timeout for the underlying {@link HttpClient}
     * @param requestTimeout per-request timeout applied to every registry call
     */
    public ConfluentSchemaPublisher(URI baseUrl, Duration connectTimeout, Duration requestTimeout) {
        String url = Objects.requireNonNull(baseUrl, "baseUrl").toString();
        this.baseUrl = URI.create(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.client = HttpClient.newBuilder()
                .connectTimeout(Objects.requireNonNull(connectTimeout, "connectTimeout"))
                .build();
    }

    @Override
    public PublishResult publish(ProtoSourceSet sources, PublishOptions options)
            throws SchemaPublishException {
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(options, "options");
        PublishRun run = new PublishRun(options);
        List<FileOutcome> outcomes = new ArrayList<>(sources.size());
        for (String path : sources.topologicalOrder()) {
            String content = sources.get(path).orElseThrow().content();
            FileOutcome outcome = run.publishFile(path, content);
            if (outcome.action() == Action.FAILED) {
                LOG.warn("Publishing {} to {} failed: {}", path, baseUrl, outcome.detail());
            }
            outcomes.add(outcome);
        }
        return new PublishResult(outcomes);
    }

    @Override
    public String target() {
        return "confluent:" + baseUrl;
    }

    /** Closes the underlying {@link HttpClient}. */
    @Override
    public void close() {
        client.close();
    }

    // ---------------------------------------------------------------- publish state machine

    /** State shared across one {@link #publish} invocation. */
    private final class PublishRun {

        private final PublishOptions options;
        /** import path -> version registered (or found) for it during this run. */
        private final Map<String, Integer> versions = new HashMap<>();
        /** Dry run only: paths that would have been written (their versions are unknown). */
        private final Set<String> pendingPaths = new HashSet<>();
        /** Paths whose publish failed; files importing them fail too. */
        private final Set<String> failedPaths = new HashSet<>();

        PublishRun(PublishOptions options) {
            this.options = options;
        }

        FileOutcome publishFile(String path, String content) throws SchemaPublishException {
            String subject = options.naming().subjectFor(path);

            List<ReferenceSpec> references = new ArrayList<>();
            boolean pendingReference = false;
            for (String importPath : ProtoImports.of(content)) {
                if (importPath.startsWith(WELL_KNOWN_PREFIX)) {
                    continue; // well-known types are compiled in, never registered
                }
                if (failedPaths.contains(importPath)) {
                    failedPaths.add(path);
                    return new FileOutcome(path, subject, Action.FAILED,
                            "import " + importPath + " failed to publish earlier in this run");
                }
                if (pendingPaths.contains(importPath)) {
                    pendingReference = true; // dry run: the import has no registered version yet
                    continue;
                }
                String referenceSubject = options.naming().subjectFor(importPath);
                Integer version = versions.get(importPath);
                if (version == null) {
                    version = latestRegisteredVersion(referenceSubject);
                    if (version == null) {
                        failedPaths.add(path);
                        return new FileOutcome(path, subject, Action.FAILED,
                                "import " + importPath + " is neither in the source set nor "
                                        + "registered as subject " + referenceSubject);
                    }
                    versions.put(importPath, version);
                }
                references.add(new ReferenceSpec(importPath, referenceSubject, version));
            }

            String candidate = candidateBody(content, references);

            // Idempotency first: identical content (schema + references) already registered?
            // Skipped when a dry run left a referenced import unwritten: the candidate's
            // references cannot match anything the registry holds.
            boolean subjectMissing = false;
            if (!pendingReference) {
                LookupResult lookup = lookupSchema(subject, candidate);
                if (lookup instanceof FoundSchema found) {
                    versions.put(path, found.version());
                    return new FileOutcome(path, subject, Action.UNCHANGED, "version " + found.version());
                }
                if (lookup instanceof InvalidSchema invalid) {
                    failedPaths.add(path);
                    return new FileOutcome(path, subject, Action.FAILED, invalid.message());
                }
                subjectMissing = ((SchemaNotFound) lookup).subjectMissing();
            }

            if (options.dryRun()) {
                pendingPaths.add(path);
                String detail = pendingReference
                        ? "depends on files that would be written in this run"
                        : subjectMissing ? "subject would be created" : "new version would be registered";
                return new FileOutcome(path, subject, Action.WOULD_WRITE, detail);
            }

            HttpResponse<String> response = post(
                    "/subjects/" + encode(subject) + "/versions", candidate,
                    "registering subject " + subject);
            int status = response.statusCode();
            if (status / 100 == 2) {
                int version = registeredVersion(subject, candidate, response.body());
                versions.put(path, version);
                return new FileOutcome(path, subject,
                        subjectMissing ? Action.CREATED : Action.UPDATED, "version " + version);
            }
            if (status == 409 || status == 422) {
                // 409: incompatible with the subject's compatibility policy; 422: invalid schema.
                failedPaths.add(path);
                return new FileOutcome(path, subject, Action.FAILED,
                        "HTTP " + status + ": " + response.body());
            }
            throw registryFailure(status, response.body(), "registering subject " + subject);
        }
    }

    /** One entry of the Schema Registry references array. */
    private record ReferenceSpec(String name, String subject, int version) {
    }

    private sealed interface LookupResult permits FoundSchema, SchemaNotFound, InvalidSchema {
    }

    /** Identical content is already registered under the subject. */
    private record FoundSchema(int version) implements LookupResult {
    }

    /** No identical content; {@code subjectMissing} distinguishes CREATED from UPDATED. */
    private record SchemaNotFound(boolean subjectMissing) implements LookupResult {
    }

    /** The registry rejected the candidate as unprocessable (HTTP 422). */
    private record InvalidSchema(String message) implements LookupResult {
    }

    // ---------------------------------------------------------------- registry protocol

    /**
     * Schema lookup ({@code POST /subjects/{subject}}): 200 with the registered version when
     * the registry already holds identical content, 404 when the subject or the schema is
     * unknown.
     */
    private LookupResult lookupSchema(String subject, String candidateBody)
            throws SchemaPublishException {
        String description = "looking up subject " + subject;
        HttpResponse<String> response = post("/subjects/" + encode(subject), candidateBody, description);
        int status = response.statusCode();
        if (status / 100 == 2) {
            return new FoundSchema(readTree(response.body(), description).path("version").asInt());
        }
        if (status == 404) {
            Integer errorCode = errorCode(response.body());
            boolean subjectMissing = errorCode != null
                    ? errorCode == ERROR_SUBJECT_NOT_FOUND
                    : !subjectExists(subject);
            return new SchemaNotFound(subjectMissing);
        }
        if (status == 422) {
            return new InvalidSchema("HTTP 422: " + response.body());
        }
        throw registryFailure(status, response.body(), description);
    }

    /** Latest registered version of a subject, or {@code null} when the subject is unknown. */
    private Integer latestRegisteredVersion(String subject) throws SchemaPublishException {
        String description = "fetching latest version of subject " + subject;
        HttpResponse<String> response = get("/subjects/" + encode(subject) + "/versions/latest", description);
        int status = response.statusCode();
        if (status / 100 == 2) {
            return readTree(response.body(), description).path("version").asInt();
        }
        if (status == 404) {
            return null;
        }
        throw registryFailure(status, response.body(), description);
    }

    private boolean subjectExists(String subject) throws SchemaPublishException {
        String description = "listing versions of subject " + subject;
        HttpResponse<String> response = get("/subjects/" + encode(subject) + "/versions", description);
        int status = response.statusCode();
        if (status / 100 == 2) {
            return true;
        }
        if (status == 404) {
            return false;
        }
        throw registryFailure(status, response.body(), description);
    }

    /**
     * Version assigned by a successful {@code POST /subjects/{subject}/versions}. Classic
     * Confluent responses carry only the schema {@code id}; when {@code version} is absent the
     * lookup endpoint (which now matches the just-written schema) supplies it.
     */
    private int registeredVersion(String subject, String candidateBody, String writeResponseBody)
            throws SchemaPublishException {
        JsonNode node = tryReadTree(writeResponseBody);
        if (node != null && node.hasNonNull("version")) {
            return node.get("version").asInt();
        }
        String description = "resolving registered version of subject " + subject;
        HttpResponse<String> lookup = post("/subjects/" + encode(subject), candidateBody, description);
        if (lookup.statusCode() / 100 == 2) {
            return readTree(lookup.body(), description).path("version").asInt();
        }
        throw registryFailure(lookup.statusCode(), lookup.body(), description);
    }

    // ---------------------------------------------------------------- HTTP plumbing

    private HttpResponse<String> get(String path, String description) throws SchemaPublishException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Accept", SCHEMA_REGISTRY_ACCEPT)
                .GET()
                .build();
        return send(request, description);
    }

    private HttpResponse<String> post(String path, String body, String description)
            throws SchemaPublishException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Accept", SCHEMA_REGISTRY_ACCEPT)
                .header("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(request, description);
    }

    private HttpResponse<String> send(HttpRequest request, String description)
            throws SchemaPublishException {
        try {
            try {
                return checked(client.send(request, HttpResponse.BodyHandlers.ofString()), description);
            } catch (IOException first) {
                // The registry's Jetty side closes idle connections after 30 seconds while the
                // JDK client pools them for much longer, so after a quiet spell the next request
                // can ride a connection the server has already closed and die mid-read. Registry
                // calls are idempotent (republishing a schema yields the same id; checks are
                // reads), so one retry on a fresh connection absorbs exactly that failure.
                try {
                    HttpResponse<String> retried =
                            checked(client.send(request, HttpResponse.BodyHandlers.ofString()), description);
                    return retried;
                } catch (IOException second) {
                    second.addSuppressed(first);
                    throw new SchemaPublishException("Registry I/O failure while " + description, second);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaPublishException("Interrupted while " + description, e);
        }
    }

    /** Auth failures and server errors mean the registry is unhealthy: abort everything. */
    private HttpResponse<String> checked(HttpResponse<String> response, String description)
            throws SchemaPublishException {
        int status = response.statusCode();
        if (status == 401 || status == 403 || status >= 500) {
            throw registryFailure(status, response.body(), description);
        }
        return response;
    }

    private static SchemaPublishException registryFailure(int status, String body, String description) {
        String detail = body == null || body.isBlank() ? "" : ": " + body;
        return new SchemaPublishException(
                "Registry returned HTTP " + status + " while " + description + detail);
    }

    // ---------------------------------------------------------------- JSON helpers

    private String candidateBody(String schema, List<ReferenceSpec> references) {
        ObjectNode node = json.createObjectNode();
        node.put("schema", schema);
        node.put("schemaType", "PROTOBUF");
        if (!references.isEmpty()) {
            ArrayNode array = node.putArray("references");
            for (ReferenceSpec reference : references) {
                array.addObject()
                        .put("name", reference.name())
                        .put("subject", reference.subject())
                        .put("version", reference.version());
            }
        }
        return node.toString();
    }

    private JsonNode readTree(String body, String description) throws SchemaPublishException {
        JsonNode node = tryReadTree(body);
        if (node == null) {
            throw new SchemaPublishException("Registry returned unparseable JSON while " + description);
        }
        return node;
    }

    private JsonNode tryReadTree(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            return json.readTree(body);
        } catch (IOException e) {
            return null;
        }
    }

    private Integer errorCode(String body) {
        JsonNode node = tryReadTree(body);
        return node != null && node.hasNonNull("error_code") ? node.get("error_code").asInt() : null;
    }

    private static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
