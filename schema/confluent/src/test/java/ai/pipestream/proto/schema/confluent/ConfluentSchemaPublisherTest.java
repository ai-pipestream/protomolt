package ai.pipestream.proto.schema.confluent;

import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.sources.publish.SchemaPublishException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ConfluentSchemaPublisher} against an in-process stateful HTTP stub
 * that mimics the Confluent Schema Registry subjects REST API (same
 * {@code com.sun.net.httpserver} idiom as {@link ConfluentSchemaRegistryLoaderTest}). The stub
 * records every request so tests can assert registration order, reference payloads and the
 * absence of writes.
 */
class ConfluentSchemaPublisherTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String CORE_PATH = "common/v1/core.proto";
    private static final String USER_PATH = "common/v1/user.proto";
    private static final String APP_PATH = "app/v1/app.proto";

    private static final String CORE_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private static final String USER_PROTO = """
            syntax = "proto3";
            package common.v1;
            import "common/v1/core.proto";
            message User {
              Core core = 1;
            }
            """;

    private static final String APP_PROTO = """
            syntax = "proto3";
            package app.v1;
            import "common/v1/user.proto";
            import "google/protobuf/timestamp.proto";
            message App {
              common.v1.User owner = 1;
              google.protobuf.Timestamp created = 2;
            }
            """;

    private static final String STANDALONE_PROTO = """
            syntax = "proto3";
            package other.v1;
            message Standalone {
              string name = 1;
            }
            """;

    private HttpServer server;
    private StubRegistry registry;
    private ConfluentSchemaPublisher publisher;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        registry = new StubRegistry();
        server.createContext("/", registry::handle);
        server.start();
        publisher = new ConfluentSchemaPublisher(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
    }

    @AfterEach
    void stopServer() {
        publisher.close();
        server.stop(0);
    }

    private static ProtoSourceSet chainSet() {
        // Inserted in NON-topological order (root first) to prove the publisher reorders.
        return ProtoSourceSet.builder()
                .add(APP_PATH, APP_PROTO, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(CORE_PATH, CORE_PROTO, "test")
                .build();
    }

    // ---------------------------------------------------------------- fresh publish

    @Test
    void publishesImportsFirstWithExactReferences() throws Exception {
        PublishResult result = publisher.publish(chainSet(), PublishOptions.defaults());

        assertThat(result.outcomes())
                .extracting(FileOutcome::path, FileOutcome::action)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(CORE_PATH, Action.CREATED),
                        org.assertj.core.groups.Tuple.tuple(USER_PATH, Action.CREATED),
                        org.assertj.core.groups.Tuple.tuple(APP_PATH, Action.CREATED));
        assertThat(result.created()).isEqualTo(3);
        assertThat(result.outcomes()).allSatisfy(o -> assertThat(o.detail()).isEqualTo("version 1"));

        // Registration order is imports-first.
        List<StubRegistry.Request> writes = registry.versionWrites();
        assertThat(writes).extracting(StubRegistry.Request::subject)
                .containsExactly(CORE_PATH, USER_PATH, APP_PATH);

        // Reference arrays carry exact {name, subject, version}; google/protobuf/* is skipped.
        JsonNode coreRefs = JSON.readTree(writes.get(0).body()).path("references");
        assertThat(coreRefs.isMissingNode() || coreRefs.isEmpty()).isTrue();

        JsonNode userRefs = JSON.readTree(writes.get(1).body()).path("references");
        assertThat(userRefs).hasSize(1);
        assertThat(userRefs.get(0).get("name").asText()).isEqualTo(CORE_PATH);
        assertThat(userRefs.get(0).get("subject").asText()).isEqualTo(CORE_PATH);
        assertThat(userRefs.get(0).get("version").asInt()).isEqualTo(1);

        JsonNode appRefs = JSON.readTree(writes.get(2).body()).path("references");
        assertThat(appRefs).hasSize(1);
        assertThat(appRefs.get(0).get("name").asText()).isEqualTo(USER_PATH);
        assertThat(appRefs.get(0).get("subject").asText()).isEqualTo(USER_PATH);
        assertThat(appRefs.get(0).get("version").asInt()).isEqualTo(1);
    }

    @Test
    void urlEncodesSubjectsWithSlashesInEveryPath() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder().add(CORE_PATH, CORE_PROTO, "test").build();
        publisher.publish(set, PublishOptions.defaults());

        // Both the lookup POST and the version-create POST must encode the subject's slashes.
        assertThat(registry.requests)
                .filteredOn(r -> r.method().equals("POST"))
                .isNotEmpty()
                .allSatisfy(r -> assertThat(r.rawPath()).contains("common%2Fv1%2Fcore.proto"));
        assertThat(registry.subjects).containsKey(CORE_PATH);
    }

    // ---------------------------------------------------------------- idempotency

    @Test
    void republishingIdenticalContentIsUnchangedWithoutVersionWrites() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());
        registry.requests.clear();

        PublishResult second = publisher.publish(chainSet(), PublishOptions.defaults());

        assertThat(second.outcomes()).extracting(FileOutcome::action)
                .containsOnly(Action.UNCHANGED);
        assertThat(second.unchanged()).isEqualTo(3);
        assertThat(second.outcomes()).allSatisfy(o -> assertThat(o.detail()).isEqualTo("version 1"));
        assertThat(registry.versionWrites()).isEmpty();
    }

    @Test
    void changedFileIsUpdatedOnlyForThatFile() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());

        String changedApp = APP_PROTO.replace("created = 2;", "created = 2;\n  string note = 3;");
        ProtoSourceSet changed = ProtoSourceSet.builder()
                .add(CORE_PATH, CORE_PROTO, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(APP_PATH, changedApp, "test")
                .build();
        registry.requests.clear();

        PublishResult result = publisher.publish(changed, PublishOptions.defaults());

        assertThat(result.outcomes())
                .extracting(FileOutcome::path, FileOutcome::action)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(CORE_PATH, Action.UNCHANGED),
                        org.assertj.core.groups.Tuple.tuple(USER_PATH, Action.UNCHANGED),
                        org.assertj.core.groups.Tuple.tuple(APP_PATH, Action.UPDATED));
        assertThat(outcomeFor(result, APP_PATH).detail()).isEqualTo("version 2");
        assertThat(registry.versionWrites()).extracting(StubRegistry.Request::subject)
                .containsExactly(APP_PATH);
    }

    // ---------------------------------------------------------------- per-file failures

    @Test
    void incompatibleSchemaFailsThatFileAndItsDependentsButContinues() throws Exception {
        registry.incompatibleSubjects.put(USER_PATH, "schema is incompatible with earlier versions");
        ProtoSourceSet set = chainSet().merge(
                ProtoSourceSet.builder().add("other/v1/standalone.proto", STANDALONE_PROTO, "test").build());

        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        assertThat(outcomeFor(result, CORE_PATH).action()).isEqualTo(Action.CREATED);
        FileOutcome user = outcomeFor(result, USER_PATH);
        assertThat(user.action()).isEqualTo(Action.FAILED);
        assertThat(user.detail()).contains("409").contains("incompatible with earlier versions");
        FileOutcome app = outcomeFor(result, APP_PATH);
        assertThat(app.action()).isEqualTo(Action.FAILED);
        assertThat(app.detail()).contains(USER_PATH);
        // Remaining independent files are still attempted.
        assertThat(outcomeFor(result, "other/v1/standalone.proto").action()).isEqualTo(Action.CREATED);

        assertThatThrownBy(result::throwIfFailed)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("2 of 4")
                .hasMessageContaining(USER_PATH)
                .hasMessageContaining(APP_PATH);
    }

    @Test
    void missingImportFailsWithImportNameAndOthersContinue() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(USER_PATH, USER_PROTO, "test") // imports core, which is nowhere
                .add("other/v1/standalone.proto", STANDALONE_PROTO, "test")
                .build();

        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        FileOutcome user = outcomeFor(result, USER_PATH);
        assertThat(user.action()).isEqualTo(Action.FAILED);
        assertThat(user.detail()).contains(CORE_PATH).contains("neither in the source set");
        assertThat(outcomeFor(result, "other/v1/standalone.proto").action()).isEqualTo(Action.CREATED);
    }

    @Test
    void importAlreadyInRegistryResolvesToItsLatestVersion() throws Exception {
        registry.preload(CORE_PATH, CORE_PROTO + "// v1\n", null);
        registry.preload(CORE_PATH, CORE_PROTO, null);
        ProtoSourceSet set = ProtoSourceSet.builder().add(USER_PATH, USER_PROTO, "test").build();

        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        assertThat(outcomeFor(result, USER_PATH).action()).isEqualTo(Action.CREATED);
        JsonNode refs = JSON.readTree(registry.versionWrites().getFirst().body()).path("references");
        assertThat(refs.get(0).get("subject").asText()).isEqualTo(CORE_PATH);
        assertThat(refs.get(0).get("version").asInt()).isEqualTo(2);
    }

    // ---------------------------------------------------------------- dry run

    @Test
    void dryRunReportsWouldWriteAndPerformsNoWrites() throws Exception {
        PublishResult result = publisher.publish(chainSet(), PublishOptions.dryRunDefaults());

        assertThat(result.outcomes()).extracting(FileOutcome::action)
                .containsOnly(Action.WOULD_WRITE);
        assertThat(registry.versionWrites()).isEmpty();
        assertThat(registry.subjects).isEmpty();
    }

    @Test
    void dryRunAfterPublishReportsUnchangedWithoutWrites() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());
        registry.requests.clear();

        PublishResult result = publisher.publish(chainSet(), PublishOptions.dryRunDefaults());

        assertThat(result.outcomes()).extracting(FileOutcome::action)
                .containsOnly(Action.UNCHANGED);
        assertThat(registry.versionWrites()).isEmpty();
    }

    // ---------------------------------------------------------------- registry-level failures

    @Test
    void authFailureThrowsSchemaPublishException() {
        registry.failAllStatus = 401;
        assertThatThrownBy(() -> publisher.publish(chainSet(), PublishOptions.defaults()))
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("401");
    }

    @Test
    void serverErrorThrowsSchemaPublishException() {
        registry.failAllStatus = 503;
        assertThatThrownBy(() -> publisher.publish(chainSet(), PublishOptions.defaults()))
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("503");
    }

    @Test
    void unreachableRegistryThrowsSchemaPublishException() {
        server.stop(0);
        assertThatThrownBy(() -> publisher.publish(chainSet(), PublishOptions.defaults()))
                .isInstanceOf(SchemaPublishException.class);
    }

    @Test
    void targetDescribesRegistry() {
        assertThat(publisher.target())
                .startsWith("confluent:")
                .contains(String.valueOf(server.getAddress().getPort()));
    }

    private static FileOutcome outcomeFor(PublishResult result, String path) {
        return result.outcomes().stream()
                .filter(o -> o.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No outcome for " + path));
    }

    // ---------------------------------------------------------------- stub registry

    /**
     * Stateful in-memory subjects API: schema lookup ({@code POST /subjects/{s}}), version
     * registration ({@code POST /subjects/{s}/versions}, response carries only {@code id} like
     * classic Confluent, forcing the publisher's follow-up version lookup) and version reads.
     * Subjects arrive URL-encoded; the dispatcher decodes raw path segments, so a publisher
     * that failed to encode slashes would corrupt its subject names and fail the assertions.
     */
    // ---------------------------------------------------------------- stale connections

    @Test
    void truncatedResponseIsRetriedOnceAndThePublishSucceeds() throws Exception {
        // The registry's Jetty side closes idle connections under the JDK client's pool
        // timeout, and the next request dies mid-read. Simulate that class of failure: the
        // first response declares more bytes than it sends, so the client sees EOF mid-body;
        // the publisher must retry once on a fresh connection and carry on.
        AtomicBoolean truncateNext = new AtomicBoolean(true);
        server.removeContext("/");
        server.createContext("/", exchange -> {
            if (truncateNext.compareAndSet(true, false)) {
                exchange.sendResponseHeaders(200, 1000);
                exchange.getResponseBody().write('{');
                exchange.close();
                return;
            }
            registry.handle(exchange);
        });
        ProtoSourceSet set = ProtoSourceSet.builder().add(CORE_PATH, CORE_PROTO, "test").build();

        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        assertThat(truncateNext.get()).isFalse();
        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.CREATED);
    }

    private static final class StubRegistry {

        record Request(String method, String rawPath, String subject, String body) {
        }

        record Stored(String schema, JsonNode references) {
        }

        final List<Request> requests = new CopyOnWriteArrayList<>();
        final Map<String, List<Stored>> subjects = new LinkedHashMap<>();
        final Map<String, String> incompatibleSubjects = new LinkedHashMap<>();
        volatile int failAllStatus;
        private int nextId = 1;

        List<Request> versionWrites() {
            return requests.stream()
                    .filter(r -> r.method().equals("POST") && r.rawPath().endsWith("/versions"))
                    .toList();
        }

        /** Registers a version directly, simulating pre-existing registry state. */
        synchronized void preload(String subject, String schema, JsonNode references) {
            subjects.computeIfAbsent(subject, s -> new ArrayList<>())
                    .add(new Stored(schema, references == null ? JSON.createArrayNode() : references));
        }

        synchronized void handle(HttpExchange exchange) {
            try {
                String method = exchange.getRequestMethod();
                String rawPath = exchange.getRequestURI().getRawPath();
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                List<String> segments = decodeSegments(rawPath);
                String subject = segments.size() > 1 && segments.get(0).equals("subjects")
                        ? segments.get(1) : null;
                requests.add(new Request(method, rawPath, subject, body));

                if (failAllStatus != 0) {
                    respond(exchange, failAllStatus, "{\"error_code\":" + failAllStatus + "}");
                    return;
                }
                if (subject == null) {
                    respond(exchange, 404, "{\"error_code\":404}");
                    return;
                }
                if (segments.size() == 2 && method.equals("POST")) {
                    lookup(exchange, subject, body);
                } else if (segments.size() == 3 && segments.get(2).equals("versions")) {
                    if (method.equals("POST")) {
                        register(exchange, subject, body);
                    } else {
                        listVersions(exchange, subject);
                    }
                } else if (segments.size() == 4 && segments.get(2).equals("versions")
                        && segments.get(3).equals("latest")) {
                    latest(exchange, subject);
                } else {
                    respond(exchange, 404, "{\"error_code\":404}");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void lookup(HttpExchange exchange, String subject, String body) throws IOException {
            List<Stored> versions = subjects.get(subject);
            if (versions == null) {
                respond(exchange, 404,
                        "{\"error_code\":40401,\"message\":\"Subject '" + subject + "' not found.\"}");
                return;
            }
            JsonNode candidate = JSON.readTree(body);
            JsonNode candidateRefs = normalizedReferences(candidate);
            for (int i = 0; i < versions.size(); i++) {
                Stored stored = versions.get(i);
                if (stored.schema().equals(candidate.path("schema").asText())
                        && stored.references().equals(candidateRefs)) {
                    ObjectNode response = JSON.createObjectNode()
                            .put("subject", subject)
                            .put("version", i + 1)
                            .put("id", i + 1)
                            .put("schemaType", "PROTOBUF")
                            .put("schema", stored.schema());
                    respond(exchange, 200, response.toString());
                    return;
                }
            }
            respond(exchange, 404, "{\"error_code\":40403,\"message\":\"Schema not found\"}");
        }

        private void register(HttpExchange exchange, String subject, String body) throws IOException {
            String incompatibleMessage = incompatibleSubjects.get(subject);
            if (incompatibleMessage != null) {
                respond(exchange, 409,
                        "{\"error_code\":409,\"message\":\"" + incompatibleMessage + "\"}");
                return;
            }
            JsonNode candidate = JSON.readTree(body);
            subjects.computeIfAbsent(subject, s -> new ArrayList<>())
                    .add(new Stored(candidate.path("schema").asText(), normalizedReferences(candidate)));
            // Classic Confluent write response: id only, no version.
            respond(exchange, 200, "{\"id\":" + nextId++ + "}");
        }

        private void latest(HttpExchange exchange, String subject) throws IOException {
            List<Stored> versions = subjects.get(subject);
            if (versions == null) {
                respond(exchange, 404, "{\"error_code\":40401}");
                return;
            }
            Stored stored = versions.getLast();
            ObjectNode response = JSON.createObjectNode()
                    .put("subject", subject)
                    .put("version", versions.size())
                    .put("id", versions.size())
                    .put("schemaType", "PROTOBUF")
                    .put("schema", stored.schema());
            response.set("references", stored.references());
            respond(exchange, 200, response.toString());
        }

        private void listVersions(HttpExchange exchange, String subject) throws IOException {
            List<Stored> versions = subjects.get(subject);
            if (versions == null) {
                respond(exchange, 404, "{\"error_code\":40401}");
                return;
            }
            ArrayNode array = JSON.createArrayNode();
            for (int i = 1; i <= versions.size(); i++) {
                array.add(i);
            }
            respond(exchange, 200, array.toString());
        }

        private static JsonNode normalizedReferences(JsonNode candidate) {
            JsonNode references = candidate.path("references");
            return references.isArray() ? references : JSON.createArrayNode();
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

        private static void respond(HttpExchange exchange, int status, String body) throws IOException {
            byte[] payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(status, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        }
    }
}
