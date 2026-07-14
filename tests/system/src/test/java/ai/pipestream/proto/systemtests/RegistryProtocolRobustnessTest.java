package ai.pipestream.proto.systemtests;

import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Protocol-robustness tests for {@link SchemaRegistryServer} over an in-memory store: hostile
 * or sloppy HTTP input must produce a bounded 4xx with the Confluent error envelope
 * ({@code {"error_code": n, "message": s}}) — never a 500, never a hang — and concurrent
 * registrations must converge without duplicate identities. Every request carries a client
 * timeout, so a hang fails the test rather than stalling the build.
 */
class RegistryProtocolRobustnessTest {

    private static final String REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Test
    void malformedJsonRegistrationBodyYieldsA4xxErrorEnvelopeNeverA500() throws Exception {
        try (Harness harness = Harness.start()) {
            HttpResponse<String> response = harness.post(
                    "/subjects/robust.proto/versions", "{this is not json", REGISTRY_CONTENT_TYPE);

            assertThat(response.statusCode()).isEqualTo(422);
            JsonNode envelope = JSON.readTree(response.body());
            assertThat(envelope.get("error_code").asInt()).isEqualTo(42201);
            assertThat(envelope.get("message").asText()).contains("not JSON");
        }
    }

    /**
     * Pins today's contract: the server never inspects the request {@code Content-Type}. A
     * well-formed registration succeeds identically under the registry media type, plain text,
     * and no declared type at all — accepted consistently, not sometimes rejected.
     */
    @Test
    void requestContentTypeIsIgnoredSoRegistrationBehavesTheSameUnderAnyDeclaredType()
            throws Exception {
        try (Harness harness = Harness.start()) {
            HttpResponse<String> vendor = harness.post("/subjects/a.proto/versions",
                    registrationBody(schema("a")), REGISTRY_CONTENT_TYPE);
            HttpResponse<String> plain = harness.post("/subjects/b.proto/versions",
                    registrationBody(schema("b")), "text/plain");
            HttpResponse<String> none = harness.post("/subjects/c.proto/versions",
                    registrationBody(schema("c")), null);

            assertThat(vendor.statusCode()).isEqualTo(200);
            assertThat(plain.statusCode()).isEqualTo(200);
            assertThat(none.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(plain.body()).get("id").asInt()).isPositive();
        }
    }

    @Test
    void bodyLargerThanTheConfiguredLimitIsRejectedWith413() throws Exception {
        try (Harness harness = Harness.start(2048)) {
            String oversized = "x".repeat(10_000);
            HttpResponse<String> response = harness.post(
                    "/subjects/big.proto/versions", oversized, REGISTRY_CONTENT_TYPE);

            assertThat(response.statusCode()).isEqualTo(413);
            JsonNode envelope = JSON.readTree(response.body());
            assertThat(envelope.get("error_code").asInt()).isEqualTo(413);
            assertThat(envelope.get("message").asText()).contains("exceeds");
        }
    }

    @Test
    void unknownPathsReturnA404ErrorEnvelope() throws Exception {
        try (Harness harness = Harness.start()) {
            HttpResponse<String> response = harness.get("/no/such/route");

            assertThat(response.statusCode()).isEqualTo(404);
            JsonNode envelope = JSON.readTree(response.body());
            assertThat(envelope.get("error_code").asInt()).isEqualTo(404);
            assertThat(envelope.get("message").asText()).isEqualTo("HTTP 404 Not Found");
        }
    }

    @Test
    void wrongMethodOnAKnownRouteReturns405WithTheAllowHeader() throws Exception {
        try (Harness harness = Harness.start()) {
            // /subjects/{subject} is the POST-only content-lookup route.
            HttpResponse<String> get = harness.get("/subjects/robust.proto");
            assertThat(get.statusCode()).isEqualTo(405);
            assertThat(get.headers().firstValue("Allow")).contains("POST");
            assertThat(JSON.readTree(get.body()).get("error_code").asInt()).isEqualTo(405);

            // /subjects/{subject}/versions supports GET and POST, nothing else.
            HttpResponse<String> delete = harness.send(HttpRequest.newBuilder(
                            harness.base.resolve("/subjects/robust.proto/versions"))
                    .timeout(Duration.ofSeconds(30))
                    .DELETE()
                    .build());
            assertThat(delete.statusCode()).isEqualTo(405);
            assertThat(delete.headers().firstValue("Allow")).contains("GET, POST");
        }
    }

    @Test
    void concurrentRegistrationsOfDifferentSubjectsAllSucceedWithUniqueGlobalIds()
            throws Exception {
        try (Harness harness = Harness.start()) {
            List<HttpResponse<String>> responses = harness.inParallel(8, worker ->
                    harness.post("/subjects/load-s" + worker + ".proto/versions",
                            registrationBody(schema("s" + worker)), REGISTRY_CONTENT_TYPE));

            assertThat(responses).allSatisfy(
                    response -> assertThat(response.statusCode()).isEqualTo(200));
            List<Integer> ids = new ArrayList<>();
            for (HttpResponse<String> response : responses) {
                ids.add(JSON.readTree(response.body()).get("id").asInt());
            }
            assertThat(ids).hasSize(8).doesNotHaveDuplicates();

            JsonNode subjects = JSON.readTree(harness.get("/subjects").body());
            assertThat(subjects.isArray()).isTrue();
            assertThat(subjects).hasSize(8);
        }
    }

    @Test
    void concurrentRegistrationsOfIdenticalContentConvergeToOneVersionAndOneId()
            throws Exception {
        try (Harness harness = Harness.start()) {
            String body = registrationBody(schema("same"));
            List<HttpResponse<String>> responses = harness.inParallel(8, worker ->
                    harness.post("/subjects/same.proto/versions", body, REGISTRY_CONTENT_TYPE));

            assertThat(responses).allSatisfy(
                    response -> assertThat(response.statusCode()).isEqualTo(200));
            List<Integer> ids = new ArrayList<>();
            for (HttpResponse<String> response : responses) {
                ids.add(JSON.readTree(response.body()).get("id").asInt());
            }
            assertThat(ids).hasSize(8);
            assertThat(ids.stream().distinct()).hasSize(1);

            JsonNode versions = JSON.readTree(
                    harness.get("/subjects/same.proto/versions").body());
            assertThat(versions).hasSize(1);
            assertThat(versions.get(0).asInt()).isEqualTo(1);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String schema(String pkg) {
        return """
                syntax = "proto3";
                package %s;
                message Doc { string id = 1; }
                """.formatted(pkg);
    }

    private static String registrationBody(String schema) {
        ObjectNode body = JSON.createObjectNode()
                .put("schema", schema)
                .put("schemaType", "PROTOBUF");
        body.putArray("references");
        return body.toString();
    }

    /** One in-memory-backed server on an ephemeral port, plus request plumbing. */
    private record Harness(InMemorySchemaRegistryStore store, SchemaRegistryServer server,
                           URI base) implements AutoCloseable {

        static Harness start() {
            return start(SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES);
        }

        static Harness start(int maxRequestBytes) {
            InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
            SchemaRegistryServer server = new SchemaRegistryServer(
                    new SchemaRegistryServerConfig("127.0.0.1", 0, "/health", "/protomolt",
                            maxRequestBytes),
                    store);
            URI base = URI.create("http://127.0.0.1:" + server.start());
            return new Harness(store, server, base);
        }

        HttpResponse<String> post(String path, String body, String contentType) throws Exception {
            HttpRequest.Builder request = HttpRequest.newBuilder(base.resolve(path))
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            if (contentType != null) {
                request.header("Content-Type", contentType);
            }
            return send(request.build());
        }

        HttpResponse<String> get(String path) throws Exception {
            return send(HttpRequest.newBuilder(base.resolve(path))
                    .timeout(Duration.ofSeconds(30)).GET().build());
        }

        HttpResponse<String> send(HttpRequest request) throws Exception {
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        }

        /**
         * Runs {@code workers} tasks that all start on one latch (no sleeps) and returns their
         * results, failing on any task error.
         */
        List<HttpResponse<String>> inParallel(int workers, ParallelTask task) throws Exception {
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            try {
                List<Future<HttpResponse<String>>> futures = new ArrayList<>();
                for (int worker = 0; worker < workers; worker++) {
                    int id = worker;
                    futures.add(pool.submit((Callable<HttpResponse<String>>) () -> {
                        start.await();
                        return task.run(id);
                    }));
                }
                start.countDown();
                List<HttpResponse<String>> responses = new ArrayList<>();
                for (Future<HttpResponse<String>> future : futures) {
                    responses.add(future.get(60, TimeUnit.SECONDS));
                }
                return responses;
            } finally {
                pool.shutdownNow();
                assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }

        @Override
        public void close() {
            server.close();
            store.close();
        }
    }

    @FunctionalInterface
    private interface ParallelTask {
        HttpResponse<String> run(int worker) throws Exception;
    }
}
