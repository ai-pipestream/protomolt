package ai.protomolt.proto.serve;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The live structured-inference acceptance: one explicitly configured,
 * unauthenticated OpenAI-compatible generative endpoint declaring
 * structured-output is exercised through BOTH the typed gRPC surface
 * (ProtoMoltService action dispatch) and the MCP surface (streamable HTTP
 * tools/call), end to end through a real in-process ProtoMoltServe.
 *
 * <p>Opt-in only; normal CI skips this class entirely (no container, no GPU,
 * no model deployment - the endpoint is the operator's, e.g. a local vLLM or
 * LM Studio server):</p>
 *
 * <pre>
 * PROTOMOLT_LIVE_STRUCTURED_ENDPOINT=http://127.0.0.1:1234 \
 * PROTOMOLT_LIVE_STRUCTURED_MODEL=qwen2.5-7b-instruct \
 * [PROTOMOLT_LIVE_STRUCTURED_CREDENTIAL_REF=env:OPENAI_TOKEN \]
 * ./gradlew :protomolt-serve:test --tests '*LiveStructuredInferenceIT'
 * </pre>
 *
 * <p>{@code PROTOMOLT_LIVE_STRUCTURED_ENDPOINT} is the base URL of an
 * OpenAI-compatible chat-completions endpoint (the path
 * {@code /v1/chat/completions} is appended). {@code PROTOMOLT_LIVE_STRUCTURED_MODEL}
 * is the backend model name sent on the wire; it is REQUIRED when the
 * endpoint is set and the test fails with a clear message when it is absent -
 * the test never defaults to a public API.</p>
 *
 * <p>Authenticated endpoints: when
 * {@code PROTOMOLT_LIVE_STRUCTURED_CREDENTIAL_REF} is set (e.g.
 * {@code env:OPENAI_TOKEN}), the catalog model carries that credential
 * reference and the transport resolves it through the environment resolver,
 * sending {@code Authorization: Bearer <resolved>} on every provider request.
 * The variable the reference names must be set in the test process
 * environment. The capturing proxy records the header and forwards it
 * upstream, and the test asserts every outbound request carried the resolved
 * bearer token while no request body ever contained it. Without the variable
 * the model is registered unauthenticated, and the endpoint MUST accept
 * unauthenticated calls.</p>
 *
 * <p>Wire-level privacy is proven deterministically: the catalog model's
 * endpoint points at an in-process capturing reverse proxy, which forwards to
 * the configured endpoint and records every outbound request body. The workflow
 * input carries an excluded-from-projection sentinel and a sensitivity-masked
 * sentinel; the test proves both appear in NO captured provider request, NO
 * persisted artifact byte, and NO serialized RunEvidence, while an allowed
 * grounded value DOES appear in every provider request (so the privacy
 * assertions are not vacuous). The excluded field also carries a sensitivity
 * annotation because the recorder persists the run input and the edge-produced
 * message, and its only redaction mechanism is the sensitivity pass; the
 * projection exclusion is what keeps the field off the wire, which the
 * captured request bodies prove directly.</p>
 *
 * <p>Repair-loop coverage is deliberately NOT exercised here: forcing a live
 * model to fail its first attempt would rely on probabilistic prompting.
 * Forced-repair coverage lives with the scripted provider
 * (StructuredGeneratorTest, StructuredWorkflowReplayTest); this test owns
 * transport, grounding, validation, and privacy acceptance, and accepts any
 * successful attempt count within the coordinator's budget of 1 to 3.</p>
 */
@EnabledIfEnvironmentVariable(named = "PROTOMOLT_LIVE_STRUCTURED_ENDPOINT", matches = ".+")
class LiveStructuredInferenceIT {

    private static final String ENDPOINT_ENV = "PROTOMOLT_LIVE_STRUCTURED_ENDPOINT";
    private static final String MODEL_ENV = "PROTOMOLT_LIVE_STRUCTURED_MODEL";
    private static final String CREDENTIAL_REF_ENV = "PROTOMOLT_LIVE_STRUCTURED_CREDENTIAL_REF";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROJECTION = "ai/protomolt/proto/projection/v1/projection.proto";
    private static final String METADATA = "ai/protomolt/proto/meta/v1/metadata.proto";
    private static final String ACCEPTANCE = "ai/protomolt/proto/serve/live/acceptance.proto";

    private static final String UPSTREAM = "live.acceptance.UpstreamResult";
    private static final String GROUNDING = "live.acceptance.CaseGrounding";
    private static final String SUMMARY = "live.acceptance.CaseSummary";
    private static final String MODEL_ID = "live-structured";
    private static final String STEP = "summarize";

    private static final String ALLOWED_DOC = "ALLOWED-DOC-24-cv-00117";
    private static final String ALLOWED_TITLE = "ALLOWED-TITLE-State v. Example";
    private static final String EXCLUDED_SENTINEL = "EXCLUDED-SENTINEL-e51c9a2f7b";
    private static final String SENSITIVE_SENTINEL = "SENSITIVE-SENTINEL-0b44d1c8a6";

    @TempDir
    Path directory;

    @Test
    void liveGroundedGenerationThroughTypedGrpcAndMcp() throws Exception {
        String endpoint = System.getenv(ENDPOINT_ENV);
        String backend = System.getenv(MODEL_ENV);
        if (backend == null || backend.isBlank()) {
            fail(MODEL_ENV + " is required when " + ENDPOINT_ENV + " is set: it names "
                    + "the backend model the endpoint serves (e.g. qwen2.5-7b-instruct). "
                    + "The test never defaults to a public API.");
        }

        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(PROJECTION, resource(PROJECTION), "test")
                .add(METADATA, resource(METADATA), "test")
                .add(ACCEPTANCE, resource(ACCEPTANCE), "test")
                .build());
        Descriptor upstream = compiled.descriptorFor(ACCEPTANCE).orElseThrow()
                .findMessageTypeByName("UpstreamResult");
        Descriptor summary = compiled.descriptorFor(ACCEPTANCE).orElseThrow()
                .findMessageTypeByName("CaseSummary");
        String descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());

        Path workflows = directory.resolve("workflows");
        CapturingProxy proxy = new CapturingProxy(endpoint);
        String credentialRef = System.getenv(CREDENTIAL_REF_ENV);
        String expectedBearer = null;
        if (credentialRef != null && !credentialRef.isBlank()) {
            expectedBearer = expectedBearer(credentialRef);
        }
        String modelSpec = MODEL_ID + "|openai|http://127.0.0.1:" + proxy.port()
                + "|" + backend + "||structured-output"
                + (expectedBearer == null ? "" : "|" + credentialRef);
        try (proxy;
             ProtoMoltServe serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                     "127.0.0.1", 0, 0, null, 0, null, false, null, null,
                     List.of(modelSpec), null, null, workflows))) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", serve.grpcPort()).usePlaintext().build();
            try {
                runScenario(new TypedGrpcSurface(channel), "live-grpc",
                        descriptorSet, upstream, summary, workflows);
            } finally {
                channel.shutdownNow();
            }
            try (McpSurface mcp = new McpSurface(serve.httpPort())) {
                runScenario(mcp, "live-mcp", descriptorSet, upstream, summary, workflows);
            }
        }

        // Wire-level proof: every request the transport sent to the (proxied)
        // endpoint carried the structured-output envelope and the allowed
        // grounding values, and carried neither sentinel. Grounding is packed
        // before the recorder ever runs, so absence here is the projection's
        // doing, not the redaction pass's.
        assertThat(proxy.bodies()).isNotEmpty();
        for (String body : proxy.bodies()) {
            assertThat(body).contains(ALLOWED_DOC).contains(ALLOWED_TITLE);
            assertThat(body).contains("response_format").contains("json_schema");
            assertThat(body).doesNotContain(EXCLUDED_SENTINEL, SENSITIVE_SENTINEL);
        }

        // Authenticated runs: every outbound request carried exactly the
        // resolved bearer token, and the token never entered a request body.
        if (expectedBearer != null) {
            assertThat(proxy.authorizations()).isNotEmpty();
            boolean everyHeaderMatches = proxy.authorizations().stream()
                    .allMatch(expectedBearer::equals);
            assertThat(everyHeaderMatches)
                    .as("every provider request carried the configured bearer credential")
                    .isTrue();
            String token = expectedBearer.substring("Bearer ".length());
            boolean bodyContainsToken = proxy.bodies().stream()
                    .anyMatch(body -> body.contains(token));
            assertThat(bodyContainsToken)
                    .as("provider request bodies never contain bearer material")
                    .isFalse();
        }

        // Persistence-level proof: no file under the workflow workspace (input,
        // request, response, and output artifacts plus the stored run
        // evidence) carries either sentinel, credential reference, or bearer
        // material. Boolean assertions deliberately avoid printing credential
        // values if this live check fails.
        boolean credentialEvidenceLeak = false;
        try (Stream<Path> paths = Files.walk(workflows)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                String content = new String(Files.readAllBytes(path),
                        StandardCharsets.ISO_8859_1);
                assertThat(content).as("persisted file %s", path)
                        .doesNotContain(EXCLUDED_SENTINEL, SENSITIVE_SENTINEL);
                if (expectedBearer != null) {
                    String token = expectedBearer.substring("Bearer ".length());
                    credentialEvidenceLeak |= content.contains(token)
                            || content.contains(credentialRef);
                }
            }
        }
        assertThat(credentialEvidenceLeak)
                .as("persisted workflow evidence excludes credential references and material")
                .isFalse();
    }

    /** The bearer header value the transport must send for the configured reference. */
    private static String expectedBearer(String credentialRef) {
        if (!credentialRef.startsWith("env:")) {
            fail(CREDENTIAL_REF_ENV + " supports the env: scheme in this test: the "
                    + "referenced variable must be readable from the test process environment.");
        }
        String value = System.getenv(credentialRef.substring("env:".length()));
        if (value == null || value.isEmpty()) {
            fail(CREDENTIAL_REF_ENV + " is set but the referenced environment variable "
                    + "is unset or empty in the test process; the authenticated live run "
                    + "cannot proceed.");
        }
        return "Bearer " + value;
    }

    /** compile-workflow, record-workflow-run, then offline replay-workflow, on one surface. */
    private void runScenario(Surface surface, String runId, String descriptorSet,
                             Descriptor upstream, Descriptor summary, Path workflows)
            throws Exception {
        ObjectNode workflow = workflow();
        ObjectNode compile = MAPPER.createObjectNode();
        compile.set("workflow", workflow);
        JsonNode compiled = surface.tool("compile-workflow", compile);
        String fingerprint = compiled.path("workflowFingerprint").asText();
        assertThat(fingerprint).hasSize(64);

        ObjectNode record = MAPPER.createObjectNode();
        record.set("workflow", workflow);
        record.set("input", inputFixture());
        record.put("runId", runId);
        JsonNode recorded = surface.tool("record-workflow-run", record);
        assertThat(recorded.path("ok").asBoolean()).as(recorded::toString).isTrue();
        JsonNode evidence = recorded.path("evidence");
        assertEvidence(evidence, fingerprint, runId);
        assertThat(evidence.toString())
                .doesNotContain(EXCLUDED_SENTINEL, SENSITIVE_SENTINEL);

        assertPersistedFixtures(evidence, upstream, summary, workflows);

        ObjectNode replay = MAPPER.createObjectNode();
        replay.set("workflow", compiled.path("workflow"));
        replay.put("runId", runId);
        replay.set("schema", MAPPER.createObjectNode()
                .put("descriptorSetBase64", descriptorSet));
        JsonNode replayed = surface.tool("replay-workflow", replay);
        assertThat(replayed.path("ok").asBoolean()).as(replayed::toString).isTrue();
    }

    /** The recorded evidence: bounded structured and edge provenance, redacted fixtures. */
    private static void assertEvidence(JsonNode evidence, String fingerprint, String runId) {
        assertThat(evidence.path("runId").asText()).isEqualTo(runId);
        assertThat(evidence.path("status").asText()).isEqualTo("RUN_STATUS_SUCCEEDED");
        assertThat(evidence.path("workflowFingerprint").asText()).isEqualTo(fingerprint);
        assertThat(evidence.path("inputArtifact").path("redacted").asBoolean()).isTrue();
        assertThat(evidence.path("outputArtifact").path("redacted").asBoolean()).isTrue();

        assertThat(evidence.path("steps")).hasSize(1);
        JsonNode step = evidence.path("steps").get(0);
        assertThat(step.path("stepName").asText()).isEqualTo(STEP);
        assertThat(step.path("status").asText()).isEqualTo("STEP_STATUS_SUCCEEDED");
        assertThat(step.path("requestArtifact").path("redacted").asBoolean()).isTrue();
        assertThat(step.path("responseArtifact").path("redacted").asBoolean()).isTrue();

        JsonNode structured = step.path("structured");
        assertThat(structured.path("targetType").asText()).isEqualTo(SUMMARY);
        assertThat(structured.path("model").asText()).isEqualTo(MODEL_ID);
        assertThat(structured.path("provider").asText()).isEqualTo("openai");
        assertThat(structured.path("promptFingerprint").asText())
                .matches("[0-9a-f]{64}");
        assertThat(structured.path("schemaFingerprint").asText())
                .matches("[0-9a-f]{64}");
        assertThat(structured.path("validationPassed").asBoolean()).isTrue();
        // A live model may legitimately need a repair inside the coordinator's
        // budget; assert the bounds and the terminal success, not a count.
        JsonNode attempts = structured.path("attempts");
        assertThat(attempts.size()).isBetween(1, 3);
        for (int i = 0; i < attempts.size(); i++) {
            assertThat(attempts.get(i).path("attempt").asInt()).isEqualTo(i + 1);
        }
        assertThat(attempts.get(attempts.size() - 1).path("outcome").asText())
                .isEqualTo("ATTEMPT_OUTCOME_SUCCEEDED");

        JsonNode edge = step.path("edge");
        assertThat(edge.path("edgeFingerprint").asText()).matches("[0-9a-f]{64}");
        assertThat(edge.path("validationPassed").asBoolean()).isTrue();
        assertThat(edge.path("sourceCount").asInt()).isEqualTo(1);
        assertThat(edge.path("itemCount").asInt()).isZero();
    }

    /**
     * The persisted fixtures unpack and validate: the output artifact is a
     * rules-clean {@code CaseSummary}, and the step's request artifact is the
     * edge-produced upstream result with both sensitive fields masked out.
     */
    private static void assertPersistedFixtures(JsonNode evidence, Descriptor upstream,
                                                Descriptor summary, Path workflows)
            throws Exception {
        Path artifacts = workflows.resolve("artifacts");

        String outputSha = evidence.path("outputArtifact").path("sha256").asText();
        DynamicMessage output = DynamicMessage.parseFrom(summary,
                Files.readAllBytes(artifacts.resolve(outputSha)));
        ValidationResult outputResult = ProtoValidator.forMessageType(summary)
                .validate(output);
        assertThat(outputResult.valid()).as(outputResult::toString).isTrue();
        assertThat((String) output.getField(summary.findFieldByName("headline")))
                .isNotBlank();

        String requestSha = evidence.path("steps").get(0)
                .path("requestArtifact").path("sha256").asText();
        DynamicMessage produced = DynamicMessage.parseFrom(upstream,
                Files.readAllBytes(artifacts.resolve(requestSha)));
        assertThat(produced.getField(upstream.findFieldByName("doc_id")))
                .isEqualTo(ALLOWED_DOC);
        assertThat(produced.getField(upstream.findFieldByName("title")))
                .isEqualTo(ALLOWED_TITLE);
        assertThat(produced.getField(upstream.findFieldByName("internal_notes")))
                .isEqualTo("");
        assertThat(produced.getField(upstream.findFieldByName("secret_token")))
                .isEqualTo("");
    }

    private static ObjectNode inputFixture() {
        ObjectNode input = MAPPER.createObjectNode();
        input.put("doc_id", ALLOWED_DOC);
        input.put("title", ALLOWED_TITLE);
        input.put("internal_notes", EXCLUDED_SENTINEL);
        input.put("secret_token", SENSITIVE_SENTINEL);
        return input;
    }

    /**
     * One structured step fed by a typed edge: the upstream result is mapped
     * whole (both sentinels included) into the produced type, projected to the
     * grounding form that carries only the allowed fields, and validated before
     * the generation runs.
     */
    private static ObjectNode workflow() {
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", "live-structured-acceptance");
        ObjectNode schema = workflow.putObject("schema");
        ObjectNode sources = schema.putObject("sources");
        sources.put(VALIDATE, resource(VALIDATE));
        sources.put(PROJECTION, resource(PROJECTION));
        sources.put(METADATA, resource(METADATA));
        sources.put(ACCEPTANCE, resource(ACCEPTANCE));
        schema.put("root", ACCEPTANCE);
        workflow.put("inputType", UPSTREAM);
        workflow.put("deadlineMs", 600_000);
        ObjectNode step = workflow.putArray("steps").addObject();
        step.put("name", STEP);
        ObjectNode structured = step.putObject("structured");
        structured.put("targetType", SUMMARY);
        structured.put("model", MODEL_ID);
        ObjectNode edge = step.putObject("edge");
        edge.putArray("sources").add("input");
        edge.put("produceType", UPSTREAM);
        edge.putArray("rules")
                .add("doc_id = input.doc_id")
                .add("title = input.title")
                .add("internal_notes = input.internal_notes")
                .add("secret_token = input.secret_token");
        edge.put("projectTo", GROUNDING);
        edge.put("validate", true);
        return workflow;
    }

    private static String resource(String name) {
        try (InputStream in = LiveStructuredInferenceIT.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One workbench verb call: compile-workflow, record-workflow-run, or replay-workflow. */
    private interface Surface {
        JsonNode tool(String name, ObjectNode arguments) throws Exception;
    }

    /** The typed gRPC surface: dynamic action dispatch against ProtoMoltService. */
    private static final class TypedGrpcSurface implements Surface {
        private final ManagedChannel channel;

        private TypedGrpcSurface(ManagedChannel channel) {
            this.channel = channel;
        }

        @Override
        public JsonNode tool(String name, ObjectNode arguments) throws Exception {
            com.google.protobuf.Descriptors.MethodDescriptor method =
                    ProtoMoltServiceSchema.service().findMethodByName(rpc(name));
            assertThat(method).as("RPC for verb %s", name).isNotNull();
            DynamicMessage.Builder request = DynamicMessage.newBuilder(method.getInputType());
            JsonFormat.parser().merge(arguments.toString(), request);
            List<DynamicMessage> responses = DynamicGrpcCalls.call(channel, method,
                    request.build(),
                    CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.MINUTES),
                    new Metadata(), 4);
            assertThat(responses).hasSize(1);
            return MAPPER.readTree(JsonFormat.printer().print(responses.getFirst()));
        }

        /** 'compile-workflow' becomes 'CompileWorkflow'. */
        private static String rpc(String verb) {
            StringBuilder rpc = new StringBuilder(verb.length());
            for (String part : verb.split("-")) {
                rpc.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
            return rpc.toString();
        }
    }

    /** The MCP surface: streamable-HTTP tools/call, per the Phase 2 acceptance. */
    private static final class McpSurface implements Surface, AutoCloseable {
        private final HttpClient http = HttpClient.newHttpClient();
        private final URI endpoint;
        private final String session;
        private final String version;

        private McpSurface(int port) throws Exception {
            endpoint = URI.create("http://127.0.0.1:" + port + "/mcp");
            ObjectNode params = MAPPER.createObjectNode();
            params.put("protocolVersion", "2025-06-18");
            params.putObject("capabilities");
            params.putObject("clientInfo")
                    .put("name", "live-structured-acceptance").put("version", "0");
            HttpResponse<String> response = send(null, null,
                    request(99, "initialize", params));
            assertThat(response.statusCode()).isEqualTo(200);
            session = response.headers().firstValue("Mcp-Session-Id").orElseThrow();
            version = MAPPER.readTree(response.body()).path("result")
                    .path("protocolVersion").asText();
            assertThat(send(session, version,
                    notification("notifications/initialized")).statusCode()).isEqualTo(202);
        }

        @Override
        public JsonNode tool(String name, ObjectNode arguments) throws Exception {
            ObjectNode params = MAPPER.createObjectNode();
            params.put("name", name);
            params.set("arguments", arguments);
            JsonNode result = MAPPER.readTree(send(session, version,
                    request(2, "tools/call", params)).body()).path("result");
            assertThat(result.path("isError").asBoolean())
                    .as(() -> result.path("structuredContent").toString()).isFalse();
            return result.path("structuredContent");
        }

        private HttpResponse<String> send(String sessionId, String protocolVersion,
                                          ObjectNode body) throws Exception {
            HttpRequest.Builder request = HttpRequest.newBuilder(endpoint)
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
            if (sessionId != null) {
                request.header("Mcp-Session-Id", sessionId);
            }
            if (protocolVersion != null) {
                request.header("MCP-Protocol-Version", protocolVersion);
            }
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        private ObjectNode request(int id, String method, ObjectNode params) {
            ObjectNode request = notification(method);
            request.put("id", id);
            request.set("params", params);
            return request;
        }

        private ObjectNode notification(String method) {
            return MAPPER.createObjectNode().put("jsonrpc", "2.0").put("method", method);
        }

        @Override
        public void close() {
            // The server owns the MCP session lifecycle; closing the server releases it.
        }
    }

    /**
     * The privacy oracle: an in-process reverse proxy the catalog model points
     * at. Every outbound request body is recorded before the request is
     * forwarded verbatim to the configured live endpoint, so the test proves
     * exactly what would have left the process.
     */
    private static final class CapturingProxy implements AutoCloseable {
        private final HttpServer server;
        private final HttpClient client = HttpClient.newHttpClient();
        private final String target;
        private final List<String> bodies = new CopyOnWriteArrayList<>();
        private final List<String> authorizations = new CopyOnWriteArrayList<>();

        private CapturingProxy(String endpoint) throws IOException {
            target = endpoint.endsWith("/")
                    ? endpoint.substring(0, endpoint.length() - 1)
                    : endpoint;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::forward);
            server.start();
        }

        private int port() {
            return server.getAddress().getPort();
        }

        private List<String> bodies() {
            return List.copyOf(bodies);
        }

        private List<String> authorizations() {
            return List.copyOf(authorizations);
        }

        private void forward(HttpExchange exchange) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            bodies.add(new String(body, StandardCharsets.UTF_8));
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization != null) {
                authorizations.add(authorization);
            }
            HttpRequest.Builder forwarded = HttpRequest.newBuilder(
                            URI.create(target + exchange.getRequestURI()))
                    .timeout(Duration.ofMinutes(10))
                    .header("Content-Type", "application/json");
            if (authorization != null) {
                forwarded.header("Authorization", authorization);
            }
            HttpRequest request = forwarded
                    .method(exchange.getRequestMethod(),
                            HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            try {
                HttpResponse<byte[]> response = client.send(request,
                        HttpResponse.BodyHandlers.ofByteArray());
                byte[] responseBody = response.body();
                exchange.getResponseHeaders().add("Content-Type",
                        response.headers().firstValue("Content-Type")
                                .orElse("application/json"));
                exchange.sendResponseHeaders(response.statusCode(),
                        responseBody.length == 0 ? -1 : responseBody.length);
                if (responseBody.length > 0) {
                    exchange.getResponseBody().write(responseBody);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                byte[] failure = "{\"error\":\"proxy forwarding interrupted\"}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, failure.length);
                exchange.getResponseBody().write(failure);
            } finally {
                exchange.close();
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
