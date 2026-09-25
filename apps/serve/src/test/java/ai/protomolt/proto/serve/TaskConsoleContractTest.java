package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.Timestamps;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.UUID;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Caller descriptors cross the browser boundary and retain their own interpretation. */
class TaskConsoleContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private DelegationBridge bridge;
    private HttpServer server;
    private String base;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws Exception {
        bridge = new DelegationBridge(new InProcessDelegationCoordinator());
        bridge.registerWorker(WorkerHello.newBuilder().setWorkerId("caller-worker")
                .setProtocolVersion(1).build());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/tasks", new TaskConsoleApiHandler(bridge, TaskConsoleSessions.open()));
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
        bridge.close();
    }

    @Test
    void callerDefinedResultsRenderUsingTheirOwnOfferIncludingOlderAttempts() throws Exception {
        FileDescriptor first = descriptor("headline");
        String taskId = offer(first);
        complete(taskId, 1, first, "first report");
        assertThat(post("/" + taskId + "/cancel", "{\"reason\":\"change the contract\"}").statusCode())
                .isEqualTo(200);
        FileDescriptor second = descriptor("conclusion");
        ObjectNode next = offerBody(second).put("taskId", taskId);
        assertThat(post("/offer", next.toString()).statusCode()).isEqualTo(201);
        complete(taskId, 2, second, "replacement report");
        // Another task can declare a different shape with the same fully-qualified name.
        String other = offer(first);
        complete(other, 1, first, "other task");
        JsonNode detail = get("/" + taskId);
        assertThat(detail.path("events").toString()).contains("\"headline\":\"first report\"")
                .contains("\"conclusion\":\"replacement report\"");
        assertThat(get("/" + other).path("events").toString()).contains("\"headline\":\"other task\"");
        assertThat(get("/events?timeoutMs=0").path("events").toString())
                .contains("replacement report", "other task");
        assertThat(detail.toString()).contains("jsonSchema");
    }

    @Test
    void refusesCallerSchemaMissingImportsMalformedBytesAndOversizedOffersBeforePersistence() throws Exception {
        ObjectNode body = offerBody(descriptor("headline"));
        ((ObjectNode) body.path("contract")).put("jsonSchema", "{}");
        assertThat(post("/offer", body.toString()).statusCode()).isEqualTo(400);
        ((ObjectNode) body.path("contract")).remove("jsonSchema");
        ((ObjectNode) body.path("contract")).put("descriptorSet", "not-base64");
        assertThat(post("/offer", body.toString()).statusCode()).isEqualTo(400);
        FileDescriptorProto missing = descriptor("headline").toProto().toBuilder()
                .addDependency("missing.proto").build();
        ((ObjectNode) body.path("contract")).put("descriptorSet", Base64.getEncoder().encodeToString(
                FileDescriptorSet.newBuilder().addFile(missing).build().toByteArray()));
        assertThat(post("/offer", body.toString()).body()).contains("missing.proto");
        assertThat(post("/offer", " ".repeat(1024 * 1024 + 1)).statusCode()).isEqualTo(413);
        assertThat(bridge.coordinator().state().tasks()).isEmpty();
    }

    @Test
    void onlyOfferAllowsTheLargerDescriptorBody() throws Exception {
        FileDescriptorProto padded = descriptor("headline").toProto().toBuilder()
                .setSourceCodeInfo(SourceCodeInfo.newBuilder().addLocation(
                        SourceCodeInfo.Location.newBuilder().setLeadingComments("x".repeat(30_000))))
                .build();
        FileDescriptor descriptor = FileDescriptor.buildFrom(padded, new FileDescriptor[0]);
        String taskId = offer(descriptor);
        assertThat(post("/" + taskId + "/messages", " ".repeat(21 * 1024)).statusCode()).isEqualTo(413);
    }

    @Test
    void recoveryKeepsProtocolContextAndUsesOnlyARecordedCheckpointOnANewAttempt() throws Exception {
        String taskId = UUID.randomUUID().toString();
        bridge.offer("caller-worker", taskId, TaskSpec.newBuilder().setObjective("first scope")
                .addConstraints("Keep the source data intact")
                .addContext(ArtifactReference.newBuilder().setSha256("b".repeat(64))
                        .setMediaType("application/json").setSizeBytes(5))
                .addAllowedScope("old")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("old-check")).build(),
                Duration.ofMinutes(5), null);
        bridge.accept("caller-worker", taskId, 1);
        bridge.checkpoint("caller-worker", taskId, 1, "checkpoint-token", "saved", null);
        assertThat(post("/" + taskId + "/cancel", "{\"reason\":\"change scope\"}").statusCode())
                .isEqualTo(200);
        ObjectNode request = offerBody(descriptor("headline")).put("taskId", taskId);
        request.putArray("allowedScopes").add("new");
        var resume = request.putObject("resumeFrom").put("attempt", 1).put("checkpointSeq", 1)
                .put("resumeToken", "wrong-token");
        int before = bridge.coordinator().eventsAfter(taskId, 0).size();
        assertThat(post("/offer", request.toString()).statusCode()).isEqualTo(400);
        assertThat(bridge.coordinator().eventsAfter(taskId, 0)).hasSize(before);
        resume.put("resumeToken", "checkpoint-token");
        var response = post("/offer", request.toString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        assertThat(JSON.readTree(response.body()).path("attempt").asInt()).isEqualTo(2);
        var offer = bridge.coordinator().eventsAfter(taskId, 0).getLast().entry()
                .getCoordinatorFrame().getOffer();
        assertThat(offer.getSpec().getConstraintsList()).containsExactly("Keep the source data intact");
        assertThat(offer.getSpec().getContext(0).getSha256()).isEqualTo("b".repeat(64));
        assertThat(offer.getSpec().getAllowedScopeList()).containsExactly("new");
        assertThat(offer.getSpec().getRequiredChecks(0).getName()).isEqualTo("report");
        assertThat(offer.getResumeFrom().getAttempt()).isEqualTo(1);
    }

    private String offer(FileDescriptor descriptor) throws Exception {
        var response = post("/offer", offerBody(descriptor).toString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        return JSON.readTree(response.body()).path("taskId").asText();
    }

    private static ObjectNode offerBody(FileDescriptor descriptor) {
        ObjectNode body = JSON.createObjectNode().put("workerId", "caller-worker")
                .put("objective", "Return a caller-defined report");
        body.putArray("requiredChecks").addObject().put("name", "report");
        body.putObject("contract").put("typeName", "caller.Report")
                .put("descriptorSet", Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                        .addFile(descriptor.toProto()).build().toByteArray()));
        return body;
    }

    private void complete(String taskId, int attempt, FileDescriptor file, String value) {
        bridge.accept("caller-worker", taskId, attempt);
        var type = file.findMessageTypeByName("Report");
        var result = DynamicMessage.newBuilder(type).setField(type.getFields().getFirst(), value).build();
        bridge.submitCandidate("caller-worker", taskId, CompletionCandidate.newBuilder()
                .setAttempt(attempt).setRevision(1).setSummary("Caller report")
                .setResult(Any.pack(result))
                .addEvidence(CheckEvidence.newBuilder().setCheckName("report")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamps.fromMillis(System.currentTimeMillis())))
                .addArtifacts(ArtifactReference.newBuilder().setSha256("a".repeat(64))
                        .setMediaType("application/json").setSizeBytes(1)).build());
    }

    private static FileDescriptor descriptor(String field) throws Exception {
        return FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("caller.proto")
                .setPackage("caller").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder()
                        .setName("Report").addField(FieldDescriptorProto.newBuilder().setName(field)
                                .setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))).build(),
                new FileDescriptor[0]);
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks" + path))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode get(String path) throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks" + path))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }
}
