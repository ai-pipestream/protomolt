package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct HTTP tests of every {@link SchemaRegistryServer} endpoint: envelopes, Confluent error
 * codes, the config GET/PUT key quirk, URL-encoded subjects, the descriptor-set extra and
 * health. The publisher/loader round trip lives in {@link SchemaRegistryServerRoundTripTest}.
 */
class SchemaRegistryServerHttpTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private static final String CORE_SUBJECT = "common/v1/core.proto";
    private static final String USER_SUBJECT = "common/v1/user.proto";

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

    private SchemaRegistryStore store;
    private SchemaRegistryServer server;
    private HttpClient client;
    private URI base;

    @BeforeEach
    void startServer() {
        store = new InMemorySchemaRegistryStore();
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store);
        base = URI.create("http://127.0.0.1:" + server.start());
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopServer() {
        client.close();
        server.close();
        store.close();
    }

    // ---------------------------------------------------------------- happy paths

    @Test
    void healthReportsUp() throws Exception {
        HttpResponse<String> response = get("/health");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(response.body()).path("status").asText()).isEqualTo("UP");
    }

    @Test
    void subjectsListsRegisteredSubjectsSorted() throws Exception {
        assertThat(JSON.readTree(get("/subjects").body())).isEmpty();
        registerOk("b.proto", CORE_PROTO);
        registerOk("a.proto", CORE_PROTO);

        HttpResponse<String> response = get("/subjects");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).contains(CONTENT_TYPE);
        assertThat(JSON.readTree(response.body()))
                .extracting(JsonNode::asText)
                .containsExactly("a.proto", "b.proto");
    }

    @Test
    void registerReturnsGlobalIdAndVersionEnvelopeOmitsEmptyReferences() throws Exception {
        HttpResponse<String> registered = post("/subjects/" + encode(CORE_SUBJECT) + "/versions",
                registerBody(CORE_PROTO));
        assertThat(registered.statusCode()).isEqualTo(200);
        int id = JSON.readTree(registered.body()).path("id").asInt();
        assertThat(id).isEqualTo(1);

        assertThat(JSON.readTree(get("/subjects/" + encode(CORE_SUBJECT) + "/versions").body()))
                .extracting(JsonNode::asInt)
                .containsExactly(1);

        for (String version : List.of("1", "latest")) {
            JsonNode envelope = JSON.readTree(
                    get("/subjects/" + encode(CORE_SUBJECT) + "/versions/" + version).body());
            assertThat(envelope.path("subject").asText()).isEqualTo(CORE_SUBJECT);
            assertThat(envelope.path("id").asInt()).isEqualTo(id);
            assertThat(envelope.path("version").asInt()).isEqualTo(1);
            assertThat(envelope.path("schemaType").asText()).isEqualTo("PROTOBUF");
            assertThat(envelope.path("schema").asText()).isEqualTo(CORE_PROTO);
            assertThat(envelope.has("references")).as("empty references are omitted").isFalse();
        }
    }

    @Test
    void versionEnvelopeCarriesReferences() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);
        registerOk(USER_SUBJECT, USER_PROTO, new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1));

        JsonNode envelope = JSON.readTree(
                get("/subjects/" + encode(USER_SUBJECT) + "/versions/latest").body());
        JsonNode references = envelope.path("references");
        assertThat(references).hasSize(1);
        assertThat(references.get(0).path("name").asText()).isEqualTo(CORE_SUBJECT);
        assertThat(references.get(0).path("subject").asText()).isEqualTo(CORE_SUBJECT);
        assertThat(references.get(0).path("version").asInt()).isEqualTo(1);
    }

    @Test
    void identicalRegistrationIsIdempotent() throws Exception {
        int first = registerOk(CORE_SUBJECT, CORE_PROTO);
        int second = registerOk(CORE_SUBJECT, CORE_PROTO);
        assertThat(second).isEqualTo(first);
        assertThat(store.versions(CORE_SUBJECT)).containsExactly(1);
    }

    @Test
    void lookupByContentReturnsTheMatchingVersionEnvelope() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);
        registerOk(CORE_SUBJECT, CORE_PROTO + "// v2\n");

        HttpResponse<String> match = post("/subjects/" + encode(CORE_SUBJECT), registerBody(CORE_PROTO));
        assertThat(match.statusCode()).isEqualTo(200);
        JsonNode envelope = JSON.readTree(match.body());
        assertThat(envelope.path("version").asInt()).isEqualTo(1);
        assertThat(envelope.path("schema").asText()).isEqualTo(CORE_PROTO);
    }

    @Test
    void schemasByIdReturnsSchemaTypeAndReferences() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);
        int userId = registerOk(USER_SUBJECT, USER_PROTO,
                new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1));

        JsonNode byId = JSON.readTree(get("/schemas/ids/" + userId).body());
        assertThat(byId.path("schema").asText()).isEqualTo(USER_PROTO);
        assertThat(byId.path("schemaType").asText()).isEqualTo("PROTOBUF");
        assertThat(byId.path("references")).hasSize(1);

        JsonNode coreById = JSON.readTree(get("/schemas/ids/1").body());
        assertThat(coreById.has("references")).isFalse();
    }

    // ---------------------------------------------------------------- error codes

    @Test
    void unknownSubjectIs40401Everywhere() throws Exception {
        for (HttpResponse<String> response : List.of(
                get("/subjects/none/versions"),
                get("/subjects/none/versions/latest"),
                post("/subjects/none", registerBody(CORE_PROTO)),
                get("/protomolt/subjects/none/descriptor-set"))) {
            assertThat(response.statusCode()).isEqualTo(404);
            JsonNode body = JSON.readTree(response.body());
            assertThat(body.path("error_code").asInt()).isEqualTo(40401);
            assertThat(body.path("message").asText()).contains("none");
        }
    }

    @Test
    void unknownVersionIs40402AndMalformedVersionIs42202() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);

        HttpResponse<String> missing = get("/subjects/" + encode(CORE_SUBJECT) + "/versions/9");
        assertThat(missing.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(missing.body()).path("error_code").asInt()).isEqualTo(40402);

        HttpResponse<String> malformed = get("/subjects/" + encode(CORE_SUBJECT) + "/versions/abc");
        assertThat(malformed.statusCode()).isEqualTo(422);
        assertThat(JSON.readTree(malformed.body()).path("error_code").asInt()).isEqualTo(42202);
    }

    @Test
    void lookupOfUnknownContentIs40403() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);
        HttpResponse<String> response = post("/subjects/" + encode(CORE_SUBJECT),
                registerBody(CORE_PROTO + "// different\n"));
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(40403);
    }

    @Test
    void unknownSchemaIdIs40403() throws Exception {
        HttpResponse<String> response = get("/schemas/ids/99");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(40403);
    }

    @Test
    void nonProtobufSchemaTypeIsRejectedWith422() throws Exception {
        ObjectNode body = JSON.createObjectNode()
                .put("schema", "{\"type\":\"record\"}")
                .put("schemaType", "AVRO");
        HttpResponse<String> response =
                post("/subjects/" + encode(CORE_SUBJECT) + "/versions", body.toString());
        assertThat(response.statusCode()).isEqualTo(422);
        JsonNode error = JSON.readTree(response.body());
        assertThat(error.path("error_code").asInt()).isEqualTo(42201);
        assertThat(error.path("message").asText()).contains("AVRO");
    }

    @Test
    void unparseableSchemaIs42201() throws Exception {
        HttpResponse<String> response = post("/subjects/" + encode(CORE_SUBJECT) + "/versions",
                registerBody("not a proto {"));
        assertThat(response.statusCode()).isEqualTo(422);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(42201);
    }

    @Test
    void unknownReferenceIs422NamingTheReference() throws Exception {
        HttpResponse<String> response = post("/subjects/" + encode(USER_SUBJECT) + "/versions",
                registerBody(USER_PROTO, new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1)));
        assertThat(response.statusCode()).isEqualTo(422);
        JsonNode error = JSON.readTree(response.body());
        assertThat(error.path("error_code").asInt()).isEqualTo(42201);
        assertThat(error.path("message").asText()).contains(CORE_SUBJECT);
    }

    @Test
    void incompatibleRegistrationIs409WithViolations() throws Exception {
        SchemaRegistryStore.WriteGate rejecting = (subject, mode, history, schemaText, references, s) ->
                history.isEmpty() ? List.of() : List.of("field 1 changed type");
        try (SchemaRegistryStore gated = new InMemorySchemaRegistryStore(rejecting);
                SchemaRegistryServer gatedServer = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), gated)) {
            URI gatedBase = URI.create("http://127.0.0.1:" + gatedServer.start());

            HttpResponse<String> first = send(HttpRequest.newBuilder(
                            gatedBase.resolve("/subjects/" + encode(CORE_SUBJECT) + "/versions"))
                    .header("Content-Type", CONTENT_TYPE)
                    .POST(HttpRequest.BodyPublishers.ofString(registerBody(CORE_PROTO))).build());
            assertThat(first.statusCode()).isEqualTo(200);

            HttpResponse<String> second = send(HttpRequest.newBuilder(
                            gatedBase.resolve("/subjects/" + encode(CORE_SUBJECT) + "/versions"))
                    .header("Content-Type", CONTENT_TYPE)
                    .POST(HttpRequest.BodyPublishers.ofString(registerBody(CORE_PROTO + "// v2\n"))).build());
            assertThat(second.statusCode()).isEqualTo(409);
            JsonNode error = JSON.readTree(second.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(409);
            assertThat(error.path("message").asText()).contains("field 1 changed type");
        }
    }

    // ---------------------------------------------------------------- config quirk

    @Test
    void globalConfigPutUsesCompatibilityAndGetUsesCompatibilityLevel() throws Exception {
        JsonNode initial = JSON.readTree(get("/config").body());
        assertThat(initial.path("compatibilityLevel").asText()).isEqualTo("BACKWARD");
        assertThat(initial.has("compatibility")).isFalse();

        HttpResponse<String> put = put("/config",
                JSON.createObjectNode().put("compatibility", "NONE").toString());
        assertThat(put.statusCode()).isEqualTo(200);
        JsonNode putBody = JSON.readTree(put.body());
        assertThat(putBody.path("compatibility").asText()).isEqualTo("NONE");
        assertThat(putBody.has("compatibilityLevel")).isFalse();

        assertThat(JSON.readTree(get("/config").body()).path("compatibilityLevel").asText())
                .isEqualTo("NONE");
    }

    @Test
    void subjectConfigFollowsTheSameQuirkAnd40408WhenUnset() throws Exception {
        HttpResponse<String> unset = get("/config/" + encode(CORE_SUBJECT));
        assertThat(unset.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(unset.body()).path("error_code").asInt()).isEqualTo(40408);

        HttpResponse<String> put = put("/config/" + encode(CORE_SUBJECT),
                JSON.createObjectNode().put("compatibility", "FULL").toString());
        assertThat(put.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(put.body()).path("compatibility").asText()).isEqualTo("FULL");

        assertThat(JSON.readTree(get("/config/" + encode(CORE_SUBJECT)).body())
                .path("compatibilityLevel").asText()).isEqualTo("FULL");
    }

    @Test
    void invalidCompatibilityLevelIs42203() throws Exception {
        for (HttpResponse<String> response : List.of(
                put("/config", JSON.createObjectNode().put("compatibility", "SIDEWAYS").toString()),
                put("/config/" + encode(CORE_SUBJECT), "{}"))) {
            assertThat(response.statusCode()).isEqualTo(422);
            assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(42203);
        }
    }

    // ---------------------------------------------------------------- subjects with slashes

    @Test
    void urlEncodedSubjectsWithSlashesRoundTrip() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);

        // The encoded path reaches the same subject...
        assertThat(get("/subjects/" + encode(CORE_SUBJECT) + "/versions/latest").statusCode())
                .isEqualTo(200);
        // ...and the subject list carries the decoded name.
        assertThat(JSON.readTree(get("/subjects").body()))
                .extracting(JsonNode::asText)
                .containsExactly(CORE_SUBJECT);
        assertThat(encode(CORE_SUBJECT)).contains("%2F"); // sanity: the test really encodes
    }

    // ---------------------------------------------------------------- native extras

    @Test
    void descriptorSetCompilesSubjectWithTransitiveReferences() throws Exception {
        registerOk(CORE_SUBJECT, CORE_PROTO);
        registerOk(USER_SUBJECT, USER_PROTO, new SchemaReference(CORE_SUBJECT, CORE_SUBJECT, 1));

        HttpResponse<byte[]> response = client.send(
                HttpRequest.newBuilder(base.resolve(
                        "/protomolt/subjects/" + encode(USER_SUBJECT) + "/descriptor-set")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).contains("application/x-protobuf");

        FileDescriptorSet set = FileDescriptorSet.parseFrom(response.body());
        List<String> files = set.getFileList().stream().map(FileDescriptorProto::getName).toList();
        // Path-like subjects keep their true import paths in the served set.
        assertThat(files).contains(USER_SUBJECT, CORE_SUBJECT);
        FileDescriptorProto user = set.getFileList().stream()
                .filter(file -> file.getName().equals(USER_SUBJECT))
                .findFirst().orElseThrow();
        assertThat(user.getMessageTypeList().getFirst().getName()).isEqualTo("User");

        // Dependencies before dependents: consumers link the set in one forward pass.
        for (int i = 0; i < set.getFileCount(); i++) {
            for (String dependency : set.getFile(i).getDependencyList()) {
                assertThat(files.indexOf(dependency))
                        .as("dependency %s of %s appears earlier", dependency, files.get(i))
                        .isLessThan(i);
            }
        }
    }

    @Test
    void methodNotAllowedIs405WithAllowHeader() throws Exception {
        HttpResponse<String> response = send(HttpRequest.newBuilder(base.resolve("/subjects"))
                .DELETE().build());
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow")).contains("GET");
    }

    @Test
    void unknownRouteIs404Json() throws Exception {
        HttpResponse<String> response = get("/nope/nothing");
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(404);
    }

    // ---------------------------------------------------------------- helpers

    private int registerOk(String subject, String schema, SchemaReference... references)
            throws Exception {
        HttpResponse<String> response = post("/subjects/" + encode(subject) + "/versions",
                registerBody(schema, references));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JSON.readTree(response.body()).path("id").asInt();
    }

    private static String registerBody(String schema, SchemaReference... references) {
        ObjectNode node = JSON.createObjectNode()
                .put("schema", schema)
                .put("schemaType", "PROTOBUF");
        if (references.length > 0) {
            ArrayNode array = node.putArray("references");
            for (SchemaReference reference : references) {
                array.addObject()
                        .put("name", reference.name())
                        .put("subject", reference.subject())
                        .put("version", reference.version());
            }
        }
        return node.toString();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(HttpRequest.newBuilder(base.resolve(path))
                .header("Accept", CONTENT_TYPE + ", application/json")
                .GET().build());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(base.resolve(path))
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private HttpResponse<String> put(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(base.resolve(path))
                .header("Content-Type", CONTENT_TYPE)
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String encode(String subject) {
        return URLEncoder.encode(subject, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
