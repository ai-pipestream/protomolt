package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;

/**
 * The {@code {nativePrefix}/workflows} routes: verbatim workflow storage over a
 * {@link GitSchemaRegistryStore}, the 40401 answer a non-Git store gets, the
 * body/name validation, and the {@code check-workflow} write gate when an action
 * catalog is mounted.
 */
class WorkflowsEndpointTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String WORKFLOW_BODY = """
            {"inputType": "t.Doc", "steps": [{"name": "s1", "target": "localhost:9", "method": "t.Svc/Run"}]}
            """;

    @TempDir
    Path tempDir;

    private HttpClient client;

    @BeforeEach
    void startClient() {
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopClient() {
        client.close();
    }

    // ------------------------------------------------------------------ non-Git store

    @Test
    void aNonGitStoreAnswers40401OnEveryWorkflowRoute() throws Exception {
        try (InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store)) {
            String base = "http://127.0.0.1:" + server.start();
            for (HttpResponse<String> response : new HttpResponse[]{
                    get(base + "/protomolt/workflows"),
                    get(base + "/protomolt/workflows/pipe"),
                    put(base + "/protomolt/workflows/pipe", WORKFLOW_BODY)}) {
                assertThat(response.statusCode()).isEqualTo(404);
                JsonNode error = JSON.readTree(response.body());
                assertThat(error.path("error_code").asInt()).isEqualTo(40401);
                assertThat(error.path("message").asText()).contains("does not hold workflows");
            }
        }
    }

    // ------------------------------------------------------------------ storage round trip

    @Test
    void workflowsPutGetAndListRoundTripVerbatim() throws Exception {
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store)) {
            String base = "http://127.0.0.1:" + server.start();

            HttpResponse<String> put = put(base + "/protomolt/workflows/pipe", WORKFLOW_BODY);
            assertThat(put.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(put.body()).path("name").asText()).isEqualTo("pipe");

            HttpResponse<String> got = get(base + "/protomolt/workflows/pipe");
            assertThat(got.statusCode()).isEqualTo(200);
            assertThat(got.headers().firstValue("Content-Type")).contains("application/json");
            // The PUT stores the parsed workflow's compact form (ObjectNode.toString());
            // the GET serves that stored document verbatim rather than reparsing it.
            assertThat(got.body()).isEqualTo(JSON.readTree(WORKFLOW_BODY).toString());

            assertThat(put(base + "/protomolt/workflows/alpha", WORKFLOW_BODY).statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(get(base + "/protomolt/workflows").body()))
                    .extracting(JsonNode::asText)
                    .containsExactly("alpha", "pipe");
        }
    }

    @Test
    void anUnknownWorkflowIs40401() throws Exception {
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store)) {
            String base = "http://127.0.0.1:" + server.start();
            HttpResponse<String> response = get(base + "/protomolt/workflows/ghost");
            assertThat(response.statusCode()).isEqualTo(404);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(40401);
            assertThat(error.path("message").asText()).contains("ghost");
        }
    }

    @Test
    void aNonObjectWorkflowBodyIs42201() throws Exception {
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store)) {
            String base = "http://127.0.0.1:" + server.start();
            for (String body : new String[]{"[1,2]", "not json at all"}) {
                HttpResponse<String> response = put(base + "/protomolt/workflows/pipe", body);
                assertThat(response.statusCode()).as(body).isEqualTo(422);
                assertThat(JSON.readTree(response.body()).path("error_code").asInt())
                        .isEqualTo(42201);
            }
        }
    }

    @Test
    void anInvalidWorkflowNameIs42201() throws Exception {
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store)) {
            String base = "http://127.0.0.1:" + server.start();
            // Workflow names are [A-Za-z0-9._-]; the decoded segment reaches the store's guard.
            HttpResponse<String> response = put(base + "/protomolt/workflows/bad%20name", WORKFLOW_BODY);
            assertThat(response.statusCode()).isEqualTo(422);
            assertThat(JSON.readTree(response.body()).path("error_code").asInt()).isEqualTo(42201);
        }
    }

    // ------------------------------------------------------------------ the check-workflow gate

    @Test
    void aFailingCheckWorkflowBlocksTheWriteWith42202() throws Exception {
        ActionCatalog actions = ActionCatalog.defaults(ActionContext.create())
                .register(checkWorkflow(false));
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store, actions)) {
            String base = "http://127.0.0.1:" + server.start();

            HttpResponse<String> response = put(base + "/protomolt/workflows/pipe", WORKFLOW_BODY);
            assertThat(response.statusCode()).isEqualTo(422);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(42202);
            assertThat(error.path("findings").isArray()).isTrue();

            // Nothing was committed behind the failed gate.
            assertThat(get(base + "/protomolt/workflows/pipe").statusCode()).isEqualTo(404);
        }
    }

    @Test
    void aThrowingCheckWorkflowAlsoBlocksTheWrite() throws Exception {
        ProtoAction broken = new ProtoAction() {
            @Override
            public String name() {
                return "check-workflow";
            }

            @Override
            public String description() {
                return "test double that always throws";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
                throw new ActionException("verify-broken", "kaboom in the verifier");
            }
        };
        ActionCatalog actions = ActionCatalog.defaults(ActionContext.create()).register(broken);
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store, actions)) {
            String base = "http://127.0.0.1:" + server.start();

            HttpResponse<String> response = put(base + "/protomolt/workflows/pipe", WORKFLOW_BODY);
            assertThat(response.statusCode()).isEqualTo(422);
            JsonNode error = JSON.readTree(response.body());
            assertThat(error.path("error_code").asInt()).isEqualTo(42202);
            assertThat(error.path("message").asText()).contains("kaboom in the verifier");
            assertThat(get(base + "/protomolt/workflows/pipe").statusCode()).isEqualTo(404);
        }
    }

    @Test
    void aPassingCheckWorkflowAdmitsTheWrite() throws Exception {
        ActionCatalog actions = ActionCatalog.defaults(ActionContext.create())
                .register(checkWorkflow(true));
        try (GitSchemaRegistryStore store = gitStore();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                        store, actions)) {
            String base = "http://127.0.0.1:" + server.start();
            assertThat(put(base + "/protomolt/workflows/pipe", WORKFLOW_BODY).statusCode()).isEqualTo(200);
            assertThat(get(base + "/protomolt/workflows/pipe").body())
                    .isEqualTo(JSON.readTree(WORKFLOW_BODY).toString());
        }
    }

    // ------------------------------------------------------------------ helpers

    private GitSchemaRegistryStore gitStore() {
        return GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("registry-repo"))
                .build();
    }

    /** A {@code check-workflow} double returning {@code ok} with findings when it fails. */
    private static ProtoAction checkWorkflow(boolean ok) {
        return new ProtoAction() {
            @Override
            public String name() {
                return "check-workflow";
            }

            @Override
            public String description() {
                return "test double for the workflow write gate";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                ObjectNode out = JSON.createObjectNode().put("ok", ok);
                if (!ok) {
                    out.putArray("findings").addObject().put("message", "step s1 has no target");
                }
                return out;
            }
        };
    }

    private HttpResponse<String> get(String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> put(String url, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
