package ai.protomolt.proto.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.grpc.health.v1.HealthProto;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Resource reads under a scoped caller (#822): {@code resources/list} and
 * {@code resources/templates/list} serve only the collections whose scope the caller
 * holds, {@code resources/read} outside that scope is an explicit permission-denied
 * error carrying no contents, and the workspace bootstrap stays readable with the
 * caller's own filtered tool inventory.
 */
class McpResourceScopeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop;
            message Order { string id = 1; }
            """;

    @TempDir
    Path directory;

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private McpServer server;

    @BeforeEach
    void setUp() throws Exception {
        InMemorySchemaRegistryStore store = new InMemorySchemaRegistryStore();
        store.register("orders-value", ORDER_PROTO, List.of());

        FileSystemServiceProfileRepository profiles =
                new FileSystemServiceProfileRepository(directory);
        byte[] descriptors = FileDescriptorSet.newBuilder()
                .addFile(HealthProto.getDescriptor().toProto())
                .build().toByteArray();
        String fingerprint = ServiceProfileValidation.sha256(descriptors);
        profiles.saveDescriptorArtifact(DescriptorArtifact.newBuilder()
                .setFingerprint(fingerprint)
                .setDescriptorSet(ByteString.copyFrom(descriptors))
                .build());
        profiles.save(ServiceProfile.newBuilder()
                .setName("health-local")
                .setDescription("Local health service")
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("local")
                        .setHost("localhost")
                        .setPort(50051)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT))
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_REFLECTION)
                        .setSourceRef("grpc-reflection:local")
                        .setDescriptorFingerprint(fingerprint)
                        .setDescriptorArtifactRef("sha256:" + fingerprint))
                .build());

        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);

        server = new McpServer(
                ActionCatalog.defaults(ActionContext.create()),
                CompositeResources.of(
                        new RegistryResources(store),
                        new ServiceProfileResources(profiles, store),
                        new DelegationResources(bridge)),
                "test", "0");
    }

    @AfterEach
    void tearDown() {
        bridge.close();
        coordinator.close();
    }

    private static ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = MAPPER.createObjectNode()
                .put("jsonrpc", "2.0").put("id", id).put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }

    private McpServer.Session ready(Caller caller) {
        McpServer.Session session = server.openSession(caller);
        ObjectNode initialize = MAPPER.createObjectNode();
        initialize.put("protocolVersion", "2025-06-18");
        session.handle(request(1, "initialize", initialize)).orElseThrow();
        session.handle(MAPPER.createObjectNode()
                .put("jsonrpc", "2.0").put("method", "notifications/initialized"));
        return session;
    }

    private static ObjectNode read(McpServer.Session session, int id, String uri) {
        ObjectNode params = MAPPER.createObjectNode().put("uri", uri);
        return session.handle(request(id, "resources/read", params)).orElseThrow();
    }

    @Test
    void schemaReaderListsAndReadsOnlyRegistryResources() {
        McpServer.Session session =
                ready(Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ)));

        ObjectNode listed = session.handle(request(2, "resources/list",
                MAPPER.createObjectNode())).orElseThrow();
        assertThat(listed.path("result").path("resources").findValuesAsText("uri"))
                .contains("protomolt://workspace", "protomolt://registry/subjects",
                        "protomolt://registry/subjects/orders-value")
                .doesNotContain("protomolt://services",
                        "protomolt://services/health-local",
                        DelegationResources.WORKERS_URI, DelegationResources.TASKS_URI);

        ObjectNode templates = session.handle(request(3, "resources/templates/list",
                MAPPER.createObjectNode())).orElseThrow();
        assertThat(templates.path("result").path("resourceTemplates")
                .findValuesAsText("uriTemplate"))
                .containsExactly(
                        "protomolt://registry/subjects/{subject}",
                        "protomolt://registry/subjects/{subject}/versions/{version}");

        ObjectNode subject = read(session, 4, "protomolt://registry/subjects/orders-value");
        assertThat(subject.path("result").path("contents").get(0).path("text").asText())
                .contains("message Order");

        ObjectNode deniedDelegation = read(session, 5, DelegationResources.TASKS_URI);
        assertThat(deniedDelegation.has("result")).isFalse();
        assertThat(deniedDelegation.path("error").path("message").asText())
                .contains("permission-denied").contains("ci-reader")
                .contains(Scopes.WORKER_COORDINATE);
        assertThat(deniedDelegation.toString()).doesNotContain("contents");

        ObjectNode deniedServices = read(session, 6, "protomolt://services");
        assertThat(deniedServices.has("result")).isFalse();
        assertThat(deniedServices.path("error").path("message").asText())
                .contains("permission-denied").contains(Scopes.SERVICE_INVOKE);
        assertThat(deniedServices.toString())
                .doesNotContain("health-local").doesNotContain("contents");
    }

    @Test
    void coordinatorReadsDelegationButNotTheRegistry() {
        McpServer.Session session =
                ready(Caller.scoped("mesh-node", Set.of(Scopes.WORKER_COORDINATE)));

        ObjectNode tasks = read(session, 2, DelegationResources.TASKS_URI);
        assertThat(tasks.path("result").path("contents").get(0).path("text").asText())
                .contains("\"tasks\"");

        ObjectNode denied = read(session, 3, "protomolt://registry/subjects/orders-value");
        assertThat(denied.has("result")).isFalse();
        assertThat(denied.path("error").path("message").asText())
                .contains("permission-denied").contains("mesh-node")
                .contains(Scopes.SCHEMA_READ);
        assertThat(denied.toString())
                .doesNotContain("message Order").doesNotContain("schemaText")
                .doesNotContain("contents");
    }

    @Test
    void operatorStillReadsEveryCollection() {
        McpServer.Session session = ready(Caller.operator());
        assertThat(read(session, 2, "protomolt://registry/subjects/orders-value")
                .path("result").path("contents").get(0).path("text").asText())
                .contains("message Order");
        assertThat(read(session, 3, "protomolt://services")
                .path("result").path("contents").get(0).path("text").asText())
                .contains("health-local");
        assertThat(read(session, 4, DelegationResources.WORKERS_URI)
                .path("result").path("contents").get(0).path("text").asText())
                .contains("\"workers\"");
    }

    @Test
    void workspaceShowsTheCallersOwnToolInventory() throws Exception {
        McpServer.Session session =
                ready(Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)));
        ObjectNode tools = session.handle(request(2, "tools/list", null)).orElseThrow();
        int visibleTools = tools.path("result").path("tools").size();

        ObjectNode workspace = read(session, 3, "protomolt://workspace");
        var document = MAPPER.readTree(
                workspace.path("result").path("contents").get(0).path("text").asText());
        assertThat(document.path("toolCatalog").path("count").asInt())
                .isEqualTo(visibleTools);
        assertThat(document.path("toolCatalog").path("names")).hasSize(visibleTools);
    }

    @Test
    void aCollectionDeclaringNoScopeIsRefusedToScopedCallersOnly() {
        McpResources undeclared = new McpResources() {
            @Override
            public ArrayNode list(ObjectMapper mapper) {
                ArrayNode resources = mapper.createArrayNode();
                ObjectNode entry = resources.addObject();
                entry.put("uri", "protomolt://undeclared/secret");
                entry.put("name", "secret");
                entry.put("description", "declares no scope");
                entry.put("mimeType", "application/json");
                return resources;
            }

            @Override
            public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
                if (!"protomolt://undeclared/secret".equals(uri)) {
                    return Optional.empty();
                }
                ObjectNode contents = mapper.createObjectNode();
                contents.put("uri", uri);
                contents.put("mimeType", "application/json");
                contents.put("text", "{\"secret\":true}");
                return Optional.of(contents);
            }
        };
        McpServer undeclaredServer = new McpServer(
                ActionCatalog.defaults(ActionContext.create()), undeclared, "test", "0");

        McpServer.Session scoped = readyOn(undeclaredServer,
                Caller.scoped("ci-reader", Set.of(Scopes.SCHEMA_READ)));
        ObjectNode listed = scoped.handle(request(2, "resources/list",
                MAPPER.createObjectNode())).orElseThrow();
        assertThat(listed.path("result").path("resources").findValuesAsText("uri"))
                .doesNotContain("protomolt://undeclared/secret");
        ObjectNode denied = read(scoped, 3, "protomolt://undeclared/secret");
        assertThat(denied.has("result")).isFalse();
        assertThat(denied.path("error").path("message").asText())
                .contains("permission-denied");
        assertThat(denied.toString()).doesNotContain("secret\":true");

        McpServer.Session operator = readyOn(undeclaredServer, Caller.operator());
        assertThat(read(operator, 2, "protomolt://undeclared/secret")
                .path("result").path("contents").get(0).path("text").asText())
                .contains("secret");
    }

    private static McpServer.Session readyOn(McpServer target, Caller caller) {
        McpServer.Session session = target.openSession(caller);
        ObjectNode initialize = MAPPER.createObjectNode();
        initialize.put("protocolVersion", "2025-06-18");
        session.handle(request(1, "initialize", initialize)).orElseThrow();
        session.handle(MAPPER.createObjectNode()
                .put("jsonrpc", "2.0").put("method", "notifications/initialized"));
        return session;
    }
}
