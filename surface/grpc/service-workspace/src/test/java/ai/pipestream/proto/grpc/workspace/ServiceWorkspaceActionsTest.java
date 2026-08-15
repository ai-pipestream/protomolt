package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceWorkspaceActionsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path directory;

    private String serverName;
    private Server server;
    private FileSystemServiceProfileRepository repository;
    private ActionCatalog catalog;

    @BeforeEach
    void start() throws Exception {
        serverName = "service-workspace-" + UUID.randomUUID();
        HealthStatusManager health = new HealthStatusManager();
        server = InProcessServerBuilder.forName(serverName)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        repository = new FileSystemServiceProfileRepository(directory);
        catalog = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository,
                (target, tls) -> InProcessChannelBuilder.forName(serverName).build());
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void registerInspectRefreshAndRestartWithoutReturningDescriptorBytes() throws Exception {
        ObjectNode registered = catalog.execute("service-register", registerInput(false));

        assertThat(registered.path("ok").asBoolean()).isTrue();
        assertThat(registered.toString()).doesNotContain("descriptorSetBase64");
        assertThat(registered.path("profile").path("name").asText()).isEqualTo("health-local");
        String fingerprint = registered.path("profile").path("schemaSource")
                .path("descriptorFingerprint").asText();
        assertThat(fingerprint).matches("[0-9a-f]{64}");
        assertThat(registered.path("services").findValuesAsText("name"))
                .contains("grpc.health.v1.Health", "Check");

        ObjectNode listed = catalog.execute("service-list", MAPPER.createObjectNode());
        assertThat(listed.path("services").get(0).path("name").asText())
                .isEqualTo("health-local");
        assertThat(listed.toString()).doesNotContain("descriptorSet");

        ObjectNode inspected = catalog.execute("service-inspect",
                MAPPER.createObjectNode().put("name", "health-local"));
        JsonNode check = inspected.path("services").get(0).path("methods").get(0);
        assertThat(check.path("fullName").asText()).isEqualTo("grpc.health.v1.Health/Check");
        assertThat(check.path("inputType").asText()).isEqualTo("grpc.health.v1.HealthCheckRequest");
        assertThat(check.path("inputFields").findValuesAsText("jsonName"))
                .contains("service");
        assertThat(inspected.toString()).doesNotContain("descriptorSetBase64");

        ObjectNode refreshed = catalog.execute("service-refresh",
                MAPPER.createObjectNode().put("name", "health-local"));
        assertThat(refreshed.path("ok").asBoolean()).isTrue();
        assertThat(refreshed.path("changed").asBoolean()).isFalse();

        var reopened = new FileSystemServiceProfileRepository(directory);
        assertThat(reopened.find("health-local")).isPresent().get()
                .extracting(profile -> profile.getSchemaSource().getDescriptorFingerprint())
                .isEqualTo(fingerprint);
        assertThat(reopened.findDescriptorArtifact(fingerprint)).isPresent();
    }

    @Test
    void registryBackedInvokeKeepsDescriptorBytesInsideProtoMolt() throws Exception {
        InMemorySchemaRegistryStore registry = new InMemorySchemaRegistryStore();
        ActionCatalog registryCatalog = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, registry,
                (target, tls) -> channel());
        ObjectNode registered = registryCatalog.execute("service-register", registerInput(false));
        String fingerprint = registered.path("profile").path("schemaSource")
                .path("descriptorFingerprint").asText();

        ObjectNode invocation = MAPPER.createObjectNode();
        invocation.put("name", "health-local");
        invocation.put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request").put("service", "");
        ObjectNode result = registryCatalog.execute("service-invoke", invocation);

        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(result.path("status").asText()).isEqualTo("OK");
        assertThat(result.path("serviceProfile").asText()).isEqualTo("health-local");
        assertThat(result.path("endpoint").asText()).isEqualTo("local");
        assertThat(result.path("descriptorFingerprint").asText()).isEqualTo(fingerprint);
        assertThat(result.toString()).doesNotContain("descriptorSetBase64");
        assertThat(repository.findDescriptorArtifact(fingerprint)).isEmpty();
        assertThat(registry.descriptorSet(fingerprint)).isPresent();
    }

    @Test
    void invokingAnExistingProfileMigratesItsLegacyArtifactIntoTheRegistry() throws Exception {
        ObjectNode registered = catalog.execute("service-register", registerInput(false));
        String fingerprint = registered.path("profile").path("schemaSource")
                .path("descriptorFingerprint").asText();
        InMemorySchemaRegistryStore registry = new InMemorySchemaRegistryStore();
        ActionCatalog migrated = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, registry,
                (target, tls) -> channel());

        ObjectNode invocation = MAPPER.createObjectNode();
        invocation.put("name", "health-local");
        invocation.put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request");
        assertThat(migrated.execute("service-invoke", invocation).path("ok").asBoolean()).isTrue();
        assertThat(registry.descriptorSet(fingerprint)).isPresent();
    }

    @Test
    void approvalPolicyStopsInvocationBeforeOpeningAChannel() throws Exception {
        InMemorySchemaRegistryStore registry = new InMemorySchemaRegistryStore();
        ActionCatalog registryCatalog = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, registry,
                (target, tls) -> channel());
        ObjectNode registration = registerInput(false);
        ObjectNode policy = ((ObjectNode) registration.path("profile"))
                .putArray("methodPolicies").addObject();
        policy.put("method", "grpc.health.v1.Health/Check");
        policy.putArray("operation").add("OPERATION_READ_ONLY")
                .add("OPERATION_APPROVAL_REQUIRED");
        policy.put("approvalRequired", true);
        registryCatalog.execute("service-register", registration);

        ActionCatalog guarded = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, registry,
                (target, tls) -> {
                    throw new AssertionError("approval-required method must not open a channel");
                });
        ObjectNode invocation = MAPPER.createObjectNode();
        invocation.put("name", "health-local");
        invocation.put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request");

        assertThatThrownBy(() -> guarded.execute("service-invoke", invocation))
                .isInstanceOf(ActionException.class)
                .extracting(error -> ((ActionException) error).code())
                .isEqualTo("approval-required");
    }

    @Test
    void refusesToUseOpaqueCredentialReferencesWithoutAResolver() throws Exception {
        assertThatThrownBy(() -> catalog.execute("service-register", registerInput(true)))
                .isInstanceOf(ActionException.class)
                .extracting(error -> ((ActionException) error).code())
                .isEqualTo("unsupported-transport");
        assertThat(repository.list()).isEmpty();
    }

    @Test
    void validatesCallerProfileBeforeOpeningANetworkConnection() throws Exception {
        ActionCatalog guarded = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository,
                (target, tls) -> {
                    throw new AssertionError("channel must not be opened for an invalid profile");
                });
        ObjectNode input = registerInput(false);
        ((ObjectNode) input.path("profile").path("endpoints").get(0)).remove("transport");

        assertThatThrownBy(() -> guarded.execute("service-register", input))
                .isInstanceOf(ActionException.class)
                .extracting(error -> ((ActionException) error).code())
                .isEqualTo("invalid-input");
        assertThat(repository.list()).isEmpty();
    }

    @Test
    void rejectsReflectionDeadlinesAboveTheServerLimit() throws Exception {
        ObjectNode input = registerInput(false).put("deadlineMs", 60_001);

        assertThatThrownBy(() -> catalog.execute("service-register", input))
                .isInstanceOf(ActionException.class)
                .extracting(error -> ((ActionException) error).code())
                .isEqualTo("invalid-input");
        assertThat(repository.list()).isEmpty();
    }

    @Test
    void unconfiguredWorkspaceKeepsDiscoverableActionsButReturnsUnavailable() {
        ActionCatalog unconfigured = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), null,
                (target, tls) -> channel());

        assertThat(unconfigured.names()).contains("service-register", "service-list",
                "service-inspect", "service-refresh", "service-invoke");
        assertThatThrownBy(() -> unconfigured.execute("service-list", MAPPER.createObjectNode()))
                .isInstanceOf(ActionException.class)
                .extracting(error -> ((ActionException) error).code())
                .isEqualTo("unavailable");
    }

    private ObjectNode registerInput(boolean credentialReference) throws Exception {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode profile = input.putObject("profile");
        profile.put("name", "health-local");
        profile.put("description", "Test health service");
        ObjectNode endpoint = profile.putArray("endpoints").addObject();
        endpoint.put("name", "local");
        endpoint.put("host", "localhost");
        endpoint.put("port", 50051);
        endpoint.put("transport", "TRANSPORT_PLAINTEXT");
        if (credentialReference) {
            endpoint.put("credentialRef", "env:HEALTH_TOKEN");
        }
        input.put("endpoint", "local");
        input.put("deadlineMs", 5_000);
        return input;
    }

    private ManagedChannel channel() {
        return InProcessChannelBuilder.forName(serverName).build();
    }
}
