package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.StreamingAction;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.inference.spi.EnvCredentialResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The four authenticated workspace paths — registration, refresh, inspection, and invocation —
 * against a server that requires a bearer token. The host resolver reads a stand-in
 * environment whose value the tests rotate, proving credentials are re-resolved per call and
 * that a missing or wrong credential fails explicitly instead of falling back to an anonymous
 * call. No raw secret value may appear in any reply, stored profile, or error.
 */
class EnvBearerCallCredentialsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @TempDir
    Path directory;

    private String serverName;
    private Server server;
    private FileSystemServiceProfileRepository repository;
    private ActionCatalog catalog;
    private final AtomicReference<String> requiredToken = new AtomicReference<>();
    private final AtomicReference<String> envValue = new AtomicReference<>();
    private final AtomicReference<String> seenAuthorization = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        serverName = "env-bearer-" + UUID.randomUUID();
        ServerInterceptor auth = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String presented = headers.get(AUTHORIZATION);
                seenAuthorization.set(presented);
                String token = requiredToken.get();
                if (token != null && !("Bearer " + token).equals(presented)) {
                    // An endpoint can echo the presented credential in its refusal; both the
                    // status text and the trailers carry it so tests can prove nothing leaks.
                    Metadata trailers = new Metadata();
                    trailers.put(AUTHORIZATION, presented == null ? "" : presented);
                    call.close(Status.UNAUTHENTICATED
                            .withDescription("presented credential: " + presented), trailers);
                    return new ServerCall.Listener<>() {
                    };
                }
                return next.startCall(call, headers);
            }
        };
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerInterceptors.intercept(
                        new HealthStatusManager().getHealthService(), auth))
                .addService(ServerInterceptors.intercept(ChannelzService.newInstance(10), auth))
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(), auth))
                .build()
                .start();
        repository = new FileSystemServiceProfileRepository(directory);
        EnvCredentialResolver environment =
                new EnvCredentialResolver(name -> "PROFILE_TOKEN".equals(name)
                        ? envValue.get() : null);
        var binding = new EnvProfileCredentialResolver.Binding("secure-health", "local",
                "localhost", 50051, Transport.TRANSPORT_PLAINTEXT, "env:PROFILE_TOKEN");
        ProfileCredentialResolver resolver =
                new EnvProfileCredentialResolver(Set.of(binding), environment);
        catalog = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, null,
                (target, tls) -> channel(), resolver);
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void registrationInspectionInvocationAndReflectedVerbCarryTheBearerToken() throws Exception {
        requiredToken.set("token-one");
        envValue.set("token-one");

        ObjectNode registered = catalog.execute("service-register", registerInput());
        assertThat(registered.path("ok").asBoolean()).isTrue();
        assertThat(seenAuthorization.get()).isEqualTo("Bearer token-one");
        assertThat(registered.toString()).doesNotContain("token-one");

        ObjectNode inspected = catalog.execute("service-inspect",
                MAPPER.createObjectNode().put("name", "secure-health"));
        assertThat(inspected.path("services").findValuesAsText("name"))
                .contains("grpc.health.v1.Health", "grpc.channelz.v1.Channelz");
        assertThat(inspected.path("profile").path("endpoints").get(0)
                .path("credentialRef").asText()).isEqualTo("env:PROFILE_TOKEN");
        assertThat(inspected.toString()).doesNotContain("token-one");

        ObjectNode invocation = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request").put("service", "");
        ObjectNode result = catalog.execute("service-invoke", invocation);
        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(result.path("status").asText()).isEqualTo("OK");
        assertThat(seenAuthorization.get()).isEqualTo("Bearer token-one");
        assertThat(result.toString()).doesNotContain("token-one");

        ObjectNode verb = catalog.execute("secure-health-check", MAPPER.createObjectNode());
        assertThat(verb.path("status").asText()).isEqualTo("SERVING");
        assertThat(seenAuthorization.get()).isEqualTo("Bearer token-one");
        assertThat(verb.toString()).doesNotContain("token-one");
    }

    @Test
    void serverStreamingInvocationCarriesTheBearerToken() throws Exception {
        requiredToken.set("token-one");
        envValue.set("token-one");
        catalog.execute("service-register", registerInput());

        ObjectNode streaming = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.channelz.v1.Channelz/GetTopChannels");
        streaming.putObject("request");
        ObjectNode result = catalog.execute("service-invoke", streaming);
        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(seenAuthorization.get()).isEqualTo("Bearer token-one");
        assertThat(result.toString()).doesNotContain("token-one");
    }

    @Test
    void refreshReResolvesRotatedCredentialsAndFailsExplicitlyOnStaleOnes() throws Exception {
        requiredToken.set("token-one");
        envValue.set("token-one");
        catalog.execute("service-register", registerInput());

        requiredToken.set("token-two");
        envValue.set("token-two");
        ObjectNode refreshed = catalog.execute("service-refresh",
                MAPPER.createObjectNode().put("name", "secure-health"));
        assertThat(refreshed.path("ok").asBoolean()).isTrue();
        assertThat(seenAuthorization.get()).isEqualTo("Bearer token-two");
        assertThat(refreshed.toString()).doesNotContain("token-two");

        // The server moved on while the client still presents the old value: the refresh
        // must fail, not succeed against a cached credential.
        requiredToken.set("token-three");
        ObjectNode stale = catalog.execute("service-refresh",
                MAPPER.createObjectNode().put("name", "secure-health"));
        assertThat(stale.path("ok").asBoolean()).isFalse();
        assertThat(stale.path("error").asText()).contains("withheld");
        assertThat(stale.toString()).doesNotContain("token-two");
    }

    @Test
    void missingCredentialFailsEveryPathExplicitlyBeforeOpeningAChannel() throws Exception {
        requiredToken.set("token-one");
        envValue.set("token-one");
        catalog.execute("service-register", registerInput());

        // Once the reference stops resolving, every authenticated path refuses before any
        // channel opens, and inspection reports the profile unusable.
        envValue.set(null);
        ActionCatalog guarded = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, null,
                (target, tls) -> {
                    throw new AssertionError("a missing credential must fail before dialing");
                }, new EnvProfileCredentialResolver(Set.of(binding()), environment()));
        ReflectedServiceActions.registerStored(guarded, repository, null,
                (target, tls) -> {
                    throw new AssertionError("a missing credential must fail before dialing");
                }, new EnvProfileCredentialResolver(Set.of(binding()), environment()));

        assertThatThrownBy(() -> guarded.execute("service-refresh",
                MAPPER.createObjectNode().put("name", "secure-health")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("credential-unavailable"));
        ObjectNode invocation = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request");
        assertThatThrownBy(() -> guarded.execute("service-invoke", invocation))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("credential-unavailable"));
        assertThatThrownBy(() -> guarded.execute("secure-health-check", MAPPER.createObjectNode()))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("credential-unavailable"));
        assertThatThrownBy(() -> guarded.execute("service-inspect",
                MAPPER.createObjectNode().put("name", "secure-health")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("credential-unavailable");
                    assertThat(e.getMessage()).doesNotContain("token-one");
                });

        ObjectNode second = registerInput();
        ((ObjectNode) second.path("profile")).put("name", "secure-other");
        assertThatThrownBy(() -> guarded.execute("service-register", second))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("credential-unavailable"));
        assertThat(repository.find("secure-other")).isEmpty();
    }

    @Test
    void wrongCredentialIsAnExplicitFailureNeverAFallbackAndNeverLeaks() throws Exception {
        requiredToken.set("right-token");
        envValue.set("wrong-token");
        ObjectNode rejected = catalog.execute("service-register", registerInput());
        assertThat(rejected.path("ok").asBoolean()).isFalse();
        assertThat(rejected.path("error").asText()).contains("UNAUTHENTICATED", "withheld");
        assertThat(rejected.toString()).doesNotContain("wrong-token");

        // Register with working material, then rotate to a wrong value: the stored profile
        // is not a license to call with bad credentials.
        requiredToken.set("token-one");
        envValue.set("token-one");
        assertThat(catalog.execute("service-register", registerInput())
                .path("ok").asBoolean()).isTrue();
        requiredToken.set("right-token");
        envValue.set("wrong-token");

        ObjectNode invocation = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request");
        ObjectNode result = catalog.execute("service-invoke", invocation);
        assertThat(result.path("ok").asBoolean()).isFalse();
        assertThat(result.path("status").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(result.path("description").asText())
                .as("authenticated calls surface the code, not the upstream echo").isEmpty();
        assertThat(result.toString()).doesNotContain("wrong-token");

        // The server-streaming path withholds the echo the same way, on the terminal event.
        // (The JSON streaming edge converts envelopes upstream of this test, so this drives
        // the verb's typed streaming path directly.)
        ObjectNode streaming = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.channelz.v1.Channelz/GetTopChannels");
        streaming.putObject("request");
        StreamingAction serviceInvoke = (StreamingAction) catalog.get("service-invoke");
        Message request = ServiceActionJson.parse(
                streaming, "ServiceInvokeRequest", "service-invoke");
        List<Message> emitted = new ArrayList<>();
        serviceInvoke.executeStreaming(request, ActionContext.create(), emitted::add);
        assertThat(emitted).isNotEmpty();
        ObjectNode terminal = CatalogContract.toReply(
                emitted.get(emitted.size() - 1), "service-invoke");
        assertThat(terminal.path("ok").asBoolean()).isFalse();
        assertThat(terminal.path("status").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(terminal.path("description").asText()).isEmpty();
        assertThat(emitted.toString()).doesNotContain("wrong-token");

        assertThatThrownBy(() -> catalog.execute("secure-health-check", MAPPER.createObjectNode()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("upstream-unauthenticated");
                    assertThat(e.getMessage()).doesNotContain("wrong-token");
                });
    }

    @Test
    void unauthenticatedCallsKeepUpstreamDiagnostics() throws Exception {
        // A profile with no credential reference: the same echoing refusal keeps its
        // diagnostics, because there is no presented credential to protect.
        requiredToken.set("token-one");
        ActionCatalog anonymous = ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()), repository, null,
                (target, tls) -> channel());
        ObjectNode plain = registerInput();
        ((ObjectNode) plain.path("profile").path("endpoints").get(0)).remove("credentialRef");

        ObjectNode rejected = anonymous.execute("service-register", plain);
        assertThat(rejected.path("ok").asBoolean()).isFalse();
        assertThat(rejected.path("error").asText()).contains("presented credential");

        requiredToken.set(null);
        assertThat(anonymous.execute("service-register", plain).path("ok").asBoolean()).isTrue();
        requiredToken.set("token-one");
        ObjectNode invocation = MAPPER.createObjectNode()
                .put("name", "secure-health")
                .put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request");
        ObjectNode result = anonymous.execute("service-invoke", invocation);
        assertThat(result.path("ok").asBoolean()).isFalse();
        assertThat(result.path("status").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(result.path("description").asText()).contains("presented credential");
    }

    @Test
    void unsupportedSchemeAndRawSecretsAreRefusedBeforeAnyCall() throws Exception {
        requiredToken.set("token-one");
        envValue.set("token-one");

        // A namespaced but non-env scheme stores in the profile contract; only the env
        // resolver's refusal makes it unusable.
        ObjectNode vault = registerInput();
        ((ObjectNode) vault.path("profile").path("endpoints").get(0))
                .put("credentialRef", "vault:team-token");
        assertThatThrownBy(() -> catalog.execute("service-register", vault))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("credential-unavailable"));
        assertThat(repository.list()).isEmpty();

        // A raw secret is not a reference at all, so profile validation refuses it before
        // anything resolves.
        ObjectNode raw = registerInput();
        ((ObjectNode) raw.path("profile").path("endpoints").get(0))
                .put("credentialRef", "Bearer hunter2");
        assertThatThrownBy(() -> catalog.execute("service-register", raw))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
        assertThat(repository.list()).isEmpty();
    }

    private ObjectNode registerInput() {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode profile = input.putObject("profile");
        profile.put("name", "secure-health");
        profile.put("description", "Bearer-gated test service");
        ObjectNode endpoint = profile.putArray("endpoints").addObject();
        endpoint.put("name", "local");
        endpoint.put("host", "localhost");
        endpoint.put("port", 50051);
        endpoint.put("transport", "TRANSPORT_PLAINTEXT");
        endpoint.put("credentialRef", "env:PROFILE_TOKEN");
        input.put("endpoint", "local");
        input.put("deadlineMs", 5_000);
        return input;
    }

    private EnvProfileCredentialResolver.Binding binding() {
        return new EnvProfileCredentialResolver.Binding("secure-health", "local",
                "localhost", 50051, Transport.TRANSPORT_PLAINTEXT, "env:PROFILE_TOKEN");
    }

    private EnvCredentialResolver environment() {
        return new EnvCredentialResolver(name -> "PROFILE_TOKEN".equals(name)
                ? envValue.get() : null);
    }

    private ManagedChannel channel() {
        return InProcessChannelBuilder.forName(serverName).build();
    }
}
