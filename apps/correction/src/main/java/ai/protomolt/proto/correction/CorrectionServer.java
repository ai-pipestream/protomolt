package ai.protomolt.proto.correction;

import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.inference.spi.*;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.registry.GitSchemaRegistryStore;
import ai.protomolt.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Candidate host for one reviewed contact-correction service. */
public final class CorrectionServer implements AutoCloseable {
    private final Server server;
    private final CorrectionGrpcService service;
    private final RemoteInferenceProvider remote;
    private final GitSchemaRegistryStore registry;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CorrectionServer(Server server, CorrectionGrpcService service,
                             RemoteInferenceProvider remote, GitSchemaRegistryStore registry) {
        this.server = server;
        this.service = service;
        this.remote = remote;
        this.registry = registry;
    }

    public static void main(String[] arguments) throws Exception {
        Options options = Options.parse(arguments);
        try (CorrectionServer host = start(options)) {
            Runtime.getRuntime().addShutdownHook(new Thread(host::close, "correction-shutdown"));
            host.server.awaitTermination();
        }
    }

    static CorrectionServer start(Options options) throws Exception {
        String token = requiredEnvironment(options.tokenEnv());
        SigningIdentity identity = SigningIdentity.open(options.workspace());
        RemoteInferenceProvider remote = null;
        GitSchemaRegistryStore registry = null;
        CorrectionGrpcService service = null;
        Server server = null;
        try {
            InferenceProvider provider;
            ModelEntry entry;
            boolean promptGuided = options.mode() == Mode.REMOTE_PROMPT;
            if (options.mode() == Mode.FIXTURE) {
                provider = new CorrectionSample.FixtureProvider();
                entry = ModelEntry.newBuilder().setId("starter-correction")
                        .setProvider(provider.id()).setEndpoint("in-process://fixture")
                        .setBackendModel("contact-fixture-v1")
                        .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true)).build();
            } else {
                remote = new RemoteInferenceProvider(options.inferenceTarget(),
                        requiredEnvironment(options.inferenceTokenEnv()));
                ModelEntry described = remote.describe("starter-correction");
                if (options.mode() == Mode.REMOTE_NATIVE
                        && !described.getCapabilities().getStructuredOutput()) {
                    throw new IllegalArgumentException("remote model lacks native structured output");
                }
                provider = remote;
                entry = described.toBuilder().setEndpoint(options.inferenceTarget()).build();
            }
            InferenceEngines engines = new InferenceEngines(new InferenceCatalog(), List.of(provider));
            engines.register(entry);
            StructuredGenerator generator = new StructuredGenerator(engines, DescriptorRegistry.create(false));
            registry = GitSchemaRegistryStore.builder()
                    .repositoryDir(options.workspace().resolve("registry")).build();
            String resource = promptGuided
                    ? "/starter/correct-contact.prompt-guided.workflow.json"
                    : "/starter/correct-contact.workflow.json";
            try (var stream = CorrectionServer.class.getResourceAsStream(resource)) {
                if (stream == null) throw new IOException("missing reviewed correction workflow");
                String selected = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                String existing = registry.workflow(ContactCorrection.NAME).orElse(null);
                if (existing == null) registry.putWorkflow(ContactCorrection.NAME, selected);
                else if (!new ObjectMapper().readTree(existing).equals(new ObjectMapper().readTree(selected))) {
                    throw new IllegalStateException("stored correction workflow differs from selected mode");
                }
            }
            GitSchemaRegistryStore mounted = registry;
            ObjectMapper mapper = new ObjectMapper();
            WorkflowRepository workflows = name -> mounted.workflow(name).map(json -> {
                try { return (ObjectNode) mapper.readTree(json); }
                catch (IOException invalid) { throw new IllegalArgumentException("invalid stored workflow", invalid); }
            });
            ContactCorrection correction = new ContactCorrection(options.workspace(), workflows,
                    generator, options.mode() == Mode.FIXTURE
                            ? CorrectionSample.fixtureEvaluator()
                            : CorrectionSample.liveEvaluator(provider, entry, promptGuided),
                    identity.signing(), Clock.systemUTC(), Duration.ofSeconds(100));
            service = new CorrectionGrpcService(correction, identity.trust(), options.workspace());
            HealthStatusManager health = new HealthStatusManager();
            health.setStatus("ai.protomolt.proto.correction.v1.CorrectionService",
                    HealthCheckResponse.ServingStatus.SERVING);
            ServerBuilder<?> builder = NettyServerBuilder.forAddress(
                    new InetSocketAddress(options.host(), options.port()))
                    .addService(service).addService(health.getHealthService())
                    .addService(ProtoReflectionServiceV1.newInstance())
                    .intercept(new ApiTokenServerInterceptor(token));
            server = builder.build().start();
            return new CorrectionServer(server, service, remote, registry);
        } catch (Exception failure) {
            if (server != null) server.shutdownNow();
            if (service != null) service.close();
            if (remote != null) remote.close();
            if (registry != null) registry.close();
            throw failure;
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        server.shutdownNow();
        service.close();
        if (remote != null) remote.close();
        registry.close();
    }

    enum Mode { FIXTURE, REMOTE_NATIVE, REMOTE_PROMPT }

    record Options(String host, int port, Path workspace, String tokenEnv,
                   Mode mode, String inferenceTarget, String inferenceTokenEnv) {
        static Options parse(String[] args) {
            String host = "0.0.0.0";
            int port = 9090;
            Path workspace = Path.of("/data/correction");
            String tokenEnv = "PROTOMOLT_CORRECTION_API_TOKEN";
            Mode mode = Mode.FIXTURE;
            String target = null;
            String inferenceTokenEnv = "PROTOMOLT_INFERENCE_API_TOKEN";
            for (int i = 0; i < args.length; i++) {
                if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
                String value = args[++i];
                switch (args[i - 1]) {
                    case "--host" -> host = value;
                    case "--port" -> port = Integer.parseInt(value);
                    case "--workspace" -> workspace = Path.of(value);
                    case "--token-env" -> tokenEnv = value;
                    case "--mode" -> mode = switch (value) {
                        case "fixture" -> Mode.FIXTURE;
                        case "remote-native" -> Mode.REMOTE_NATIVE;
                        case "remote-prompt" -> Mode.REMOTE_PROMPT;
                        default -> throw new IllegalArgumentException("unsupported correction mode");
                    };
                    case "--inference-target" -> target = value;
                    case "--inference-token-env" -> inferenceTokenEnv = value;
                    default -> throw new IllegalArgumentException("unknown correction argument");
                }
            }
            if (port < 1 || port > 65535 || host.isBlank() || tokenEnv.isBlank()
                    || (mode != Mode.FIXTURE && (target == null || target.isBlank()))) {
                throw new IllegalArgumentException("invalid correction server configuration");
            }
            return new Options(host, port, workspace, tokenEnv, mode, target, inferenceTokenEnv);
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing " + name);
        return value;
    }
}
