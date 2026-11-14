package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import io.grpc.Attributes;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.ServerCalls;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link JobsFleetMount} and {@link ProtoMoltServe} proving that durable
 * pipeline jobs honor the configured {@link OutboundChannelPolicy}: denied endpoints and plaintext
 * targets under TLS-required policy are refused before connecting, while allowed TLS endpoints
 * connect over TLS and complete cleanly.
 */
@Tag("integration")
@Testcontainers
class JobsFleetPipelineOutboundPolicyIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROFILE_NAME = "serve-pipeline-echo";
    private static final Context.Key<String> OP_ID_CTX = Context.key("serve-op-id");

    private static final String SCHEMA = """
            syntax = "proto3";
            package serve.jobs.pipeline;

            message EchoRequest {
              string message = 1;
            }
            message EchoResponse {
              string echoed = 1;
              string operation_id = 2;
            }
            service EchoService {
              rpc Echo(EchoRequest) returns (EchoResponse);
            }
            """;

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> POSTGRES =
            new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withEnv("POSTGRES_DB", "protomolt")
                    .withEnv("POSTGRES_USER", "protomolt")
                    .withEnv("POSTGRES_PASSWORD", "protomolt-test")
                    .withExposedPorts(5432)
                    .withStartupAttempts(3);

    @TempDir
    static Path pkiDir;

    private static List<FileDescriptor> files;
    private static FileDescriptor echoFile;
    private static String descriptorFp;
    private static Path caCertPem;
    private static Path serverCertPem;
    private static Path serverKeyPem;

    @TempDir
    Path workspaceDir;

    private final AtomicInteger plainTransports = new AtomicInteger();
    private final AtomicInteger plainCalls = new AtomicInteger();
    private final AtomicInteger tlsTransports = new AtomicInteger();
    private final AtomicInteger tlsCalls = new AtomicInteger();
    private final AtomicReference<String> tlsProtocol = new AtomicReference<>();

    private Server plainServer;
    private int plainPort;
    private Server tlsServer;
    private int tlsPort;

    @BeforeAll
    static void compileSchemaAndIssueCertificates() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serve_echo.proto", SCHEMA, "test")
                .build());
        echoFile = compiled.descriptorFor("serve_echo.proto").orElseThrow();
        files = List.of(echoFile);
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(files.stream().map(FileDescriptor::toProto)
                        .sorted((a, b) -> a.getName().compareTo(b.getName()))
                        .toList())
                .build();
        descriptorFp = ServiceProfileValidation.sha256(set.toByteArray());

        String password = "test-only-password";
        Path caStore = pkiDir.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=serve-jobs-ca", "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        caCertPem = pkiDir.resolve("ca.pem");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-file", caCertPem.toString(),
                "-keystore", caStore.toString(), "-storepass", password);

        Path serverStore = pkiDir.resolve("server.p12");
        keytool("-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=127.0.0.1", "-validity", "2",
                "-keystore", serverStore.toString(), "-storetype", "PKCS12",
                "-storepass", password);
        Path serverCsr = pkiDir.resolve("server.csr");
        keytool("-certreq", "-alias", "server", "-file", serverCsr.toString(),
                "-keystore", serverStore.toString(), "-storepass", password);
        serverCertPem = pkiDir.resolve("server.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", serverCsr.toString(),
                "-outfile", serverCertPem.toString(), "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "2", "-keystore", caStore.toString(), "-storepass", password);
        serverKeyPem = exportKey(serverStore, "server", password, pkiDir.resolve("server.key"));
    }

    @BeforeEach
    void startTargetServers() throws Exception {
        plainTransports.set(0);
        plainCalls.set(0);
        tlsTransports.set(0);
        tlsCalls.set(0);
        tlsProtocol.set(null);

        plainServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addTransportFilter(countingFilter(plainTransports))
                .addService(echoService(plainCalls, null))
                .build()
                .start();
        plainPort = plainServer.getPort();

        tlsServer = Grpc.newServerBuilderForPort(
                        0, TlsServerCredentials.create(serverCertPem.toFile(), serverKeyPem.toFile()))
                .addTransportFilter(countingFilter(tlsTransports))
                .addService(echoService(tlsCalls, tlsProtocol))
                .build()
                .start();
        tlsPort = tlsServer.getPort();

        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(workspaceDir);
        repository.save(ServiceProfile.newBuilder()
                .setName(PROFILE_NAME)
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("serve_echo.proto")
                        .setDescriptorFingerprint(descriptorFp)
                        .setDescriptorArtifactRef("sha256:" + descriptorFp)
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("plain")
                        .setHost("127.0.0.1")
                        .setPort(plainPort)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT)
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("secure")
                        .setHost("127.0.0.1")
                        .setPort(tlsPort)
                        .setTransport(Transport.TRANSPORT_TLS)
                        .setTrustRef("tls:serve-ca")
                        .build())
                .build());
    }

    @AfterEach
    void stopTargetServers() throws Exception {
        if (plainServer != null) {
            plainServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (tlsServer != null) {
            tlsServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void protoMoltServeRefusesDeniedEndpointBeforeConnecting() throws Exception {
        OutboundChannelPolicy denyHostPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("allowed.only.internal"))
                .build();
        ProtoMoltServe.JobsOptions jobsOptions = jobsOptions();
        ProtoMoltServe.Options options = new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, null, false, null,
                jobsOptions, List.of(), workspaceDir, denyHostPolicy);

        try (ProtoMoltServe ignored = ProtoMoltServe.start(options);
                WorkflowRunDatabase db = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                        jobsOptions.jdbcUrl(), jobsOptions.username(), jobsOptions.password()))) {
            JdbcWorkflowRunStore store = new JdbcWorkflowRunStore(db);
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
            String jobId = submitPipeline(submitter, echoPipeline("plain"), "denied");

            WorkflowRunRecord settled = awaitTerminal(store, UUID.fromString(jobId));
            assertThat(settled.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(settled.attempt).isEqualTo(1);
            assertThat(settled.error).contains("gRPC target host is not allowed: 127.0.0.1");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void jobsFleetMountEnforcesTlsRequiredPolicyRefusingPlaintextAndConnectingOverTls()
            throws Exception {
        OutboundChannelPolicy tlsRequiredPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(plainPort, tlsPort))
                .allowPlaintext(false)
                .allowTls(true)
                .maxActiveChannels(1)
                .build();
        ChannelCredentials caCredentials = TlsChannelCredentials.newBuilder()
                .trustManager(caCertPem.toFile())
                .build();
        ProfileCredentialResolver credentials = new ProfileCredentialResolver() {
            @Override
            public Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint)
                    throws ActionException {
                throw new ActionException("credential-unavailable", "not expected");
            }

            @Override
            public ChannelCredentials channelCredentials(ServiceProfile profile,
                                                         ServiceEndpoint endpoint)
                    throws ActionException {
                if ("tls:serve-ca".equals(endpoint.getTrustRef())) {
                    return caCredentials;
                }
                throw new ActionException("unsupported-transport", "unknown trust_ref");
            }
        };

        ActionContext context = ActionContext.create();
        for (FileDescriptor fd : files) {
            context.registry().registerFile(fd);
        }
        var serviceProfiles = WorkspaceMount.serviceProfiles(workspaceDir);
        JobsFleetMount.Prepared fleet = JobsFleetMount.prepare(
                jobsOptions(), context, null, null, tlsRequiredPolicy, serviceProfiles,
                credentials);
        try {
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(fleet.store(), null, 4);

            // 1. Plaintext endpoint is refused before opening a socket.
            String plainJobId = submitPipeline(submitter, echoPipeline("plain"), "plain-blocked");
            assertThat(fleet.worker().workOnce()).isTrue();
            WorkflowRunRecord plainFailed = fleet.store()
                    .get(UUID.fromString(plainJobId)).orElseThrow();
            assertThat(plainFailed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(plainFailed.error)
                    .contains("plaintext outbound channels are disabled by policy");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();

            // 2. TLS endpoint connects with TLS credentials under the same policy and completes.
            String tlsJobId = submitPipeline(submitter, echoPipeline("secure"), "serve-tls");
            assertThat(fleet.worker().workOnce()).isTrue();
            WorkflowRunRecord completed = fleet.store()
                    .get(UUID.fromString(tlsJobId)).orElseThrow();
            assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(tlsCalls.get()).isEqualTo(1);
            assertThat(tlsProtocol.get()).startsWith("TLS");
            JsonNode result = JSON.readTree(completed.result);
            assertThat(result.path("echoed").asText()).isEqualTo("echo:serve-tls");
            assertThat(result.path("operationId").asText()).isEqualTo(tlsJobId + "/call_echo");
        } finally {
            fleet.worker().close();
            if (fleet.relay() != null) {
                fleet.relay().close();
            }
            fleet.database().close();
        }
    }

    private static ProtoMoltServe.JobsOptions jobsOptions() {
        String jdbcUrl = "jdbc:postgresql://" + POSTGRES.getHost() + ":"
                + POSTGRES.getMappedPort(5432) + "/protomolt";
        return new ProtoMoltServe.JobsOptions(
                jdbcUrl, "protomolt", "protomolt-test", null, null, 1, 4);
    }

    private static WorkflowRunRecord awaitTerminal(JdbcWorkflowRunStore store, UUID jobId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            WorkflowRunRecord record = store.get(jobId).orElseThrow();
            if (WorkflowRunRecord.STATUS_COMPLETED.equals(record.status)
                    || WorkflowRunRecord.STATUS_FAILED.equals(record.status)
                    || WorkflowRunRecord.STATUS_DEAD.equals(record.status)) {
                return record;
            }
            Thread.sleep(25);
        }
        return store.get(jobId).orElseThrow();
    }

    private static String submitPipeline(WorkflowRunSubmitter submitter, Pipeline pipeline,
                                         String text) {
        Descriptor reqType = echoFile.findMessageTypeByName("EchoRequest");
        DynamicMessage input = DynamicMessage.newBuilder(reqType)
                .setField(reqType.findFieldByName("message"), text)
                .build();
        WorkflowRunSubmitter.Outcome outcome = submitter.submitPipeline(pipeline, files, input, null);
        assertThat(outcome.ok()).as(outcome.error()).isTrue();
        return outcome.jobId();
    }

    private static Pipeline echoPipeline(String endpointName) {
        return Pipeline.newBuilder()
                .setName("serve-policy-pipeline")
                .setInputType("serve.jobs.pipeline.EchoRequest")
                .setDescriptorFingerprint(descriptorFp)
                .setMaxStreamMessages(8)
                .setDeadline(Duration.newBuilder().setSeconds(15).build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("echo")
                        .setServiceProfile(PROFILE_NAME)
                        .setEndpoint(endpointName)
                        .setDescriptorFingerprint(descriptorFp)
                        .build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("call_echo")
                        .setDependency("echo")
                        .setGrpcCall(GrpcCallStep.newBuilder()
                                .setMethod("serve.jobs.pipeline.EchoService/Echo")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("serve.jobs.pipeline.EchoRequest")
                                        .addRules("message = input.message")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
    }

    private static ServerTransportFilter countingFilter(AtomicInteger counter) {
        return new ServerTransportFilter() {
            @Override
            public Attributes transportReady(Attributes transportAttrs) {
                counter.incrementAndGet();
                return transportAttrs;
            }
        };
    }

    private static ServerServiceDefinition echoService(AtomicInteger callCounter,
                                                       AtomicReference<String> tlsProtocolSink) {
        MethodDescriptor md = echoFile.findServiceByName("EchoService").findMethodByName("Echo");
        io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod =
                DynamicGrpcCalls.methodDescriptor(md);
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (tlsProtocolSink != null) {
                    SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
                    if (session != null) {
                        tlsProtocolSink.set(session.getProtocol());
                    }
                }
                String opId = headers.get(DynamicPipelineTransport.OPERATION_ID_HEADER);
                Context ctx = Context.current().withValue(OP_ID_CTX, opId == null ? "" : opId);
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
        ServerServiceDefinition raw = ServerServiceDefinition.builder(
                        "serve.jobs.pipeline.EchoService")
                .addMethod(grpcMethod, ServerCalls.asyncUnaryCall((req, obs) -> {
                    callCounter.incrementAndGet();
                    Descriptor outType = echoFile.findMessageTypeByName("EchoResponse");
                    String msg = (String) req.getField(
                            req.getDescriptorForType().findFieldByName("message"));
                    obs.onNext(DynamicMessage.newBuilder(outType)
                            .setField(outType.findFieldByName("echoed"), "echo:" + msg)
                            .setField(outType.findFieldByName("operation_id"), OP_ID_CTX.get())
                            .build());
                    obs.onCompleted();
                }))
                .build();
        return ServerInterceptors.intercept(raw, interceptor);
    }

    private static Path exportKey(Path store, String alias, String password, Path outFile)
            throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(store)) {
            ks.load(in, password.toCharArray());
        }
        PrivateKey key = (PrivateKey) ks.getKey(alias, password.toCharArray());
        Files.writeString(outFile, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n", StandardCharsets.US_ASCII);
        return outFile;
    }

    private static void keytool(String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.add("-noprompt");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }
}
