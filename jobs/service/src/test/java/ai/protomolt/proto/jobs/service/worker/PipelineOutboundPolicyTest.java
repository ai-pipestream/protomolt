package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
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
import io.grpc.ManagedChannel;
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
import io.grpc.inprocess.InProcessServerBuilder;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that durable pipeline execution in {@link WorkflowRunWorker} enforces
 * {@link OutboundChannelPolicy} target allowlists and TLS requirements before opening outbound
 * gRPC channels, connects over TLS when permitted, refuses unconfigured workspaces or smuggled
 * {@code in-process} endpoints, and releases channel slots on shutdown.
 */
class PipelineOutboundPolicyTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROFILE_NAME = "echo-service";
    private static final Context.Key<String> OP_ID_CTX = Context.key("op-id");

    private static final String SCHEMA = """
            syntax = "proto3";
            package jobs.pipeline.policy;

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
    private final AtomicReference<String> negotiatedTlsProtocol = new AtomicReference<>();

    private Server plainServer;
    private int plainPort;
    private Server tlsServer;
    private int tlsPort;
    private ActionContext context;
    private FileSystemServiceProfileRepository serviceProfiles;

    @BeforeAll
    static void compileSchemaAndIssueCertificates() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("policy_echo.proto", SCHEMA, "test")
                .build());
        echoFile = compiled.descriptorFor("policy_echo.proto").orElseThrow();
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
                "-dname", "CN=pipeline-policy-ca", "-ext", "bc:c", "-validity", "2",
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
    void setUp() throws Exception {
        plainTransports.set(0);
        plainCalls.set(0);
        tlsTransports.set(0);
        tlsCalls.set(0);
        negotiatedTlsProtocol.set(null);

        plainServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addTransportFilter(countingFilter(plainTransports))
                .addService(echoService(plainCalls, null))
                .build()
                .start();
        plainPort = plainServer.getPort();

        tlsServer = Grpc.newServerBuilderForPort(
                        0, TlsServerCredentials.create(serverCertPem.toFile(), serverKeyPem.toFile()))
                .addTransportFilter(countingFilter(tlsTransports))
                .addService(echoService(tlsCalls, negotiatedTlsProtocol))
                .build()
                .start();
        tlsPort = tlsServer.getPort();

        context = ActionContext.create();
        for (FileDescriptor fd : files) {
            context.registry().registerFile(fd);
        }
        serviceProfiles = new FileSystemServiceProfileRepository(workspaceDir);
        serviceProfiles.save(ServiceProfile.newBuilder()
                .setName(PROFILE_NAME)
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("policy_echo.proto")
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
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("secure-custom-trust")
                        .setHost("127.0.0.1")
                        .setPort(tlsPort)
                        .setTransport(Transport.TRANSPORT_TLS)
                        .setTrustRef("tls:test-ca")
                        .build())
                .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (plainServer != null) {
            plainServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (tlsServer != null) {
            tlsServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void deniedHostAndDeniedPortAreRefusedBeforeConnecting() {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);

        OutboundChannelPolicy denyHostPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("allowed.example.internal"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(denyHostPolicy),
                denyHostPolicy, serviceProfiles, null, workerConfig("deny-host"))) {
            String jobId = submitPipeline(submitter, echoPipeline("plain"), "hello");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.attempt).isEqualTo(1);
            assertThat(failed.error).contains("gRPC target host is not allowed: 127.0.0.1");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();
        }

        int otherPort = plainPort == 65535 ? 65534 : plainPort + 1;
        OutboundChannelPolicy denyPortPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(otherPort))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(denyPortPolicy),
                denyPortPolicy, serviceProfiles, null, workerConfig("deny-port"))) {
            String jobId = submitPipeline(submitter, echoPipeline("plain"), "hello");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.attempt).isEqualTo(1);
            assertThat(failed.error).contains("gRPC target port is not allowed: " + plainPort);
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void tlsRequiredPolicyRefusesPlaintextBeforeConnectingAndConnectsOverTlsWhenAllowed()
            throws Exception {
        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);

        OutboundChannelPolicy tlsRequiredPolicy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .allowedPorts(Set.of(plainPort, tlsPort))
                .allowPlaintext(false)
                .allowTls(true)
                .maxActiveChannels(1)
                .build();
        ChannelCredentials trustedCa = TlsChannelCredentials.newBuilder()
                .trustManager(caCertPem.toFile())
                .build();
        ChannelFactory policyTlsFactory = policyBackedTlsFactory(tlsRequiredPolicy, trustedCa);
        WorkflowRunWorker.EndpointCredentials endpointCredentials =
                new WorkflowRunWorker.EndpointCredentials() {
                    @Override
                    public ChannelCredentials channelCredentials(ServiceProfile profile,
                                                                 ServiceEndpoint endpoint) {
                        if ("tls:test-ca".equals(endpoint.getTrustRef())) {
                            return trustedCa;
                        }
                        return WorkflowRunWorker.EndpointCredentials.NONE
                                .channelCredentials(profile, endpoint);
                    }
                };

        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(tlsRequiredPolicy),
                policyTlsFactory, serviceProfiles, null, endpointCredentials,
                workerConfig("tls-required"))) {
            // 1. Plaintext endpoint is refused by OutboundChannelPolicy before connecting.
            String plainJobId = submitPipeline(submitter, echoPipeline("plain"), "blocked");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord plainFailed = store.get(UUID.fromString(plainJobId)).orElseThrow();
            assertThat(plainFailed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(plainFailed.attempt).isEqualTo(1);
            assertThat(plainFailed.error).contains("plaintext outbound channels are disabled by policy");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();

            // 2. TLS endpoint connects over TLS, propagates operation identity, and completes.
            String tlsJobId = submitPipeline(submitter, echoPipeline("secure"), "hello-tls");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord tlsCompleted = store.get(UUID.fromString(tlsJobId)).orElseThrow();
            assertThat(tlsCompleted.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(tlsCalls.get()).isEqualTo(1);
            assertThat(negotiatedTlsProtocol.get()).startsWith("TLS");
            JsonNode result = JSON.readTree(tlsCompleted.result);
            assertThat(result.path("echoed").asText()).isEqualTo("echo:hello-tls");
            assertThat(result.path("operationId").asText()).isEqualTo(tlsJobId + "/call_echo");

            // 3. Custom trust_ref endpoint also goes through the same policy and completes.
            // First close the shared channel by finishing worker scope below, so test after close.
        }

        // Closing the worker above MUST release the single maxActiveChannels(1) lease on
        // tlsRequiredPolicy so a fresh worker sharing tlsRequiredPolicy can open a new TLS channel.
        try (WorkflowRunWorker secondWorker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(tlsRequiredPolicy),
                ChannelFactory.standard(tlsRequiredPolicy), serviceProfiles, null,
                endpointCredentials, workerConfig("tls-custom-trust"))) {
            String customTlsJobId = submitPipeline(
                    submitter, echoPipeline("secure-custom-trust"), "mtls-ca");
            assertThat(secondWorker.workOnce()).isTrue();

            WorkflowRunRecord completed = store.get(UUID.fromString(customTlsJobId)).orElseThrow();
            assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(tlsCalls.get()).isEqualTo(2);
            JsonNode result = JSON.readTree(completed.result);
            assertThat(result.path("echoed").asText()).isEqualTo("echo:mtls-ca");
            assertThat(result.path("operationId").asText())
                    .isEqualTo(customTlsJobId + "/call_echo");
        }
    }

    @Test
    void defaultWorkerRefusesUnconfiguredServiceWorkspaceAndSmuggledInProcessEndpoints()
            throws Exception {
        AtomicInteger inProcessCalls = new AtomicInteger();
        String inProcessName = InProcessServerBuilder.generateName();
        Server inProcessServer = InProcessServerBuilder.forName(inProcessName)
                .directExecutor()
                .addService(echoService(inProcessCalls, null))
                .build()
                .start();
        try {
            InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
            try (WorkflowRunWorker worker = new WorkflowRunWorker(
                    store, context, null, new WorkflowRunner(), workerConfig("default-worker"))) {
                String smuggledJobId = submitPipeline(
                        submitter, echoPipeline("in-process." + inProcessName), "smuggle");
                assertThat(worker.workOnce()).isTrue();

                WorkflowRunRecord failed = store.get(UUID.fromString(smuggledJobId)).orElseThrow();
                assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
                assertThat(failed.error).contains("service profile repository is not configured");
                assertThat(inProcessCalls.get()).isZero();
            }
        } finally {
            inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ChannelFactory policyBackedTlsFactory(OutboundChannelPolicy policy,
                                                         ChannelCredentials defaultTlsCredentials) {
        ChannelFactory standard = ChannelFactory.standard(policy);
        return new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                return tls
                        ? standard.open(target, defaultTlsCredentials)
                        : standard.open(target, false);
            }

            @Override
            public ManagedChannel open(String target, ChannelCredentials credentials) {
                return standard.open(target, credentials);
            }

            @Override
            public void validateTarget(String target, boolean tls) {
                standard.validateTarget(target, tls);
            }

            @Override
            public void validateDeadline(long deadlineMillis) {
                standard.validateDeadline(deadlineMillis);
            }

            @Override
            public OutboundChannelPolicy policy() {
                return policy;
            }
        };
    }

    @Test
    void approvalRequiredMethodPolicyStopsThePipelineCallBeforeConnecting() throws Exception {
        ServiceProfile base = serviceProfiles.find(PROFILE_NAME).orElseThrow();
        serviceProfiles.save(base.toBuilder()
                .addMethodPolicies(MethodPolicy.newBuilder()
                        .setMethod("jobs.pipeline.policy.EchoService/Echo")
                        .addOperation(Operation.OPERATION_APPROVAL_REQUIRED)
                        .setApprovalRequired(true)
                        .build())
                .build());

        InMemoryWorkflowRunStore store = new InMemoryWorkflowRunStore();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
        OutboundChannelPolicy allowLoopback = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("127.0.0.1"))
                .build();
        try (WorkflowRunWorker worker = new WorkflowRunWorker(
                store, context, null, new WorkflowRunner(allowLoopback),
                allowLoopback, serviceProfiles, null, workerConfig("approval-required"))) {
            String jobId = submitPipeline(submitter, echoPipeline("plain"), "hello");
            assertThat(worker.workOnce()).isTrue();

            WorkflowRunRecord failed = store.get(UUID.fromString(jobId)).orElseThrow();
            assertThat(failed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(failed.error).contains("requires approval");
            assertThat(plainCalls.get()).isZero();
        }
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
                .setName("policy-echo-pipeline")
                .setInputType("jobs.pipeline.policy.EchoRequest")
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
                                .setMethod("jobs.pipeline.policy.EchoService/Echo")
                                .setMethodShape(MethodShape.METHOD_SHAPE_UNARY)
                                .setEdge(TypedEdge.newBuilder()
                                        .addSources("input")
                                        .setProduceType("jobs.pipeline.policy.EchoRequest")
                                        .addRules("message = input.message")
                                        .build())
                                .setEdgeCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setOutputCardinality(EdgeCardinality.EDGE_CARDINALITY_ONE)
                                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                                .build())
                        .build())
                .build();
    }

    private static WorkflowRunsConfig workerConfig(String workerId) {
        return new WorkflowRunsConfig(workerId, 1, java.time.Duration.ofSeconds(30),
                java.time.Duration.ofMillis(20), 1, 4, 4,
                null, "workflow-run-events", null, null);
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
                        "jobs.pipeline.policy.EchoService")
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
