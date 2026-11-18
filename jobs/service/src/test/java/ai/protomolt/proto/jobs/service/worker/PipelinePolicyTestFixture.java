package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
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
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import ai.protomolt.proto.pipeline.v1.MethodShape;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
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
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared schema, PKI, gRPC service, and pipeline builders for {@link PipelineOutboundPolicyTest}.
 */
final class PipelinePolicyTestFixture {

    static final String PROFILE_NAME = "echo-service";
    static final Context.Key<String> OP_ID_CTX = Context.key("op-id");

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

    record CompiledSchema(FileDescriptor echoFile, List<FileDescriptor> files,
                          String descriptorFingerprint) {
    }

    record IssuedPki(Path caCertPem, Path serverCertPem, Path serverKeyPem) {
    }

    private PipelinePolicyTestFixture() {
    }

    static CompiledSchema compileSchema() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("policy_echo.proto", SCHEMA, "test")
                .build());
        FileDescriptor echoFile = compiled.descriptorFor("policy_echo.proto").orElseThrow();
        List<FileDescriptor> files = List.of(echoFile);
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(files.stream().map(FileDescriptor::toProto)
                        .sorted((a, b) -> a.getName().compareTo(b.getName()))
                        .toList())
                .build();
        return new CompiledSchema(
                echoFile, files, ServiceProfileValidation.sha256(set.toByteArray()));
    }

    static IssuedPki issueCertificates(Path pkiDir) throws Exception {
        String password = "test-only-password";
        Path caStore = pkiDir.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=pipeline-policy-ca", "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path caCertPem = pkiDir.resolve("ca.pem");
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
        Path serverCertPem = pkiDir.resolve("server.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", serverCsr.toString(),
                "-outfile", serverCertPem.toString(), "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "2", "-keystore", caStore.toString(), "-storepass", password);
        Path serverKeyPem = exportKey(serverStore, "server", password, pkiDir.resolve("server.key"));
        return new IssuedPki(caCertPem, serverCertPem, serverKeyPem);
    }

    static String submitPipeline(WorkflowRunSubmitter submitter, CompiledSchema schema,
                                 Pipeline pipeline, String text) {
        Descriptor reqType = schema.echoFile().findMessageTypeByName("EchoRequest");
        DynamicMessage input = DynamicMessage.newBuilder(reqType)
                .setField(reqType.findFieldByName("message"), text)
                .build();
        WorkflowRunSubmitter.Outcome outcome =
                submitter.submitPipeline(pipeline, schema.files(), input, null);
        assertThat(outcome.ok()).as(outcome.error()).isTrue();
        return outcome.jobId();
    }

    static Pipeline echoPipeline(CompiledSchema schema, String alias, String endpointName) {
        ServiceDependency.Builder dep = ServiceDependency.newBuilder()
                .setAlias(alias)
                .setServiceProfile(PROFILE_NAME)
                .setDescriptorFingerprint(schema.descriptorFingerprint());
        if (endpointName != null && !endpointName.isEmpty()) {
            dep.setEndpoint(endpointName);
        }
        return Pipeline.newBuilder()
                .setName("policy-echo-pipeline")
                .setInputType("jobs.pipeline.policy.EchoRequest")
                .setDescriptorFingerprint(schema.descriptorFingerprint())
                .setMaxStreamMessages(8)
                .setDeadline(Duration.newBuilder().setSeconds(15).build())
                .addDependencies(dep.build())
                .addSteps(PipelineStep.newBuilder()
                        .setName("call_echo")
                        .setDependency(alias)
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

    static WorkflowRunsConfig workerConfig(String workerId, int maxAttempts,
                                           int maxConcurrentPerTarget) {
        return new WorkflowRunsConfig(workerId, 1, java.time.Duration.ofSeconds(30),
                java.time.Duration.ofMillis(20), 1, maxAttempts, maxConcurrentPerTarget,
                null, "workflow-run-events", null, null);
    }

    static void saveDefaultServiceProfile(FileSystemServiceProfileRepository repository,
                                          CompiledSchema schema,
                                          int plainPort,
                                          int secondaryPort,
                                          int tlsPort) throws Exception {
        repository.save(ServiceProfile.newBuilder()
                .setName(PROFILE_NAME)
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("policy_echo.proto")
                        .setDescriptorFingerprint(schema.descriptorFingerprint())
                        .setDescriptorArtifactRef("sha256:" + schema.descriptorFingerprint())
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("plain")
                        .setHost("127.0.0.1")
                        .setPort(plainPort)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT)
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("plain-mirror")
                        .setHost("127.0.0.1")
                        .setPort(plainPort)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT)
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("plain-secondary")
                        .setHost("127.0.0.1")
                        .setPort(secondaryPort)
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

    static ChannelFactory policyBackedTlsFactory(OutboundChannelPolicy policy,
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

    static ServerTransportFilter countingFilter(AtomicInteger counter) {
        return new ServerTransportFilter() {
            @Override
            public Attributes transportReady(Attributes transportAttrs) {
                counter.incrementAndGet();
                return transportAttrs;
            }
        };
    }

    static ServerServiceDefinition echoService(CompiledSchema schema,
                                               AtomicInteger callCounter,
                                               AtomicReference<String> tlsProtocolSink,
                                               Runnable onInvokeHook) {
        MethodDescriptor md = schema.echoFile()
                .findServiceByName("EchoService").findMethodByName("Echo");
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
                    if (onInvokeHook != null) {
                        onInvokeHook.run();
                    }
                    Descriptor outType = schema.echoFile().findMessageTypeByName("EchoResponse");
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
