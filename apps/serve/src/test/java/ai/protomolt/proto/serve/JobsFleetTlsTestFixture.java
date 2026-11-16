package ai.protomolt.proto.serve;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.grpc.workspace.v1.ApprovedTlsEndpoint;
import ai.protomolt.proto.grpc.workspace.v1.ClientCertificateMount;
import ai.protomolt.proto.grpc.workspace.v1.ProfileTlsConfig;
import ai.protomolt.proto.grpc.workspace.v1.TrustBundleMount;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
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
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared schema, PKI, gRPC service, and pipeline helpers for
 * {@link JobsFleetPipelineOutboundPolicyIT}.
 */
final class JobsFleetTlsTestFixture {

    static final String PROFILE_NAME = "serve-pipeline-echo";
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

    record CompiledSchema(FileDescriptor echoFile, List<FileDescriptor> files,
                          String descriptorFingerprint) {
    }

    record IssuedPki(Path pkiDir, Path caCertPem, Path serverCertPem, Path serverKeyPem,
                     Path clientCertPem, Path clientKeyPem) {
    }

    private JobsFleetTlsTestFixture() {
    }

    static CompiledSchema compileSchema() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("serve_echo.proto", SCHEMA, "test")
                .build());
        FileDescriptor echoFile = compiled.descriptorFor("serve_echo.proto").orElseThrow();
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
                "-dname", "CN=serve-jobs-ca", "-ext", "bc:c", "-validity", "2",
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

        Path clientStore = pkiDir.resolve("client.p12");
        keytool("-genkeypair", "-alias", "client", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=serve-jobs-mtls-client", "-validity", "2",
                "-keystore", clientStore.toString(), "-storetype", "PKCS12",
                "-storepass", password);
        Path clientCsr = pkiDir.resolve("client.csr");
        keytool("-certreq", "-alias", "client", "-file", clientCsr.toString(),
                "-keystore", clientStore.toString(), "-storepass", password);
        Path clientCertPem = pkiDir.resolve("client.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", clientCsr.toString(),
                "-outfile", clientCertPem.toString(), "-validity", "2",
                "-keystore", caStore.toString(), "-storepass", password);
        Path clientKeyPem = exportKey(clientStore, "client", password, pkiDir.resolve("client.key"));
        return new IssuedPki(pkiDir, caCertPem, serverCertPem, serverKeyPem,
                clientCertPem, clientKeyPem);
    }

    static Path writeProfileTlsConfig(Path target, IssuedPki pki, int mtlsPort) throws Exception {
        ProfileTlsConfig config = ProfileTlsConfig.newBuilder()
                .setRootDirectory(pki.pkiDir().toString())
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:serve-ca")
                        .setBundlePath("ca.pem"))
                .addClientCertificates(ClientCertificateMount.newBuilder()
                        .setRef("tls:serve-client")
                        .setCertificateChainPath("client.pem")
                        .setPrivateKeyPath("client.key"))
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile(PROFILE_NAME)
                        .setEndpoint("mtls")
                        .setHost("127.0.0.1")
                        .setPort(mtlsPort)
                        .setTrustRef("tls:serve-ca")
                        .setClientCertificateRef("tls:serve-client"))
                .build();
        Files.writeString(target, JsonFormat.printer().print(config), StandardCharsets.UTF_8);
        return target;
    }

    static WorkflowRunRecord awaitTerminal(JdbcWorkflowRunStore store, UUID jobId)
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

    static Pipeline echoPipeline(CompiledSchema schema, String endpointName) {
        return Pipeline.newBuilder()
                .setName("serve-policy-pipeline")
                .setInputType("serve.jobs.pipeline.EchoRequest")
                .setDescriptorFingerprint(schema.descriptorFingerprint())
                .setMaxStreamMessages(8)
                .setDeadline(Duration.newBuilder().setSeconds(15).build())
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("echo")
                        .setServiceProfile(PROFILE_NAME)
                        .setEndpoint(endpointName)
                        .setDescriptorFingerprint(schema.descriptorFingerprint())
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
                                               AtomicReference<String> peerSubjectSink) {
        MethodDescriptor md = schema.echoFile()
                .findServiceByName("EchoService").findMethodByName("Echo");
        io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod =
                DynamicGrpcCalls.methodDescriptor(md);
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
                if (session != null) {
                    if (tlsProtocolSink != null) {
                        tlsProtocolSink.set(session.getProtocol());
                    }
                    if (peerSubjectSink != null) {
                        try {
                            X509Certificate peer =
                                    (X509Certificate) session.getPeerCertificates()[0];
                            peerSubjectSink.set(peer.getSubjectX500Principal().getName());
                        } catch (SSLPeerUnverifiedException ignored) {
                            peerSubjectSink.set(null);
                        }
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
