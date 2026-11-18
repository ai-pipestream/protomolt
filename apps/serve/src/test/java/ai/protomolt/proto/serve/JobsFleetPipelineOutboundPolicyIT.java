package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workspace.ProfileCredentialResolver;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.serve.JobsFleetTlsTestFixture.CompiledSchema;
import ai.protomolt.proto.serve.JobsFleetTlsTestFixture.IssuedPki;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
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

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.PROFILE_NAME;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.awaitTerminal;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.countingFilter;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.echoPipeline;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.echoService;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.submitPipeline;
import static ai.protomolt.proto.serve.JobsFleetTlsTestFixture.writeProfileTlsConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link JobsFleetMount} and {@link ProtoMoltServe} proving that durable
 * pipeline jobs honor {@link OutboundChannelPolicy} and {@code --profile-tls-config} mTLS
 * credentials in stock {@link ProtoMoltServe}.
 */
@Tag("integration")
@Testcontainers
class JobsFleetPipelineOutboundPolicyIT {

    private static final ObjectMapper JSON = new ObjectMapper();

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

    private static CompiledSchema schema;
    private static IssuedPki pki;

    @TempDir
    Path workspaceDir;

    private final AtomicInteger plainTransports = new AtomicInteger();
    private final AtomicInteger plainCalls = new AtomicInteger();
    private final AtomicInteger tlsTransports = new AtomicInteger();
    private final AtomicInteger tlsCalls = new AtomicInteger();
    private final AtomicInteger mtlsCalls = new AtomicInteger();
    private final AtomicReference<String> tlsProtocol = new AtomicReference<>();
    private final AtomicReference<String> mtlsPeerSubject = new AtomicReference<>();

    private Server plainServer;
    private int plainPort;
    private Server tlsServer;
    private int tlsPort;
    private Server mtlsServer;
    private int mtlsPort;

    @BeforeAll
    static void compileSchemaAndIssueCertificates() throws Exception {
        schema = JobsFleetTlsTestFixture.compileSchema();
        pki = JobsFleetTlsTestFixture.issueCertificates(pkiDir);
    }

    @BeforeEach
    void startTargetServers() throws Exception {
        plainTransports.set(0);
        plainCalls.set(0);
        tlsTransports.set(0);
        tlsCalls.set(0);
        mtlsCalls.set(0);
        tlsProtocol.set(null);
        mtlsPeerSubject.set(null);

        plainServer = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addTransportFilter(countingFilter(plainTransports))
                .addService(echoService(schema, plainCalls, null, null))
                .build()
                .start();
        plainPort = plainServer.getPort();

        tlsServer = Grpc.newServerBuilderForPort(0, TlsServerCredentials.create(
                        pki.serverCertPem().toFile(), pki.serverKeyPem().toFile()))
                .addTransportFilter(countingFilter(tlsTransports))
                .addService(echoService(schema, tlsCalls, tlsProtocol, null))
                .build()
                .start();
        tlsPort = tlsServer.getPort();

        mtlsServer = Grpc.newServerBuilderForPort(0, TlsServerCredentials.newBuilder()
                        .keyManager(pki.serverCertPem().toFile(), pki.serverKeyPem().toFile())
                        .trustManager(pki.caCertPem().toFile())
                        .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
                        .build())
                .addService(echoService(schema, mtlsCalls, null, mtlsPeerSubject))
                .build()
                .start();
        mtlsPort = mtlsServer.getPort();

        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(workspaceDir);
        repository.save(ServiceProfile.newBuilder()
                .setName(PROFILE_NAME)
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("serve_echo.proto")
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
                        .setName("secure")
                        .setHost("127.0.0.1")
                        .setPort(tlsPort)
                        .setTransport(Transport.TRANSPORT_TLS)
                        .setTrustRef("tls:serve-ca")
                        .build())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("mtls")
                        .setHost("127.0.0.1")
                        .setPort(mtlsPort)
                        .setTransport(Transport.TRANSPORT_TLS)
                        .setTrustRef("tls:serve-ca")
                        .setClientCertificateRef("tls:serve-client")
                        .build())
                .build());
    }

    @AfterEach
    void stopTargetServers() throws Exception {
        for (Server srv : new Server[] {plainServer, tlsServer, mtlsServer}) {
            if (srv != null) {
                srv.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
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
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "plain"), "denied");

            WorkflowRunRecord settled = awaitTerminal(store, UUID.fromString(jobId));
            assertThat(settled.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(settled.attempt).isEqualTo(1);
            assertThat(settled.error).contains("gRPC target host is not allowed: 127.0.0.1");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();
        }
    }

    @Test
    void stockProtoMoltServeWithProfileTlsConfigCompletesDurableMtlsPipelineJob()
            throws Exception {
        Path tlsConfigFile = writeProfileTlsConfig(
                workspaceDir.resolve("profile-tls.json"), pki, mtlsPort);
        ProtoMoltServe.JobsOptions jobsOptions = jobsOptions();
        ProtoMoltServe.Options parsed = ProtoMoltServe.Options.parse(new String[] {
                "--host", "127.0.0.1",
                "--grpc-port", "0",
                "--http-port", "0",
                "--jobs-jdbc", jobsOptions.jdbcUrl(),
                "--jobs-user", jobsOptions.username(),
                "--jobs-password", jobsOptions.password(),
                "--service-workspace", workspaceDir.toString(),
                "--profile-tls-config", tlsConfigFile.toString()
        });

        try (ProtoMoltServe ignored = ProtoMoltServe.start(parsed);
                WorkflowRunDatabase db = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                        jobsOptions.jdbcUrl(), jobsOptions.username(), jobsOptions.password()))) {
            JdbcWorkflowRunStore store = new JdbcWorkflowRunStore(db);
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(store, null, 4);
            String jobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "mtls"), "mtls-pipeline");

            WorkflowRunRecord settled = awaitTerminal(store, UUID.fromString(jobId));
            assertThat(settled.status).as("error: %s", settled.error)
                    .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(mtlsCalls.get()).isEqualTo(1);
            assertThat(mtlsPeerSubject.get()).isEqualTo("CN=serve-jobs-mtls-client");
            JsonNode result = PipelineJobCoordinator.view(settled).result();
            assertThat(result.path("echoed").asText()).isEqualTo("echo:mtls-pipeline");
            assertThat(result.path("operationId").asText()).isEqualTo(jobId + "/call_echo");
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
                .trustManager(pki.caCertPem().toFile())
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
        for (FileDescriptor fd : schema.files()) {
            context.registry().registerFile(fd);
        }
        var serviceProfiles = WorkspaceMount.serviceProfiles(workspaceDir);
        JobsFleetMount.Prepared fleet = JobsFleetMount.prepare(
                jobsOptions(), context, null, null, tlsRequiredPolicy, serviceProfiles,
                credentials);
        try {
            WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(fleet.store(), null, 4);

            String plainJobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "plain"), "plain-blocked");
            assertThat(fleet.worker().workOnce()).isTrue();
            WorkflowRunRecord plainFailed = fleet.store()
                    .get(UUID.fromString(plainJobId)).orElseThrow();
            assertThat(plainFailed.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
            assertThat(plainFailed.error)
                    .contains("plaintext outbound channels are disabled by policy");
            assertThat(plainTransports.get()).isZero();
            assertThat(plainCalls.get()).isZero();

            String tlsJobId = submitPipeline(
                    submitter, schema, echoPipeline(schema, "secure"), "serve-tls");
            assertThat(fleet.worker().workOnce()).isTrue();
            WorkflowRunRecord completed = fleet.store()
                    .get(UUID.fromString(tlsJobId)).orElseThrow();
            assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
            assertThat(tlsCalls.get()).isEqualTo(1);
            assertThat(tlsProtocol.get()).startsWith("TLS");
            JsonNode result = PipelineJobCoordinator.view(completed).result();
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
}
