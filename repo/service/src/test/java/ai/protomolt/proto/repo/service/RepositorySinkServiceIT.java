package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.authz.RequestCallerResolver;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.store.JdbcWorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunDatabase;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.protomolt.proto.jobs.service.worker.WorkflowRunWorker;
import ai.protomolt.proto.repo.archive.RepositorySinkRenderers;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.sink.v1.ArchiveRenditionSource;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkServiceGrpc;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end host integration suite for {@code RepositorySinkService} over {@link RepoServices},
 * PostgreSQL, LocalStack S3, Connect HTTP gateway, and {@link WorkflowRunWorker}:
 *
 * <ul>
 *   <li>Workflow runs without a {@code RepositorySinkService} step persist checkpoints only and
 *       create zero repository assets or revisions.
 *   <li>Explicit workflow sink steps preserve binary-faithful checkpoints ({@code bytes},
 *       full-range {@code uint64}, and unknown wire fields) across {@link JdbcWorkflowRunStore}
 *       requeue/resume, replay post-commit retries idempotently without duplicate revisions, and
 *       survive complete workflow-run row deletion.
 *   <li>Scoped {@link AccountBinding} callers enforce read-only capability/provenance discovery and
 *       fail closed at {@code callForCommit} when writer authority is revoked mid-conversion.
 *   <li>Durable sink jobs ({@link JdbcRepositorySinkJobStore}), renditions, and provenance survive
 *       full {@link RepoServices} shutdown and restart.
 * </ul>
 */
@Testcontainers
class RepositorySinkServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String OPERATOR = "synthetic-sink-it-operator-token";
    private static final String READER_TOKEN = "synthetic-sink-it-reader-token";
    private static final String WRITER_TOKEN = "synthetic-sink-it-writer-token";
    private static final String ARCHIVE = "sink-archive";
    private static final String ARCHIVE_DRIVE = "sink-archive-drive";
    private static final String DEFAULT_DATASOURCE = "ds-archive";

    private String accountId;

    private static final int UNKNOWN_TAG = 999;
    private static final ByteString UNKNOWN_WIRE_VALUE =
            ByteString.copyFromUtf8("unknown-wire-extension-preserved");
    private static final long FULL_RANGE_UINT64 =
            Long.parseUnsignedLong("18446744073709551613");
    private static final ByteString RAW_BINARY_BYTES =
            ByteString.copyFrom(new byte[] {0x00, (byte) 0xFF, 0x7F, (byte) 0x80, 0x42, 0x13});

    private static final String WORKFLOW_PROTO = """
            syntax = "proto3";
            package sink.it;

            message DomainRecord {
              string record_id = 1;
              string title = 2;
              bytes raw_signal = 3;
              uint64 exact_counter = 4;
            }

            message RenderMarkdownOptions {
              string heading_prefix = 1;
            }

            message WorkflowInput {
              string account_id = 1;
              string archive = 2;
              string entry_id = 3;
              string rendition_name = 4;
              string operation_id = 5;
              string workflow_run_id = 6;
              string record_id = 7;
              string title = 8;
            }

            message ExtractRequest {
              string record_id = 1;
              string title = 2;
            }

            message ExtractResponse {
              uint64 exact_counter = 1;
              bytes payload = 2;
              bytes descriptors = 3;
              string message_type = 4;
              string schema_fingerprint = 5;
            }

            message VerifyCheckpointRequest {
              uint64 exact_counter = 1;
              bytes payload = 2;
            }

            message VerifyCheckpointResponse {
              bool verified = 1;
              string summary = 2;
            }

            message PipelineSummary {
              string summary = 1;
              string published_operation_id = 2;
              int64 destination_version = 3;
              bool replayed = 4;
            }

            service Extractor {
              rpc Extract(ExtractRequest) returns (ExtractResponse);
            }

            service CheckpointVerifier {
              rpc Verify(VerifyCheckpointRequest) returns (VerifyCheckpointResponse);
            }
            """;

    private FileDescriptor workflowFile;
    private Descriptor domainRecordDesc;
    private Descriptor renderOptionsDesc;
    private FileDescriptorSet domainClosure;
    private String domainFingerprint;
    private String combinedWorkflowDescriptorsBase64;

    private RepoServiceConfig repoConfig;
    private AccessPolicyCallers accessPolicy;
    private RepoServices services;
    private ManagedChannel repoChannel;
    private String httpBase;

    private WorkflowRunDatabase jobsDatabase;
    private JdbcWorkflowRunStore jobsStore;
    private ActionContext actionContext;

    private Server workerStepServer;
    private String workerStepInProcessName;
    private final AtomicInteger extractInvocations = new AtomicInteger();
    private final AtomicInteger verifyInvocations = new AtomicInteger();
    private final AtomicBoolean failFirstVerify = new AtomicBoolean();
    private final AtomicBoolean dropFirstPublishReply = new AtomicBoolean();

    @BeforeEach
    void setUp() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler()
                .compile(ProtoSourceSet.builder()
                        .add("sink/it/workflow.proto", WORKFLOW_PROTO, "test")
                        .build());
        workflowFile = compiled.descriptorFor("sink/it/workflow.proto").orElseThrow();
        domainRecordDesc = workflowFile.findMessageTypeByName("DomainRecord");
        renderOptionsDesc = workflowFile.findMessageTypeByName("RenderMarkdownOptions");
        domainClosure = DescriptorFingerprints.closure(domainRecordDesc);
        domainFingerprint = DescriptorFingerprints.fingerprint(domainClosure);
        combinedWorkflowDescriptorsBase64 = Base64.getEncoder().encodeToString(
                buildCombinedDescriptorSet(
                        workflowFile,
                        PublishRepositorySinkRequest.getDescriptor().getFile())
                        .toByteArray());

        String uniqueSuffix = UUID.randomUUID().toString().substring(0, 8);
        accountId = "sink-it-acct-" + uniqueSuffix;
        String uniquePrefix = "it-sink-" + uniqueSuffix;
        repoConfig = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                uniquePrefix,
                0,
                null,
                null,
                0,
                0L)
                .withManagedStorage(new ManagedStoragePolicy(
                        uniquePrefix + "-orig", uniquePrefix + "-realm", true));

        accessPolicy = new AccessPolicyCallers(AccessPolicy.newBuilder()
                .addPrincipals(principal("reader", READER_TOKEN))
                .addPrincipals(principal("writer", WRITER_TOKEN))
                .build());

        bootRepoHost(defaultAccountBindings(true), RepositorySinkRenderers.none());

        as(OPERATOR, DriveServiceGrpc.newBlockingStub(repoChannel))
                .createDrive(CreateDriveRequest.newBuilder()
                        .setName(ARCHIVE_DRIVE)
                        .setAccountId(accountId)
                        .setDriveType(DriveType.DRIVE_TYPE_CUSTOM)
                        .build());
        as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                .createArchive(CreateArchiveRequest.newBuilder()
                        .setArchive(Archive.newBuilder()
                                .setName(ARCHIVE)
                                .setAccountId(accountId)
                                .setDriveName(ARCHIVE_DRIVE)
                                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED))
                        .build());

        try (var conn = services.ledgerDataSource().getConnection();
                var stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS jobs_it");
        }
        String jobsJdbcUrl = POSTGRES.getJdbcUrl()
                + (POSTGRES.getJdbcUrl().contains("?") ? "&" : "?")
                + "currentSchema=jobs_it";
        jobsDatabase = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                jobsJdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), 4, null));
        jobsStore = new JdbcWorkflowRunStore(jobsDatabase);
        actionContext = ActionContext.create();

        extractInvocations.set(0);
        verifyInvocations.set(0);
        failFirstVerify.set(false);
        dropFirstPublishReply.set(false);
        workerStepInProcessName = InProcessServerBuilder.generateName();
        workerStepServer = InProcessServerBuilder.forName(workerStepInProcessName)
                .directExecutor()
                .addService(extractorService())
                .addService(verifierService())
                .build()
                .start();
    }

    @AfterEach
    void tearDown() {
        if (workerStepServer != null) {
            workerStepServer.shutdownNow();
        }
        if (jobsDatabase != null) {
            jobsDatabase.close();
        }
        closeRepoHost();
    }

    @Test
    void workflowRunWithoutRepositorySinkStepPersistsCheckpointsOnlyAndCreatesZeroRepositoryAssets()
            throws Exception {
        String runId = UUID.randomUUID().toString();
        ObjectNode workflow = buildPersistOnlyWorkflow();
        ObjectNode input = workflowInputJson("entry-never-published", "op-unused", runId, "rec-1", "Draft Only");

        WorkflowRunWorker worker = newWorkflowWorker();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(jobsStore, null, 3);
        WorkflowRunSubmitter.Outcome submitted =
                submitter.submit(workflow, null, input, runId, actionContext);
        assertThat(submitted.ok()).as(submitted.error()).isTrue();

        assertThat(worker.workOnce()).isTrue();
        assertThat(worker.workOnce()).isFalse();

        WorkflowRunRecord completed = jobsStore.get(UUID.fromString(runId)).orElseThrow();
        assertThat(completed.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        JsonNode checkpoints = MAPPER.readTree(completed.checkpoints);
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(0).hasNonNull("responseBase64")).isTrue();

        assertThat(as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                .listEntries(ListEntriesRequest.newBuilder()
                        .setAccountId(accountId)
                        .setArchive(ARCHIVE)
                        .setLimit(50)
                        .build())
                .getEntriesList())
                .isEmpty();
        assertThat(as(OPERATOR, DocumentServiceGrpc.newBlockingStub(repoChannel))
                .listDocuments(ListDocumentsRequest.newBuilder()
                        .setAccountId(accountId)
                        .setPageSize(50)
                        .build())
                .getDocumentsList())
                .filteredOn(doc -> !doc.getAddress().getGraphId().startsWith("archive-def:"))
                .isEmpty();
    }

    @Test
    void workflowSinkStepPublishesBinaryFaithfulCheckpointAcrossResumeReplaysPostCommitRetryAndSurvivesWorkflowPurge()
            throws Exception {
        failFirstVerify.set(true);
        dropFirstPublishReply.set(true);

        String runId = UUID.randomUUID().toString();
        String entryId = "published-from-workflow";
        String operationId = "wf-sink-op-" + runId;
        ObjectNode workflow = buildPublishingWorkflow();
        ObjectNode input = workflowInputJson(entryId, operationId, runId, "rec-binary-42", "Binary Signal");

        WorkflowRunWorker worker = newWorkflowWorker();
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(jobsStore, null, 4);
        WorkflowRunSubmitter.Outcome submitted =
                submitter.submit(workflow, null, input, runId, actionContext);
        assertThat(submitted.ok()).as(submitted.error()).isTrue();
        UUID jobUuid = UUID.fromString(runId);

        // Attempt 1: 'extract' succeeds and checkpoints responseBase64 (including unknown field 999);
        // 'verify' fails UNAVAILABLE -> job requeues for Attempt 2.
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord afterAttempt1 = jobsStore.get(jobUuid).orElseThrow();
        assertThat(afterAttempt1.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(afterAttempt1.attempt).isEqualTo(1);
        assertThat(extractInvocations.get()).isEqualTo(1);
        assertThat(verifyInvocations.get()).isEqualTo(1);

        // Inspect stored Step 1 checkpoint directly in PostgreSQL: unknown tag 999 on ExtractResponse
        // is preserved in responseBase64 even though proto3 JSON preview omits unknown fields.
        JsonNode storedCheckpoints = MAPPER.readTree(afterAttempt1.checkpoints);
        assertThat(storedCheckpoints).hasSize(1);
        byte[] step1Wire = Base64.getDecoder()
                .decode(storedCheckpoints.get(0).get("responseBase64").asText());
        Descriptor extractRespDesc = workflowFile.findMessageTypeByName("ExtractResponse");
        DynamicMessage decodedStep1 = DynamicMessage.parseFrom(extractRespDesc, step1Wire);
        assertThat(decodedStep1.getUnknownFields().hasField(UNKNOWN_TAG)).isTrue();
        assertThat(decodedStep1.getUnknownFields().getField(UNKNOWN_TAG).getLengthDelimitedList())
                .containsExactly(UNKNOWN_WIRE_VALUE);

        // Attempt 2: 'extract' is NOT re-run; rebuilt from responseBase64! 'verify' verifies exact
        // uint64, raw binary bytes, and unknown tag 999 inside DomainRecord payload; 'publish_sink'
        // commits version 1 in RepoServices, then our fault injector drops the gRPC reply with UNAVAILABLE!
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord afterAttempt2 = jobsStore.get(jobUuid).orElseThrow();
        assertThat(afterAttempt2.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(afterAttempt2.attempt).isEqualTo(2);
        assertThat(extractInvocations.get()).isEqualTo(1);
        assertThat(verifyInvocations.get()).isEqualTo(2);

        // Version 1 already committed in RepoServices before the reply was dropped.
        EntryAddress publishedEntry = entry(entryId);
        GetEntryManifestResponse manifestAfterAttempt2 =
                as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                        .getEntryManifest(GetEntryManifestRequest.newBuilder()
                                .setAddress(publishedEntry)
                                .build());
        assertThat(manifestAfterAttempt2.getManifest().getVersion()).isEqualTo(1L);

        // Attempt 3: 'extract' and 'verify' replay from checkpoints; 'publish_sink' retries with the
        // SAME operation_id, receives the replayed receipt (replayed == true, destination_version == 1)
        // with ZERO duplicate revisions, and settles the workflow run COMPLETED!
        assertThat(worker.workOnce()).isTrue();
        WorkflowRunRecord settled = jobsStore.get(jobUuid).orElseThrow();
        assertThat(settled.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(extractInvocations.get()).isEqualTo(1);
        assertThat(verifyInvocations.get()).isEqualTo(2);
        JsonNode finalResult = MAPPER.readTree(settled.result);
        assertThat(finalResult.get("publishedOperationId").asText()).isEqualTo(operationId);
        assertThat(finalResult.get("destinationVersion").asLong()).isEqualTo(1L);
        assertThat(finalResult.get("replayed").asBoolean()).isTrue();

        GetEntryManifestResponse manifestAfterRetry =
                as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                        .getEntryManifest(GetEntryManifestRequest.newBuilder()
                                .setAddress(publishedEntry)
                                .build());
        assertThat(manifestAfterRetry.getManifest().getVersion()).isEqualTo(1L);

        // Purge workflow persistence rows completely - published asset, retained schema, and
        // RenditionProvenance have an independent repository lifecycle and remain intact!
        jobsDatabase.inTransaction(conn -> {
            try (PreparedStatement delEvents =
                            conn.prepareStatement(
                                    "DELETE FROM workflow_run_events_outbox WHERE kafka_key = ?");
                    PreparedStatement delRun =
                            conn.prepareStatement("DELETE FROM workflow_run WHERE job_id = ?")) {
                delEvents.setString(1, runId);
                delEvents.executeUpdate();
                delRun.setObject(1, jobUuid);
                assertThat(delRun.executeUpdate()).isEqualTo(1);
                return null;
            } catch (Exception sqlFailure) {
                throw new IllegalStateException(sqlFailure);
            }
        });
        assertThat(jobsStore.get(jobUuid)).isEmpty();

        byte[] readBackBytes = readAllRenditionBytes(publishedEntry, "raw_state");
        DynamicMessage readDomainRecord = DynamicMessage.parseFrom(domainRecordDesc, readBackBytes);
        assertThat(readDomainRecord.getField(domainRecordDesc.findFieldByName("record_id")))
                .isEqualTo("rec-binary-42");
        assertThat(readDomainRecord.getField(domainRecordDesc.findFieldByName("raw_signal")))
                .isEqualTo(RAW_BINARY_BYTES);
        assertThat(readDomainRecord.getField(domainRecordDesc.findFieldByName("exact_counter")))
                .isEqualTo(FULL_RANGE_UINT64);
        assertThat(readDomainRecord.getUnknownFields().getField(UNKNOWN_TAG).getLengthDelimitedList())
                .containsExactly(UNKNOWN_WIRE_VALUE);

        ListRenditionProvenanceResponse provenanceResp =
                as(OPERATOR, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listRenditionProvenance(ListRenditionProvenanceRequest.newBuilder()
                                .setAddress(publishedEntry)
                                .build());
        assertThat(provenanceResp.getEntriesList()).hasSize(1);
        RenditionProvenance prov = provenanceResp.getEntries(0).getProvenance();
        assertThat(prov.getSourceRevision()).isZero();
        assertThat(prov.getProcessingRunId()).isEqualTo(runId);
        assertThat(prov.getProcessingStepName()).isEqualTo("extract");
        assertThat(prov.getSchemaType()).isEqualTo(domainRecordDesc.getFullName());
        assertThat(prov.getSchemaFingerprint()).isEqualTo(domainFingerprint);
        assertThat(prov.getAuthoritativeProtobuf()).isTrue();
        assertThat(provenanceResp.getEntries(0).getSourceStale()).isFalse();
    }

    @Test
    void scopedCallerAuthorizationEnforcesReadOnlyDiscoveryAndFailsClosedWhenWriterRevokedMidConversion()
            throws Exception {
        CountDownLatch renderStarted = new CountDownLatch(1);
        CountDownLatch allowFinishRender = new CountDownLatch(1);
        AtomicInteger renderCalls = new AtomicInteger();

        RendererProvider gatedMarkdownRenderer = gatedMarkdownProvider(
                renderStarted, allowFinishRender, renderCalls);
        RepositorySinkRenderers mountedRenderers = RepositorySinkRenderers.of(
                List.of(gatedMarkdownRenderer),
                Set.of(gatedMarkdownRenderer.capability().getProviderId()));
        services.withRenderers(mountedRenderers);

        // Seed a source protobuf asset as OPERATOR with ACL granting account principals read/write.
        EntryAddress sourceEntry = entry("scoped-source-entry");
        DynamicMessage seedRecord = buildDomainRecord("rec-scoped-1", "Scoped Document");
        PublishRepositorySinkResponse seeded =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId("seed-scoped-source-1")
                                .setSource(processingSource(seedRecord, "run-seed", "seed"))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(sourceEntry)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("state")
                                                .setMediaType("application/x-protobuf")))
                                .build());
        assertThat(seeded.getJob().getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);

        SinkSource repoSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setArchiveRendition(ArchiveRenditionSource.newBuilder()
                                .setAddress(sourceEntry)
                                .setRendition(RenditionDescriptor.newBuilder().setName("state"))))
                .build();

        // 1. Read-only caller succeeds on ListSaveAsCapabilities (both gRPC and Connect HTTP gateway)
        // and ListRenditionProvenance, without invoking render or authorizing writes.
        ListSaveAsCapabilitiesResponse readerCaps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(repoSource)
                                .build());
        assertThat(readerCaps.getCapabilitiesList()).hasSize(1);
        assertThat(readerCaps.getCapabilities(0).getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);

        HttpResponse<byte[]> connectCaps = connectUnary(
                READER_TOKEN,
                RepositorySinkServiceGrpc.getListSaveAsCapabilitiesMethod().getFullMethodName(),
                ListSaveAsCapabilitiesRequest.newBuilder().setSource(repoSource).build());
        assertThat(connectCaps.statusCode()).isEqualTo(200);
        assertThat(ListSaveAsCapabilitiesResponse.parseFrom(connectCaps.body()).getCapabilitiesList())
                .hasSize(1);

        // 2. Read-only caller is refused with PERMISSION_DENIED on PublishRepositorySink BEFORE rendering.
        ConversionSpec markdownSpec = markdownConversionSpec(gatedMarkdownRenderer.capability());
        PublishRepositorySinkRequest readerPublishReq = PublishRepositorySinkRequest.newBuilder()
                .setOperationId("reader-forbidden-publish")
                .setSource(repoSource)
                .setConversion(markdownSpec)
                .setDestination(SinkDestination.newBuilder()
                        .setAddress(sourceEntry)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("summary_md")
                                .setMediaType("text/markdown")))
                .build();
        assertThatThrownBy(() -> as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(readerPublishReq))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.PERMISSION_DENIED));
        assertThat(renderCalls.get()).isZero();

        // 3. Writer-bound caller admits async conversion; while conversion is running, revoke writer's
        // IntakeWriterScope via withAccountBindings(...). Commit MUST fail closed with PERMISSION_DENIED!
        String revokedOpId = "writer-revoked-mid-flight";
        PublishRepositorySinkResponse admitted =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId(revokedOpId)
                                .setAsync(true)
                                .setSource(repoSource)
                                .setConversion(markdownSpec)
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(sourceEntry)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("summary_md")
                                                .setMediaType("text/markdown")))
                                .build());
        assertThat(admitted.getJob().getFrozenSourceRevision()).isEqualTo(1L);
        assertThat(renderStarted.await(10, TimeUnit.SECONDS)).isTrue();

        // Revoke WRITER's IntakeWriterScope while conversion is actively running!
        services.withAccountBindings(AccountBindingResolver.of(defaultAccountBindings(false)));
        allowFinishRender.countDown();

        RepositorySinkJob settledRevoked = awaitTerminalJob(WRITER_TOKEN, revokedOpId);
        assertThat(settledRevoked.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED);
        assertThat(settledRevoked.getFailure().getGrpcCode())
                .isEqualTo(Status.Code.PERMISSION_DENIED.value());
        assertThat(settledRevoked.getBytesRendered()).isGreaterThan(0L);
        assertThat(settledRevoked.getBytesCommitted()).isZero();

        // Source entry is untouched at version 1 with only its original 'state' rendition.
        GetEntryManifestResponse manifest =
                as(READER_TOKEN, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                        .getEntryManifest(GetEntryManifestRequest.newBuilder()
                                .setAddress(sourceEntry)
                                .build());
        assertThat(manifest.getManifest().getVersion()).isEqualTo(1L);
        assertThat(manifest.getManifest().getRenditionsList())
                .extracting(r -> r.getRendition().getName())
                .containsExactly("state");
    }

    @Test
    void sinkJobsRenditionsAndProvenanceSurviveHostRestartOverPostgresAndS3() throws Exception {
        RendererProvider markdownProvider = immediateMarkdownProvider();
        RepositorySinkRenderers mountedRenderers = RepositorySinkRenderers.of(
                List.of(markdownProvider),
                Set.of(markdownProvider.capability().getProviderId()));
        services.withRenderers(mountedRenderers);

        EntryAddress entryAddr = entry("durable-restart-entry");
        DynamicMessage record = buildDomainRecord("rec-restart-99", "Durable Across Restarts");
        String rawOpId = "op-restart-raw";
        PublishRepositorySinkResponse rawPublished =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId(rawOpId)
                                .setSource(processingSource(record, "run-restart-1", "ingest"))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(entryAddr)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("state")
                                                .setMediaType("application/x-protobuf")))
                                .build());
        assertThat(rawPublished.getJob().getProvenance().getDestinationVersion()).isEqualTo(1L);

        SinkSource repoSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setArchiveRendition(ArchiveRenditionSource.newBuilder()
                                .setAddress(entryAddr)
                                .setRendition(RenditionDescriptor.newBuilder().setName("state"))))
                .build();
        ConversionSpec spec = markdownConversionSpec(markdownProvider.capability());
        String mdOpId = "op-restart-md";
        PublishRepositorySinkRequest mdRequest = PublishRepositorySinkRequest.newBuilder()
                .setOperationId(mdOpId)
                .setSource(repoSource)
                .setConversion(spec)
                .setDestination(SinkDestination.newBuilder()
                        .setAddress(entryAddr)
                        .setExpectedDestinationVersion(1L)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("readme_md")
                                .setMediaType("text/markdown")))
                .build();
        PublishRepositorySinkResponse mdPublished =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(mdRequest);
        assertThat(mdPublished.getJob().getProvenance().getDestinationVersion()).isEqualTo(2L);
        assertThat(mdPublished.getJob().getProvenance().getAuthoritativeProtobuf()).isFalse();
        assertThat(mdPublished.getJob().getProvenance().getLossDescription())
                .contains("Projections omit raw binary");

        // Shut down RepoServices completely and boot a fresh instance over the same Postgres + S3!
        closeRepoHost();
        bootRepoHost(defaultAccountBindings(true), mountedRenderers);

        RepositorySinkJob persistedJob =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                                .setAccountId(accountId)
                                .setOperationId(mdOpId)
                                .build())
                        .getJob();
        assertThat(persistedJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        assertThat(persistedJob.getProvenance().getDestinationVersion()).isEqualTo(2L);

        // Re-submitting identical PublishRepositorySinkRequest after restart replays from
        // JdbcRepositorySinkJobStore without creating version 3!
        PublishRepositorySinkResponse replayedAfterRestart =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(mdRequest);
        assertThat(replayedAfterRestart.getJob().getProvenance().getReplayed()).isTrue();
        assertThat(replayedAfterRestart.getJob().getProvenance().getDestinationVersion()).isEqualTo(2L);

        // Reusing mdOpId with a different destination rendition fails with ALREADY_EXISTS.
        PublishRepositorySinkRequest conflictingReuse = mdRequest.toBuilder()
                .setDestination(mdRequest.getDestination().toBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("other_md")
                                .setMediaType("text/markdown")))
                .build();
        assertThatThrownBy(() -> as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(conflictingReuse))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.ALREADY_EXISTS));

        String markdownText = new String(
                readAllRenditionBytes(entryAddr, "readme_md"), StandardCharsets.UTF_8);
        assertThat(markdownText).isEqualTo("# Durable Across Restarts\n\nRecord: rec-restart-99\n");

        ListRenditionProvenanceResponse provenanceAfterRestart =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listRenditionProvenance(ListRenditionProvenanceRequest.newBuilder()
                                .setAddress(entryAddr)
                                .build());
        assertThat(provenanceAfterRestart.getCurrentEntryVersion()).isEqualTo(2L);
        assertThat(provenanceAfterRestart.getEntriesList()).hasSize(2);
    }

    // -------------------------------------------------------------------------
    // Helpers & Fixtures
    // -------------------------------------------------------------------------

    private void bootRepoHost(
            AccountBindings bindings, RepositorySinkRenderers renderers) {
        services = RepoServices.build(repoConfig)
                .withAccountBindings(AccountBindingResolver.of(bindings))
                .withRenderers(renderers);
        int grpcPort = services.startNetty(0, OPERATOR, accessPolicy).getPort();
        UploadHttpServer http = services.startHttp(
                0,
                OPERATOR,
                RepositoryConsole.notConfigured(),
                Map.of(),
                RepoServices.requestCallers(OPERATOR, accessPolicy, Optional.empty()));
        httpBase = "http://127.0.0.1:" + http.port();
        repoChannel = NettyChannelBuilder.forAddress("127.0.0.1", grpcPort)
                .usePlaintext()
                .maxInboundMessageSize(10 * 1024 * 1024)
                .build();
    }

    private void closeRepoHost() {
        if (repoChannel != null) {
            repoChannel.shutdownNow();
            repoChannel = null;
        }
        if (services != null) {
            services.close();
            services = null;
        }
    }

    private AccountBindings defaultAccountBindings(boolean writerEnabled) {
        AclIdentity sharedGroup = AclIdentity.newBuilder()
                .setIdentityType("group")
                .setIdentity("sink-team")
                .build();
        AccountBinding reader = AccountBinding.newBuilder()
                .setPrincipal("reader")
                .addAccountIds(accountId)
                .addAclIdentities(sharedGroup)
                .build();
        AccountBinding.Builder writer = AccountBinding.newBuilder()
                .setPrincipal("writer")
                .addAccountIds(accountId)
                .addAclIdentities(sharedGroup);
        if (writerEnabled) {
            writer.addIntakeWriters(IntakeWriterScope.newBuilder()
                    .setAccountId(accountId)
                    .setDatasourceId(DEFAULT_DATASOURCE)
                    .setAllowCreate(true));
        }
        return AccountBindings.newBuilder()
                .addBindings(reader)
                .addBindings(writer)
                .build();
    }

    private WorkflowRunWorker newWorkflowWorker() {
        WorkflowRunner runner = new WorkflowRunner(step -> {
            if ("repo-sink".equals(step.target())) {
                return new ForwardingManagedChannel(repoChannel, operatorAndFaultInterceptor());
            }
            return InProcessChannelBuilder.forName(workerStepInProcessName)
                    .directExecutor()
                    .build();
        });
        WorkflowRunsConfig workerConfig = new WorkflowRunsConfig(
                "sink-it-worker",
                1,
                Duration.ofSeconds(30),
                Duration.ofMillis(50),
                0L,
                4,
                4,
                null,
                WorkflowRunsConfig.DEFAULT_EVENTS_TOPIC,
                null,
                null);
        return new WorkflowRunWorker(jobsStore, actionContext, null, runner, workerConfig);
    }

    private ClientInterceptor operatorAndFaultInterceptor() {
        Metadata authHeaders = new Metadata();
        authHeaders.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), OPERATOR);
        ClientInterceptor attachToken = MetadataUtils.newAttachHeadersInterceptor(authHeaders);
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                Channel authedNext = ClientInterceptors.intercept(next, attachToken);
                ClientCall<ReqT, RespT> delegate = authedNext.newCall(method, callOptions);
                if (!method.getFullMethodName()
                        .equals(RepositorySinkServiceGrpc.getPublishRepositorySinkMethod()
                                .getFullMethodName())) {
                    return delegate;
                }
                return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        super.start(
                                new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                        responseListener) {
                                    private boolean swallowed;

                                    @Override
                                    public void onMessage(RespT message) {
                                        if (dropFirstPublishReply.compareAndSet(true, false)) {
                                            swallowed = true;
                                            return;
                                        }
                                        super.onMessage(message);
                                    }

                                    @Override
                                    public void onClose(Status status, Metadata trailers) {
                                        if (swallowed) {
                                            super.onClose(
                                                    Status.UNAVAILABLE.withDescription(
                                                            "simulated lost publish reply after commit"),
                                                    new Metadata());
                                            return;
                                        }
                                        super.onClose(status, trailers);
                                    }
                                },
                                headers);
                    }
                };
            }
        };
    }

    private ObjectNode buildPersistOnlyWorkflow() {
        ObjectNode wf = MAPPER.createObjectNode();
        wf.put("name", "persist-only-pipeline");
        wf.putObject("schema").put("descriptorSetBase64", combinedWorkflowDescriptorsBase64);
        wf.put("inputType", "sink.it.WorkflowInput");
        ArrayNode steps = wf.putArray("steps");

        ObjectNode extract = steps.addObject();
        extract.put("name", "extract");
        extract.put("target", "worker-steps");
        extract.put("method", "sink.it.Extractor/Extract");
        extract.putArray("rules")
                .add("record_id = input.record_id")
                .add("title = input.title");

        ObjectNode verify = steps.addObject();
        verify.put("name", "verify");
        verify.put("target", "worker-steps");
        verify.put("method", "sink.it.CheckpointVerifier/Verify");
        verify.putArray("rules")
                .add("exact_counter = extract.exact_counter")
                .add("payload = extract.payload");

        ObjectNode out = wf.putObject("output");
        out.put("type", "sink.it.PipelineSummary");
        out.putArray("rules").add("summary = verify.summary");
        return wf;
    }

    private ObjectNode buildPublishingWorkflow() {
        ObjectNode wf = buildPersistOnlyWorkflow();
        wf.put("name", "explicit-sink-pipeline");
        ArrayNode steps = (ArrayNode) wf.get("steps");

        ObjectNode publish = steps.addObject();
        publish.put("name", "publish_sink");
        publish.put("target", "repo-sink");
        publish.put("method", "ai.protomolt.proto.repo.sink.v1.RepositorySinkService/PublishRepositorySink");
        publish.putArray("rules")
                .add("operation_id = input.operation_id")
                .add("source.processing_state.account_id = input.account_id")
                .add("source.processing_state.message_type = extract.message_type")
                .add("source.processing_state.schema_fingerprint = extract.schema_fingerprint")
                .add("source.processing_state.descriptors = extract.descriptors")
                .add("source.processing_state.payload = extract.payload")
                .add("source.processing_state.run_id = input.workflow_run_id")
                .add("source.processing_state.step_name = \"extract\"")
                .add("destination.address.account_id = input.account_id")
                .add("destination.address.archive = input.archive")
                .add("destination.address.entry_id = input.entry_id")
                .add("destination.rendition.name = input.rendition_name")
                .add("destination.rendition.media_type = \"application/x-protobuf\"");

        ObjectNode out = (ObjectNode) wf.get("output");
        out.putArray("rules")
                .add("summary = verify.summary")
                .add("published_operation_id = publish_sink.job.operation_id")
                .add("destination_version = publish_sink.job.provenance.destination_version")
                .add("replayed = publish_sink.job.provenance.replayed");
        return wf;
    }

    private ObjectNode workflowInputJson(
            String entryId, String operationId, String runId, String recordId, String title) {
        ObjectNode in = MAPPER.createObjectNode();
        in.put("account_id", accountId);
        in.put("archive", ARCHIVE);
        in.put("entry_id", entryId);
        in.put("rendition_name", "raw_state");
        in.put("operation_id", operationId);
        in.put("workflow_run_id", runId);
        in.put("record_id", recordId);
        in.put("title", title);
        return in;
    }

    private ServerServiceDefinition extractorService() {
        ServiceDescriptor svc = workflowFile.findServiceByName("Extractor");
        var method = DynamicGrpcCalls.methodDescriptor(svc.findMethodByName("Extract"));
        Descriptor respDesc = workflowFile.findMessageTypeByName("ExtractResponse");
        return ServerServiceDefinition.builder(
                        io.grpc.ServiceDescriptor.newBuilder(svc.getFullName()).addMethod(method).build())
                .addMethod(
                        method,
                        ServerCalls.asyncUnaryCall((req, observer) -> {
                            extractInvocations.incrementAndGet();
                            String recordId = (String) req.getField(
                                    req.getDescriptorForType().findFieldByName("record_id"));
                            String title = (String) req.getField(
                                    req.getDescriptorForType().findFieldByName("title"));
                            DynamicMessage domainRecord = buildDomainRecord(recordId, title);
                            UnknownFieldSet unknowns = UnknownFieldSet.newBuilder()
                                    .addField(
                                            UNKNOWN_TAG,
                                            UnknownFieldSet.Field.newBuilder()
                                                    .addLengthDelimited(UNKNOWN_WIRE_VALUE)
                                                    .build())
                                    .build();
                            DynamicMessage response = DynamicMessage.newBuilder(respDesc)
                                    .setField(respDesc.findFieldByName("exact_counter"), FULL_RANGE_UINT64)
                                    .setField(respDesc.findFieldByName("payload"), domainRecord.toByteString())
                                    .setField(respDesc.findFieldByName("descriptors"), domainClosure.toByteString())
                                    .setField(respDesc.findFieldByName("message_type"), domainRecordDesc.getFullName())
                                    .setField(respDesc.findFieldByName("schema_fingerprint"), domainFingerprint)
                                    .setUnknownFields(unknowns)
                                    .build();
                            observer.onNext(response);
                            observer.onCompleted();
                        }))
                .build();
    }

    private ServerServiceDefinition verifierService() {
        ServiceDescriptor svc = workflowFile.findServiceByName("CheckpointVerifier");
        var method = DynamicGrpcCalls.methodDescriptor(svc.findMethodByName("Verify"));
        Descriptor respDesc = workflowFile.findMessageTypeByName("VerifyCheckpointResponse");
        return ServerServiceDefinition.builder(
                        io.grpc.ServiceDescriptor.newBuilder(svc.getFullName()).addMethod(method).build())
                .addMethod(
                        method,
                        ServerCalls.asyncUnaryCall((req, observer) -> {
                            verifyInvocations.incrementAndGet();
                            if (failFirstVerify.compareAndSet(true, false)) {
                                observer.onError(Status.UNAVAILABLE
                                        .withDescription("transient verifier restart")
                                        .asRuntimeException());
                                return;
                            }
                            long counter = (Long) req.getField(
                                    req.getDescriptorForType().findFieldByName("exact_counter"));
                            ByteString payload = (ByteString) req.getField(
                                    req.getDescriptorForType().findFieldByName("payload"));
                            if (counter != FULL_RANGE_UINT64) {
                                observer.onError(Status.DATA_LOSS
                                        .withDescription("uint64 precision lost across checkpoint: " + counter)
                                        .asRuntimeException());
                                return;
                            }
                            try {
                                DynamicMessage parsed = DynamicMessage.parseFrom(domainRecordDesc, payload);
                                ByteString signal = (ByteString) parsed.getField(
                                        domainRecordDesc.findFieldByName("raw_signal"));
                                long recCounter = (Long) parsed.getField(
                                        domainRecordDesc.findFieldByName("exact_counter"));
                                boolean hasUnknown = parsed.getUnknownFields().hasField(UNKNOWN_TAG);
                                if (!RAW_BINARY_BYTES.equals(signal)
                                        || recCounter != FULL_RANGE_UINT64
                                        || !hasUnknown) {
                                    observer.onError(Status.DATA_LOSS
                                            .withDescription("binary payload corrupted across checkpoint")
                                            .asRuntimeException());
                                    return;
                                }
                            } catch (Exception parseErr) {
                                observer.onError(Status.DATA_LOSS
                                        .withDescription("failed parsing DomainRecord")
                                        .withCause(parseErr)
                                        .asRuntimeException());
                                return;
                            }
                            DynamicMessage response = DynamicMessage.newBuilder(respDesc)
                                    .setField(respDesc.findFieldByName("verified"), true)
                                    .setField(respDesc.findFieldByName("summary"), "binary-checkpoint-verified")
                                    .build();
                            observer.onNext(response);
                            observer.onCompleted();
                        }))
                .build();
    }

    private DynamicMessage buildDomainRecord(String recordId, String title) {
        UnknownFieldSet unknowns = UnknownFieldSet.newBuilder()
                .addField(
                        UNKNOWN_TAG,
                        UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(UNKNOWN_WIRE_VALUE)
                                .build())
                .build();
        return DynamicMessage.newBuilder(domainRecordDesc)
                .setField(domainRecordDesc.findFieldByName("record_id"), recordId)
                .setField(domainRecordDesc.findFieldByName("title"), title)
                .setField(domainRecordDesc.findFieldByName("raw_signal"), RAW_BINARY_BYTES)
                .setField(domainRecordDesc.findFieldByName("exact_counter"), FULL_RANGE_UINT64)
                .setUnknownFields(unknowns)
                .build();
    }

    private SinkSource processingSource(
            DynamicMessage record, String workflowRunId, String stepName) {
        return SinkSource.newBuilder()
                .setProcessingState(ai.protomolt.proto.repo.sink.v1.ProcessingStateSource.newBuilder()
                        .setAccountId(accountId)
                        .setMessageType(domainRecordDesc.getFullName())
                        .setSchemaFingerprint(domainFingerprint)
                        .setDescriptors(domainClosure.toByteString())
                        .setPayload(record.toByteString())
                        .setRunId(workflowRunId)
                        .setStepName(stepName))
                .build();
    }

    private ConversionSpec markdownConversionSpec(RendererCapability cap) {
        return ConversionSpec.newBuilder()
                .setProviderId(cap.getProviderId())
                .setProviderVersion(cap.getProviderVersion())
                .setSourceProfile(cap.getSourceProfile())
                .setMediaType(cap.getMediaType())
                .setExtension(cap.getExtension())
                .setOptionsType(cap.getOptionsType())
                .setOptionsFingerprint(cap.getOptionsFingerprint())
                .build();
    }

    private RendererProvider immediateMarkdownProvider() {
        return markdownProvider("domain-markdown", null, null, new AtomicInteger());
    }

    private RendererProvider gatedMarkdownProvider(
            CountDownLatch started, CountDownLatch finishGate, AtomicInteger counter) {
        return markdownProvider("gated-domain-markdown", started, finishGate, counter);
    }

    private RendererProvider markdownProvider(
            String providerId,
            CountDownLatch started,
            CountDownLatch finishGate,
            AtomicInteger counter) {
        String optionsFp = DescriptorFingerprints.fingerprint(
                DescriptorFingerprints.closure(renderOptionsDesc));
        RendererCapability capability = RendererCapability.newBuilder()
                .setProviderId(providerId)
                .setProviderVersion("1.0.0")
                .setSourceProfile("sink.it.DomainRecord")
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(renderOptionsDesc.getFullName())
                .setOptionsFingerprint(optionsFp)
                .setMaxInputBytes(4 * 1024 * 1024)
                .setMaxOutputBytes(4 * 1024 * 1024)
                .setLossDescription("Projections omit raw binary signals and unknown protobuf fields.")
                .build();
        return new RendererProvider() {
            @Override
            public RendererCapability capability() {
                return capability;
            }

            @Override
            public Descriptor optionsDescriptor() {
                return renderOptionsDesc;
            }

            @Override
            public void check(Message input, Message options) throws RenderFailure {
                if (!input.getDescriptorForType().getFullName().equals(domainRecordDesc.getFullName())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED,
                            "Expected " + domainRecordDesc.getFullName());
                }
            }

            @Override
            public void render(
                    Message input, Message options, OutputStream output, Cancellation cancellation)
                    throws IOException {
                counter.incrementAndGet();
                check(input, options);
                Descriptor desc = input.getDescriptorForType();
                String recId = (String) input.getField(desc.findFieldByName("record_id"));
                String title = (String) input.getField(desc.findFieldByName("title"));
                byte[] rendered = ("# " + title + "\n\nRecord: " + recId + "\n")
                        .getBytes(StandardCharsets.UTF_8);
                try {
                    output.write(rendered);
                    output.flush();
                    if (started != null) {
                        started.countDown();
                    }
                    if (finishGate != null && !finishGate.await(15, TimeUnit.SECONDS)) {
                        throw new RenderFailure(RenderFailure.Kind.LIMIT, "Render gate timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RenderFailure(RenderFailure.Kind.CANCELLED, "Interrupted", interrupted);
                }
            }
        };
    }

    private RepositorySinkJob awaitTerminalJob(String token, String operationId) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadlineNanos) {
            RepositorySinkJob job = as(token, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                    .getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                            .setAccountId(accountId)
                            .setOperationId(operationId)
                            .build())
                    .getJob();
            if (job.getState() != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED
                    && job.getState() != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING) {
                return job;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("Timed out waiting for terminal job state: " + operationId);
    }

    private byte[] readAllRenditionBytes(EntryAddress address, String renditionName) {
        Iterator<ReadRenditionResponse> frames =
                as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                        .readRendition(ReadRenditionRequest.newBuilder()
                                .setAddress(address)
                                .setRendition(renditionName)
                                .build());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (frames.hasNext()) {
            ReadRenditionResponse frame = frames.next();
            if (frame.hasData()) {
                out.writeBytes(frame.getData().toByteArray());
            }
        }
        return out.toByteArray();
    }

    private HttpResponse<byte[]> connectUnary(String token, String procedure, Message request)
            throws Exception {
        HttpRequest httpReq = HttpRequest.newBuilder(
                        URI.create(httpBase + ConnectGateway.PATH + "/" + procedure))
                .header("Content-Type", ConnectGateway.UNARY_CONTENT_TYPE)
                .header("Connect-Protocol-Version", "1")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()))
                .build();
        return HTTP.send(httpReq, HttpResponse.BodyHandlers.ofByteArray());
    }

    private EntryAddress entry(String entryId) {
        return EntryAddress.newBuilder()
                .setAccountId(accountId)
                .setArchive(ARCHIVE)
                .setEntryId(entryId)
                .build();
    }

    private static Principal principal(String name, String credential) {
        return Principal.newBuilder()
                .setName(name)
                .addCredentialSha256(AccessPolicyCallers.sha256Hex(credential))
                .addScopes("artifact-access")
                .build();
    }

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static FileDescriptorSet buildCombinedDescriptorSet(FileDescriptor... roots) {
        FileDescriptorSet.Builder out = FileDescriptorSet.newBuilder();
        Set<String> visited = new HashSet<>();
        for (FileDescriptor root : roots) {
            appendFileDependenciesFirst(root, visited, out);
        }
        return out.build();
    }

    private static void appendFileDependenciesFirst(
            FileDescriptor file, Set<String> visited, FileDescriptorSet.Builder out) {
        if (!visited.add(file.getName())) {
            return;
        }
        for (FileDescriptor dep : file.getDependencies()) {
            appendFileDependenciesFirst(dep, visited, out);
        }
        out.addFile(file.toProto());
    }

    /**
     * ManagedChannel wrapper whose {@link #shutdownNow()} is a no-op over the shared {@code repoChannel}
     * so {@link WorkflowRunner}'s per-segment channel cleanup leaves {@code repoChannel} open.
     */
    private static final class ForwardingManagedChannel extends ManagedChannel {
        private final Channel intercepted;

        private ForwardingManagedChannel(ManagedChannel base, ClientInterceptor interceptor) {
            this.intercepted = ClientInterceptors.intercept(base, interceptor);
        }

        @Override
        public ManagedChannel shutdown() {
            return this;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public ManagedChannel shutdownNow() {
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return intercepted.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return intercepted.authority();
        }
    }
}
