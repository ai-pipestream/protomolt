package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.HealthProbe;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workspace.EnvProfileCredentialResolver;
import ai.protomolt.proto.receipt.Completeness;
import ai.protomolt.proto.receipt.CompletenessStatus;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.RecordSubject;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.SignedWorkRecord;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.repo.accession.AccessionGrpcService;
import ai.protomolt.proto.repo.accession.DocumentDeletes;
import ai.protomolt.proto.repo.accession.DocumentLists;
import ai.protomolt.proto.repo.accession.DocumentPurges;
import ai.protomolt.proto.repo.accession.DocumentReads;
import ai.protomolt.proto.repo.accession.DocumentSaves;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.external.v1.ExternalBindingLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalPendingPhase;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalRetentionPolicy;
import ai.protomolt.proto.repo.external.v1.ExternalServingVisibility;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.external.v1.ManagedSearchIdentity;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Timestamp;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * End-to-end public-path integration tests for Issue #814: versioned
 * {@link ExternalSystemBinding} configurations, {@link ExternalLink} publication receipts,
 * provider receipt verification, reconciliation, monotonic fencing, authorization,
 * tombstone/purge retention, and coordinator restart recovery over real PostgreSQL,
 * LocalStack S3, {@link GitSchemaRegistryStore}, and {@link FileSystemServiceProfileRepository}.
 */
class ExternalPublicationIT {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:18-alpine");
    private static final LocalStackContainer S3 =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");
    private static final String BUCKET = "repo-external-publication-it";
    private static final String ISSUER = "test-issuer";
    private static final String SECRET_TOKEN_V1 = "SUPER_SECRET_BEARER_TOKEN_V1_NEVER_PERSIST";
    private static final String SECRET_TOKEN_V2 = "SUPER_SECRET_BEARER_TOKEN_V2_NEVER_PERSIST";
    private static final String SHA_NLP_IN = "11".repeat(32);
    private static final String SHA_NLP_OUT = "22".repeat(32);
    private static final String SHA_NLP_RCPT = "33".repeat(32);
    private static final String SHA_GEN = "44".repeat(32);
    private static final String SHA_MANIFEST = "55".repeat(32);

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();
    private static S3Client s3Client;

    private static synchronized void ensureContainersStarted() {
        if (s3Client != null) {
            return;
        }
        POSTGRES.start();
        S3.start();
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(S3.getEndpoint().toString()))
                .region(Region.of(S3.getRegion()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create())
                .build();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @Test
    void successfulPublicationSeparatesNlpReceiptAcceptancePublicationAndServingVisibilityWithoutMutatingSource(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            ExternalSystemBinding binding = h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");
            assertThat(binding.getBindingRevision()).isEqualTo(1L);

            NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-1");
            h.saveSourceDocument("save-doc-1-r1", sourceAddr, 0L, "Quarterly Report", "alice");

            long sourceEventsBefore = h.catalog.pendingEvents(100).stream()
                    .filter(ev -> !ExternalPublicationCoordinates.isInternalExternalAddress(ev.event().getAddress()))
                    .count();
            assertThat(sourceEventsBefore).isEqualTo(1L);

            ExternalPublicationCoordinator.VerifiedSourceSnapshot verifiedSource =
                    h.coordinator.inspectSourceRevision(sourceAddr, 1L);
            assertThat(verifiedSource.partDigests()).isNotEmpty();
            assertThat(verifiedSource.retainedSchemaSha256s()).isNotEmpty();

            // Stateless Turboembed / NLP processing receipt digests exist prior to publication,
            // and provider initially returns ACCEPTED_ONLY (accepted, NOT yet published!).
            ExternalProcessingIdentity processing = ExternalProcessingIdentity.newBuilder()
                    .addAllSourcePartDigests(verifiedSource.partDigests())
                    .addAllRetainedSchemaSha256S(verifiedSource.retainedSchemaSha256s())
                    .setMappingId("managed-search-projection-v1")
                    .setGenerationDigest(SHA_GEN)
                    .setNlpInputSha256(SHA_NLP_IN)
                    .setNlpResultSha256(SHA_NLP_OUT)
                    .setNlpReceiptSha256(SHA_NLP_RCPT)
                    .build();

            provider.nextExecuteBehavior = ctx -> provider.acceptOnly(ctx, 10L, 200L);

            ExternalPublicationCoordinator.OperationResult acceptedOnly = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "pub-doc-1-r1",
                            sourceAddr,
                            1L,
                            "rust-search",
                            1L,
                            "ext-doc-1",
                            processing,
                            "https://search.example.com/collections/corpus-main/items/ext-doc-1"));

            assertThat(acceptedOnly.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
            assertThat(acceptedOnly.operation().getPendingPhase())
                    .isEqualTo(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_ACCEPTED_AWAITING_PUBLICATION);
            assertThat(acceptedOnly.currentLink()).isEmpty();
            assertThat(h.coordinator.listActiveExternalLinks("tenant-alpha", "rust-search"))
                    .isEmpty();

            // Provider commits index publication at catalog epoch 77; serving visibility still UNKNOWN.
            provider.commitCatalogPublication("pub-doc-1-r1", 77L, SHA_MANIFEST);
            int execCallsBeforeReconcile = provider.executeCalls.get();

            ExternalPublicationCoordinator.OperationResult confirmed =
                    h.coordinator.reconcileOperation(sourceAddr, "rust-search", "pub-doc-1-r1", false);
            assertThat(provider.executeCalls.get()).isEqualTo(execCallsBeforeReconcile);
            assertThat(confirmed.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(confirmed.currentLink()).isPresent();

            ExternalLink activeLink = confirmed.currentLink().orElseThrow();
            assertThat(activeLink.getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE);
            assertThat(activeLink.getSourceAddress()).isEqualTo(sourceAddr);
            assertThat(activeLink.getSourceRevision()).isEqualTo(1L);
            assertThat(activeLink.getBindingId()).isEqualTo("rust-search");
            assertThat(activeLink.getBindingRevision()).isEqualTo(1L);
            assertThat(activeLink.getExternalObjectId()).isEqualTo("ext-doc-1");
            assertThat(activeLink.getExternalVersion()).isEqualTo("epoch-77");
            assertThat(activeLink.getManagedSearch().getAcceptedVersion()).isEqualTo(10L);
            assertThat(activeLink.getManagedSearch().getAcceptedSequence()).isEqualTo(200L);
            assertThat(activeLink.getManagedSearch().getPublishedCatalogEpoch()).isEqualTo(77L);
            assertThat(activeLink.getManagedSearch().getServingVisibility())
                    .isEqualTo(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN);
            assertThat(activeLink.getConfirmationEvidence().hasSignedWorkRecordBytes()).isTrue();
            assertThat(activeLink.getProcessing().getNlpReceiptSha256()).isEqualTo(SHA_NLP_RCPT);

            // Separate serving visibility observation updates serving_visibility to CURRENT_ROWS.
            provider.markServingVisible("pub-doc-1-r1",
                    ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS);
            ExternalPublicationCoordinator.OperationResult withServing =
                    h.coordinator.reconcileOperation(sourceAddr, "rust-search", "pub-doc-1-r1", true);
            assertThat(withServing.currentLink().orElseThrow().getManagedSearch().getServingVisibility())
                    .isEqualTo(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS);
            assertThat(provider.executeCalls.get()).isEqualTo(execCallsBeforeReconcile);

            // Circular-update prevention proof: source document was NEVER modified or re-accessioned,
            // and every new outbox event belongs exclusively to internal graphs filtered by consumers.
            assertThat(h.catalog.current(sourceAddr).orElseThrow().revision()).isEqualTo(1L);
            var nonInternalEvents = h.catalog.pendingEvents(100).stream()
                    .filter(ev -> !ExternalPublicationCoordinates.isInternalExternalAddress(ev.event().getAddress()))
                    .toList();
            assertThat(nonInternalEvents).hasSize(1);
            assertThat(nonInternalEvents.getFirst().event().getAddress()).isEqualTo(sourceAddr);
            assertThat(nonInternalEvents.getFirst().event().getRevision()).isEqualTo(1L);
        }
    }

    @Test
    void rejectedPublicationRecordsFailedOutcomeWithoutActiveExternalLink(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");
            NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-rejected");
            h.saveSourceDocument("save-rejected", sourceAddr, 0L, "Malformed Doc", "alice");

            provider.nextExecuteBehavior = ctx -> provider.rejected(
                    ctx, "MAPPING_PROJECTION_REJECTED", "Missing mandatory facet field");

            ExternalPublicationCoordinator.OperationResult rejected = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "pub-rejected",
                            sourceAddr,
                            1L,
                            "rust-search",
                            1L,
                            "ext-rejected",
                            null,
                            null));

            assertThat(rejected.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
            assertThat(rejected.operation().getFailureCode()).isEqualTo("MAPPING_PROJECTION_REJECTED");
            assertThat(rejected.currentLink()).isEmpty();
            assertThat(h.coordinator.listActiveExternalLinks("tenant-alpha", "rust-search"))
                    .isEmpty();
        }
    }

    @Test
    void lostReplyReconcilesToConfirmedOrFailedWithoutDuplicateProviderExecution(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");

            // Case A: Provider published the write, but network reply was lost.
            NodeAddress docA = sourceAddress("tenant-alpha", "doc-lost-ok");
            h.saveSourceDocument("save-lost-a", docA, 0L, "Lost Reply Doc A", "alice");
            provider.nextExecuteBehavior = ctx -> {
                provider.recordPublishedSilently(ctx, 1L, 101L, 15L, SHA_MANIFEST);
                return new ExternalProviderGateway.ProviderCallResult.Unresolved(
                        "DEADLINE_EXCEEDED", "Transport deadline expired after commit");
            };

            ExternalPublicationCoordinator.OperationResult lostOk = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-lost-ok", docA, 1L, "rust-search", 1L, "ext-lost-a", null, null));
            assertThat(lostOk.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
            assertThat(lostOk.operation().getPendingPhase())
                    .isEqualTo(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY);
            assertThat(lostOk.currentLink()).isEmpty();
            assertThat(provider.executeCalls.get()).isEqualTo(1);

            ExternalPublicationCoordinator.OperationResult reconciledOk =
                    h.coordinator.reconcileOperation(docA, "rust-search", "op-lost-ok", false);
            assertThat(provider.executeCalls.get()).isEqualTo(1);
            assertThat(reconciledOk.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(reconciledOk.currentLink()).isPresent();
            assertThat(reconciledOk.currentLink().orElseThrow().getExternalVersion())
                    .isEqualTo("epoch-15");

            // Case B: Reply lost and provider status confirms deterministic rejection.
            NodeAddress docB = sourceAddress("tenant-alpha", "doc-lost-fail");
            h.saveSourceDocument("save-lost-b", docB, 0L, "Lost Reply Doc B", "alice");
            provider.nextExecuteBehavior = ctx -> {
                provider.recordRejectedSilently(ctx, "QUOTA_EXCEEDED");
                return new ExternalProviderGateway.ProviderCallResult.Unresolved(
                        "LOST_REPLY", "Stream closed before status trailers");
            };

            ExternalPublicationCoordinator.OperationResult lostFail = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-lost-fail", docB, 1L, "rust-search", 1L, "ext-lost-b", null, null));
            assertThat(lostFail.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);

            ExternalPublicationCoordinator.OperationResult reconciledFail =
                    h.coordinator.reconcileOperation(docB, "rust-search", "op-lost-fail", false);
            assertThat(provider.executeCalls.get()).isEqualTo(2);
            assertThat(reconciledFail.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
            assertThat(reconciledFail.operation().getFailureCode()).isEqualTo("QUOTA_EXCEEDED");
            assertThat(reconciledFail.currentLink()).isEmpty();
        }
    }

    @Test
    void sameIntentRetryIsIdempotentWhileAlteredIntentUnderSameOperationKeyRefusesConflict(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");
            NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-idempotent");
            h.saveSourceDocument("save-idem-r1", sourceAddr, 0L, "Idempotent v1", "alice");
            h.saveSourceDocument("save-idem-r2", sourceAddr, 1L, "Idempotent v2", "alice");

            ExternalPublicationCoordinator.PublishRequest original =
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-fixed-key",
                            sourceAddr,
                            1L,
                            "rust-search",
                            1L,
                            "ext-idem-1",
                            ExternalProcessingIdentity.newBuilder()
                                    .setMappingId("map-v1")
                                    .build(),
                            "https://search.example.com/items/ext-idem-1");

            ExternalPublicationCoordinator.OperationResult first = h.coordinator.publish(original);
            assertThat(first.replayed()).isFalse();
            assertThat(first.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(provider.executeCalls.get()).isEqualTo(1);

            ExternalPublicationCoordinator.OperationResult replay = h.coordinator.publish(original);
            assertThat(replay.replayed()).isTrue();
            assertThat(replay.operation().getOperationId()).isEqualTo("op-fixed-key");
            assertThat(provider.executeCalls.get()).isEqualTo(1);

            // Altered source revision under same operation_id must fail with ALREADY_EXISTS.
            ExternalPublicationCoordinator.PublishRequest alteredRevision =
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-fixed-key",
                            sourceAddr,
                            2L,
                            "rust-search",
                            1L,
                            "ext-idem-1",
                            ExternalProcessingIdentity.newBuilder()
                                    .setMappingId("map-v1")
                                    .build(),
                            "https://search.example.com/items/ext-idem-1");
            assertGrpcCode(() -> h.coordinator.publish(alteredRevision), Status.Code.ALREADY_EXISTS);

            // Altered external object ID under same operation_id must fail with ALREADY_EXISTS.
            ExternalPublicationCoordinator.PublishRequest alteredObject =
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-fixed-key",
                            sourceAddr,
                            1L,
                            "rust-search",
                            1L,
                            "ext-idem-DIFFERENT",
                            ExternalProcessingIdentity.newBuilder()
                                    .setMappingId("map-v1")
                                    .build(),
                            "https://search.example.com/items/ext-idem-1");
            assertGrpcCode(() -> h.coordinator.publish(alteredObject), Status.Code.ALREADY_EXISTS);
            assertThat(provider.executeCalls.get()).isEqualTo(1);
        }
    }

    @Test
    void concurrentSourceRevisionChangeFencesStaleRevisionWhilePreservingHistoricalLink(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");
            NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-race");
            h.saveSourceDocument("save-race-r1", sourceAddr, 0L, "Race v1", "alice");
            h.saveSourceDocument("save-race-r2", sourceAddr, 1L, "Race v2", "alice");
            h.saveSourceDocument("save-race-r3", sourceAddr, 2L, "Race v3", "alice");

            // Publish r2 first and confirm it.
            ExternalPublicationCoordinator.OperationResult pubR2 = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-race-r2", sourceAddr, 2L, "rust-search", 1L, "ext-race", null, null));
            assertThat(pubR2.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(pubR2.currentLink().orElseThrow().getSourceRevision()).isEqualTo(2L);

            // Now publish r3, superseding r2 into historical_links.
            ExternalPublicationCoordinator.OperationResult pubR3 = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-race-r3", sourceAddr, 3L, "rust-search", 1L, "ext-race", null, null));
            assertThat(pubR3.currentLink().orElseThrow().getSourceRevision()).isEqualTo(3L);

            int execCallsBeforeStaleR1 = provider.executeCalls.get();
            // Delayed out-of-order initial publish for older r1 MUST be fenced without calling provider!
            ExternalPublicationCoordinator.OperationResult staleR1 = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-race-r1-late", sourceAddr, 1L, "rust-search", 1L, "ext-race", null, null));
            assertThat(provider.executeCalls.get()).isEqualTo(execCallsBeforeStaleR1);
            assertThat(staleR1.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
            assertThat(staleR1.operation().getFailureCode()).isEqualTo("FENCED_BY_NEWER_STATE");

            ExternalPublicationCoordinator.ExternalLedgerView view =
                    h.coordinator.getLedgerView(sourceAddr, "rust-search").orElseThrow();
            assertThat(view.currentLink().orElseThrow().getSourceRevision()).isEqualTo(3L);
            assertThat(view.historicalLinks()).hasSize(1);
            assertThat(view.historicalLinks().getFirst().getSourceRevision()).isEqualTo(2L);
            assertThat(view.historicalLinks().getFirst().getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED);

            // Also verify an in-flight r1 lost-reply operation fenced by r2 on another document:
            NodeAddress docInFlight = sourceAddress("tenant-alpha", "doc-inflight-race");
            h.saveSourceDocument("save-if-r1", docInFlight, 0L, "InFlight r1", "alice");
            h.saveSourceDocument("save-if-r2", docInFlight, 1L, "InFlight r2", "alice");

            provider.nextExecuteBehavior = ctx -> {
                provider.recordPublishedSilently(ctx, 1L, 500L, 88L, SHA_MANIFEST);
                return new ExternalProviderGateway.ProviderCallResult.Unresolved(
                        "LOST_REPLY", "Delayed r1 network path");
            };
            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-if-r1", docInFlight, 1L, "rust-search", 1L, "ext-if", null, null));

            // r2 publishes and confirms while r1 is still unresolved.
            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-if-r2", docInFlight, 2L, "rust-search", 1L, "ext-if", null, null));

            // Late reconciliation of r1 records the verified historical receipt as SUPERSEDED
            // and MUST NOT overwrite r2's active currentLink!
            ExternalPublicationCoordinator.OperationResult lateR1Reconcile =
                    h.coordinator.reconcileOperation(docInFlight, "rust-search", "op-if-r1", false);
            assertThat(lateR1Reconcile.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(lateR1Reconcile.currentLink().orElseThrow().getSourceRevision()).isEqualTo(2L);
            assertThat(lateR1Reconcile.currentLink().orElseThrow().getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE);
            ExternalPublicationCoordinator.ExternalLedgerView ifView =
                    h.coordinator.getLedgerView(docInFlight, "rust-search").orElseThrow();
            assertThat(ifView.historicalLinks()).hasSize(1);
            assertThat(ifView.historicalLinks().getFirst().getSourceRevision()).isEqualTo(1L);
            assertThat(ifView.historicalLinks().getFirst().getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED);
        }
    }

    @Test
    void bindingRotationAndRetirementPreserveHistoricalSnapshotAndExcludeAllSecrets(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            ExternalSystemBinding v1 = h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-2025",
                    "ns-v1",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-v1");
            assertThat(v1.getBindingRevision()).isEqualTo(1L);
            assertThat(v1.getEndpointSummary().getHost()).isEqualTo("search-v1.internal");
            assertThat(v1.getEndpointSummary().getPort()).isEqualTo(9443);

            NodeAddress docAddr = sourceAddress("tenant-alpha", "doc-rotated");
            h.saveSourceDocument("save-rot-r1", docAddr, 0L, "Rotation v1", "alice");
            h.saveSourceDocument("save-rot-r2", docAddr, 1L, "Rotation v2", "alice");

            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-rot-1", docAddr, 1L, "rust-search", 1L, "ext-rot", null, null));

            // Rotate binding to revision 2 pointing at a new profile, host, port, dataset, and namespace.
            ExternalSystemBinding v2 = h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v2",
                    "ingest-tls",
                    "corpus-2026",
                    "ns-v2",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-v2");
            assertThat(v2.getBindingRevision()).isEqualTo(2L);
            assertThat(v2.getEndpointSummary().getHost()).isEqualTo("search-v2.internal");
            assertThat(v2.getEndpointSummary().getPort()).isEqualTo(9444);

            // Even if search-profile-v1's host is later changed in the profile repo, historical link keeps v1 snapshot!
            h.profileRepo.save(serviceProfile(
                    "search-profile-v1",
                    "ingest",
                    "decommissioned-host.internal",
                    9999,
                    Transport.TRANSPORT_TLS,
                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V1"));

            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-rot-2", docAddr, 2L, "rust-search", 2L, "ext-rot", null, null));

            // Retire the binding (creates revision 3 with RETIRED lifecycle).
            ExternalSystemBinding v3Retired = h.bindingStore.retireBinding(
                    "tenant-alpha",
                    "rust-search",
                    "Migrated search traffic to dedicated shard binding",
                    "op-bind-retire");
            assertThat(v3Retired.getBindingRevision()).isEqualTo(3L);
            assertThat(v3Retired.getLifecycle())
                    .isEqualTo(ExternalBindingLifecycle.EXTERNAL_BINDING_LIFECYCLE_RETIRED);

            // Retired binding refuses new publications targeting either v2 or v3!
            h.saveSourceDocument("save-rot-r3", docAddr, 2L, "Rotation v3", "alice");
            assertGrpcCode(
                    () -> h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                            "op-rot-refused", docAddr, 3L, "rust-search", 2L, "ext-rot", null, null)),
                    Status.Code.FAILED_PRECONDITION);

            // Historical binding revisions and ledger snapshots remain interpretable and secret-free.
            assertThat(h.bindingStore.getBindingRevision("tenant-alpha", "rust-search", 1L))
                    .contains(v1);
            assertThat(h.bindingStore.getBindingRevision("tenant-alpha", "rust-search", 2L))
                    .contains(v2);

            ExternalPublicationCoordinator.ExternalLedgerView view =
                    h.coordinator.getLedgerView(docAddr, "rust-search").orElseThrow();
            assertThat(view.currentLink().orElseThrow().getBindingSnapshot().getDataset())
                    .isEqualTo("corpus-2026");
            assertThat(view.historicalLinks()).hasSize(1);
            ExternalSystemBinding historicalSnapshot =
                    view.historicalLinks().getFirst().getBindingSnapshot();
            assertThat(historicalSnapshot.getBindingRevision()).isEqualTo(1L);
            assertThat(historicalSnapshot.getEndpointSummary().getHost())
                    .isEqualTo("search-v1.internal");
            assertThat(historicalSnapshot.getDataset()).isEqualTo("corpus-2025");
            assertThat(historicalSnapshot.getNamespace()).isEqualTo("ns-v1");

            // Verify config-lane JSON and persisted protobuf payloads contain ZERO secret material!
            String configName = ExternalPublicationCoordinates.configDocumentName(
                    "tenant-alpha", "rust-search");
            String configJson = h.gitRegistry.config(configName).orElseThrow();
            assertThat(configJson).doesNotContain(SECRET_TOKEN_V1).doesNotContain(SECRET_TOKEN_V2);
            String v1BytesUtf8 = new String(v1.toByteArray(), StandardCharsets.ISO_8859_1);
            String v2BytesUtf8 = new String(v2.toByteArray(), StandardCharsets.ISO_8859_1);
            assertThat(v1BytesUtf8).doesNotContain(SECRET_TOKEN_V1);
            assertThat(v2BytesUtf8).doesNotContain(SECRET_TOKEN_V2);
            assertThat(provider.lastAuthorizationHeader.get())
                    .isEqualTo("Bearer " + SECRET_TOKEN_V2);
        }
    }

    @Test
    void endpointUnavailabilityRecordsPendingUnresolvedWithoutFalseConfirmation(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");
            NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-unavail");
            h.saveSourceDocument("save-unavail", sourceAddr, 0L, "Outage Doc", "alice");

            provider.nextExecuteBehavior = ctx -> {
                throw Status.UNAVAILABLE
                        .withDescription("Connection refused: search-v1.internal:9443")
                        .asRuntimeException();
            };

            ExternalPublicationCoordinator.OperationResult outageResult = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-outage", sourceAddr, 1L, "rust-search", 1L, "ext-outage", null, null));

            assertThat(outageResult.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
            assertThat(outageResult.operation().getPendingPhase())
                    .isEqualTo(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY);
            assertThat(outageResult.currentLink()).isEmpty();
            assertThat(h.coordinator.listActiveExternalLinks("tenant-alpha", "rust-search"))
                    .isEmpty();

            // Retry with exact same operation key after endpoint recovers succeeds cleanly!
            ExternalPublicationCoordinator.OperationResult recovered = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-outage", sourceAddr, 1L, "rust-search", 1L, "ext-outage", null, null));
            assertThat(recovered.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(recovered.operation().getAttemptsCount()).isEqualTo(2);
            assertThat(recovered.currentLink()).isPresent();
        }
    }

    @Test
    void crossAccountAndUnauthorizedDocumentAclCallersAreRefused(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            h.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-1");

            NodeAddress aliceOnlyDoc = sourceAddress("tenant-alpha", "doc-alice-secret");
            NodeAddress sharedDoc = sourceAddress("tenant-alpha", "doc-shared");
            h.saveSourceDocument("save-alice-only", aliceOnlyDoc, 0L, "Alice Confidential", "alice");
            h.saveSourceDocument("save-shared", sharedDoc, 0L, "Shared Team Doc", "alice", "bob");

            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-alice-pub", aliceOnlyDoc, 1L, "rust-search", 1L, "ext-alice", null, null));
            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-shared-pub", sharedDoc, 1L, "rust-search", 1L, "ext-shared", null, null));

            // Cross-account caller (beta-mallory in tenant-beta) is refused everywhere on tenant-alpha!
            h.useCaller(h.betaMallory);
            assertGrpcCode(
                    () -> h.coordinator.getLedgerView(aliceOnlyDoc, "rust-search"),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.getOperationRecord(aliceOnlyDoc, "rust-search", "op-alice-pub"),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.reconcileOperation(aliceOnlyDoc, "rust-search", "op-alice-pub", false),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                            "op-mallory", aliceOnlyDoc, 1L, "rust-search", 1L, "ext-mallory", null, null)),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.bindingStore.getCurrentBinding("tenant-alpha", "rust-search"),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.listActiveExternalLinks("tenant-alpha", "rust-search"),
                    Status.Code.PERMISSION_DENIED);

            // Same-account caller (bob in tenant-alpha) WITHOUT ACL access to aliceOnlyDoc is refused on aliceOnlyDoc
            // and only sees sharedDoc in listActiveExternalLinks!
            h.useCaller(h.alphaBob);
            assertGrpcCode(
                    () -> h.coordinator.getLedgerView(aliceOnlyDoc, "rust-search"),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.getOperationRecord(aliceOnlyDoc, "rust-search", "op-alice-pub"),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.reconcileOperation(aliceOnlyDoc, "rust-search", "op-alice-pub", false),
                    Status.Code.PERMISSION_DENIED);
            assertGrpcCode(
                    () -> h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                            "op-bob-unauth", aliceOnlyDoc, 1L, "rust-search", 1L, "ext-bob", null, null)),
                    Status.Code.PERMISSION_DENIED);

            List<ExternalLink> bobVisibleLinks =
                    h.coordinator.listActiveExternalLinks("tenant-alpha", "rust-search");
            assertThat(bobVisibleLinks)
                    .extracting(ExternalLink::getSourceAddress)
                    .containsExactly(sharedDoc);
        }
    }

    @Test
    void tombstoneDeletionAndRevisionZeroPurgeFenceStaleRetriesAndHonorRetentionPolicies(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        try (Harness h = Harness.boot(tempDir, provider)) {
            // Binding 1: RETAIN_HISTORY
            h.saveActiveBinding(
                    "tenant-alpha",
                    "search-retain",
                    "search-profile-v1",
                    "ingest",
                    "corpus-retain",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-retain");

            NodeAddress docRetain = sourceAddress("tenant-alpha", "doc-tombstone-retain");
            h.saveSourceDocument("save-tr-1", docRetain, 0L, "To Delete And Retain", "alice");
            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-tr-pub-1", docRetain, 1L, "search-retain", 1L, "ext-tr-1", null, null));

            // Tombstone source document at revision 2.
            h.stub.deleteDocument(DeleteDocumentRequest.newBuilder()
                    .setAddress(docRetain)
                    .setExpected(ExpectedRevision.newBuilder().setRevision(1L))
                    .setOperationId("del-doc-retain-r2")
                    .setReason("Operator deleted document")
                    .build());

            ExternalPublicationCoordinator.OperationResult deleted = h.coordinator.deleteForTombstone(
                    new ExternalPublicationCoordinator.DeleteRequest(
                            "op-tr-del-2", docRetain, 2L, "search-retain", 0L, null));
            assertThat(deleted.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED);
            assertThat(deleted.currentLink()).isEmpty();

            // Delayed retry of pre-delete r1 publication MUST be fenced and MUST NOT restore ACTIVE!
            int execBeforeDelayed = provider.executeCalls.get();
            ExternalPublicationCoordinator.OperationResult delayedPreDelete = h.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-tr-delayed-r1", docRetain, 1L, "search-retain", 1L, "ext-tr-1", null, null));
            assertThat(provider.executeCalls.get()).isEqualTo(execBeforeDelayed);
            assertThat(delayedPreDelete.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED);
            assertThat(delayedPreDelete.operation().getFailureCode()).isEqualTo("FENCED_BY_NEWER_STATE");
            assertThat(delayedPreDelete.currentLink()).isEmpty();

            // Now execute revision-zero PurgeDocument as administrator, then handlePurge under RETAIN_HISTORY.
            h.stub.purgeDocument(PurgeDocumentRequest.newBuilder()
                    .setPurgeId("purge-retain-001")
                    .setAddress(docRetain)
                    .setReason("Compliance retention purge of source")
                    .build());
            assertThat(h.catalog.current(docRetain)).isEmpty();

            ExternalPublicationCoordinator.OperationResult purgeRetained = h.coordinator.handlePurge(
                    new ExternalPublicationCoordinator.PurgeRequest(
                            "op-tr-purge", docRetain, "purge-retain-001", "search-retain", null));
            assertThat(purgeRetained.ledgerPurged()).isFalse();
            assertThat(purgeRetained.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED);
            assertThat(purgeRetained.operation().getIntent().getSourceRevision()).isZero();
            assertThat(purgeRetained.operation().getIntent().getPurgeId()).isEqualTo("purge-retain-001");

            ExternalPublicationCoordinator.ExternalLedgerView retainedView =
                    h.coordinator.getLedgerView(docRetain, "search-retain").orElseThrow();
            assertThat(retainedView.sourcePurged()).isTrue();
            assertThat(retainedView.lastPurgeId()).contains("purge-retain-001");
            assertThat(retainedView.historicalLinks()).hasSize(1);
            assertThat(retainedView.historicalLinks().getFirst().getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);

            // Even after source purge, retained ledger enforces Alice's DocumentSecurity ACL (Bob is denied!).
            h.useCaller(h.alphaBob);
            assertGrpcCode(
                    () -> h.coordinator.getLedgerView(docRetain, "search-retain"),
                    Status.Code.PERMISSION_DENIED);
            h.useCaller(h.alphaAlice);

            // Binding 2: PURGE_WITH_SOURCE purges the ledger document itself after external deletion settles!
            h.saveActiveBinding(
                    "tenant-alpha",
                    "search-ephemeral",
                    "search-profile-v1",
                    "ingest",
                    "corpus-ephemeral",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_PURGE_WITH_SOURCE,
                    "op-bind-ephemeral");

            NodeAddress docPurgeAll = sourceAddress("tenant-alpha", "doc-purge-all");
            h.saveSourceDocument("save-pa-1", docPurgeAll, 0L, "Purge Everything", "alice");
            h.coordinator.publish(new ExternalPublicationCoordinator.PublishRequest(
                    "op-pa-pub-1", docPurgeAll, 1L, "search-ephemeral", 1L, "ext-pa-1", null, null));

            h.stub.purgeDocument(PurgeDocumentRequest.newBuilder()
                    .setPurgeId("purge-all-002")
                    .setAddress(docPurgeAll)
                    .setReason("Full erasure")
                    .build());

            ExternalPublicationCoordinator.OperationResult purgedWithSource = h.coordinator.handlePurge(
                    new ExternalPublicationCoordinator.PurgeRequest(
                            "op-pa-purge", docPurgeAll, "purge-all-002", "search-ephemeral", null));
            assertThat(purgedWithSource.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED);
            assertThat(purgedWithSource.ledgerPurged()).isTrue();

            NodeAddress ephemeralLedgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                    "tenant-alpha", "search-ephemeral", docPurgeAll);
            assertThat(h.catalog.current(ephemeralLedgerAddr)).isEmpty();
            assertThat(h.coordinator.getLedgerView(docPurgeAll, "search-ephemeral")).isEmpty();
        }
    }

    @Test
    void coordinatorRestartReloadsDurablePendingOperationsAndReconcilesCleanly(
            @TempDir Path tempDir) throws Exception {
        SimulatedManagedSearchProvider provider = new SimulatedManagedSearchProvider();
        NodeAddress sourceAddr = sourceAddress("tenant-alpha", "doc-restart");

        String savedJdbcUrl;
        String savedStackId;
        String savedPrefix;
        CallerIdentity savedAlice;
        CallerIdentity savedBob;
        CallerIdentity savedMallory;

        try (Harness firstHost = Harness.boot(tempDir, provider)) {
            savedJdbcUrl = firstHost.jdbcUrl;
            savedStackId = firstHost.stackId;
            savedPrefix = firstHost.s3Prefix;
            savedAlice = firstHost.alphaAlice;
            savedBob = firstHost.alphaBob;
            savedMallory = firstHost.betaMallory;

            firstHost.saveActiveBinding(
                    "tenant-alpha",
                    "rust-search",
                    "search-profile-v1",
                    "ingest",
                    "corpus-main",
                    "ns-alpha",
                    ExternalRetentionPolicy.EXTERNAL_RETENTION_POLICY_RETAIN_HISTORY,
                    "op-bind-restart");
            firstHost.saveSourceDocument("save-restart-doc", sourceAddr, 0L, "Across Restart", "alice");

            provider.nextExecuteBehavior = ctx -> {
                provider.recordPublishedSilently(ctx, 1L, 700L, 42L, SHA_MANIFEST);
                return new ExternalProviderGateway.ProviderCallResult.Unresolved(
                        "COORDINATOR_CRASH_WINDOW", "Process restarted before reply arrived");
            };

            ExternalPublicationCoordinator.OperationResult inFlight = firstHost.coordinator.publish(
                    new ExternalPublicationCoordinator.PublishRequest(
                            "op-survive-restart",
                            sourceAddr,
                            1L,
                            "rust-search",
                            1L,
                            "ext-restart-1",
                            null,
                            "https://search.example.com/items/ext-restart-1"));
            assertThat(inFlight.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
        }

        // Boot a brand-new host over the exact same Postgres DB, S3 prefix, Git registry, and profile dir.
        try (Harness restartedHost = Harness.restart(
                tempDir, savedJdbcUrl, savedStackId, savedPrefix,
                savedAlice, savedBob, savedMallory, provider)) {
            ExternalPublicationOperationRecord reloadedOp = restartedHost.coordinator
                    .getOperationRecord(sourceAddr, "rust-search", "op-survive-restart")
                    .orElseThrow();
            assertThat(reloadedOp.getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING);
            assertThat(reloadedOp.getPendingPhase())
                    .isEqualTo(ExternalPendingPhase.EXTERNAL_PENDING_PHASE_UNRESOLVED_LOST_REPLY);

            int execBeforeReconcile = provider.executeCalls.get();
            ExternalPublicationCoordinator.OperationResult settled = restartedHost.coordinator
                    .reconcileOperation(sourceAddr, "rust-search", "op-survive-restart", false);
            assertThat(provider.executeCalls.get()).isEqualTo(execBeforeReconcile);
            assertThat(settled.operation().getOutcome())
                    .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_CONFIRMED);
            assertThat(settled.currentLink()).isPresent();
            assertThat(settled.currentLink().orElseThrow().getExternalVersion())
                    .isEqualTo("epoch-42");
        }
    }

    private static NodeAddress sourceAddress(String accountId, String docId) {
        return NodeAddress.newBuilder()
                .setAccountId(accountId)
                .setGraphId("intake:" + accountId)
                .setGraphAddressId("ds-1")
                .setDocId(docId)
                .build();
    }

    private static void assertGrpcCode( Runnable action, Status.Code expectedCode) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        ex -> assertThat(ex.getStatus().getCode()).isEqualTo(expectedCode));
    }

    private static ServiceProfile serviceProfile(
            String profileName,
            String endpointName,
            String host,
            int port,
            Transport transport,
            String credentialRef) {
        String fp = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        return ServiceProfile.newBuilder()
                .setName(profileName)
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName(endpointName)
                        .setHost(host)
                        .setPort(port)
                        .setTransport(transport)
                        .setCredentialRef(credentialRef)
                        .build())
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("registry://descriptors/" + profileName)
                        .setDescriptorFingerprint(fp)
                        .setDescriptorArtifactRef(fp)
                        .build())
                .setHealthProbe(HealthProbe.newBuilder()
                        .setMethod("grpc.health.v1.Health/Check")
                        .setService("protomolt.search.v1.ManagedSearchService")
                        .setTimeout(Duration.newBuilder().setSeconds(2).build())
                        .setEnabled(true)
                        .build())
                .addMethodPolicies(MethodPolicy.newBuilder()
                        .setMethod("protomolt.search.v1.ManagedSearchService/Publish")
                        .addOperation(Operation.OPERATION_MUTATING)
                        .addOperation(Operation.OPERATION_IDEMPOTENT)
                        .setDeadline(Duration.newBuilder().setSeconds(5).build())
                        .setMaxAttempts(2)
                        .build())
                .setDescription("External system profile " + profileName)
                .build();
    }

    private record CallerIdentity(
            Caller caller,
            AuthenticatedCaller authenticated,
            RepositoryCaller repoCaller,
            boolean purgeAdmin) {
    }

    /**
     * Full repository + config-lane + service-profile + signed-receipt integration harness.
     */
    private static final class Harness implements AutoCloseable {
        final String jdbcUrl;
        final String stackId;
        final String s3Prefix;
        final PostgresCatalog catalog;
        final GitSchemaRegistryStore gitRegistry;
        final FileSystemServiceProfileRepository profileRepo;
        final Server server;
        final ManagedChannel channel;
        final DocumentServiceGrpc.DocumentServiceBlockingStub stub;
        final ExternalBindingStore bindingStore;
        final ExternalPublicationCoordinator coordinator;

        final CallerIdentity alphaAlice;
        final CallerIdentity alphaBob;
        final CallerIdentity betaMallory;
        private final AtomicReference<CallerIdentity> activeCaller;

        private Harness(
                String jdbcUrl,
                String stackId,
                String s3Prefix,
                PostgresCatalog catalog,
                GitSchemaRegistryStore gitRegistry,
                FileSystemServiceProfileRepository profileRepo,
                Server server,
                ManagedChannel channel,
                ExternalBindingStore bindingStore,
                ExternalPublicationCoordinator coordinator,
                CallerIdentity alphaAlice,
                CallerIdentity alphaBob,
                CallerIdentity betaMallory,
                AtomicReference<CallerIdentity> activeCaller) {
            this.jdbcUrl = jdbcUrl;
            this.stackId = stackId;
            this.s3Prefix = s3Prefix;
            this.catalog = catalog;
            this.gitRegistry = gitRegistry;
            this.profileRepo = profileRepo;
            this.server = server;
            this.channel = channel;
            this.stub = DocumentServiceGrpc.newBlockingStub(channel);
            this.bindingStore = bindingStore;
            this.coordinator = coordinator;
            this.alphaAlice = alphaAlice;
            this.alphaBob = alphaBob;
            this.betaMallory = betaMallory;
            this.activeCaller = activeCaller;
        }

        static Harness boot(Path tempDir, SimulatedManagedSearchProvider provider) throws Exception {
            ensureContainersStarted();
            String dbName = "ext_pub_" + DB_COUNTER.incrementAndGet() + "_" + System.nanoTime();
            try (var conn = DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
                conn.createStatement().execute("CREATE DATABASE " + dbName);
            }
            String jdbcUrl = replaceDatabaseInJdbcUrl(POSTGRES.getJdbcUrl(), dbName);
            PostgresCatalog catalog = new PostgresCatalog(
                    new CatalogConfig(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), 8));
            String s3Prefix = "ext-it/" + UUID.randomUUID();
            String stackId = catalog.registerStack(StackRecord.newBuilder()
                    .setStackId("stack-" + UUID.randomUUID())
                    .setProvider("s3")
                    .setEndpoint(S3.getEndpoint().toString())
                    .setNamespace(BUCKET + "/" + s3Prefix)
                    .build());

            CallerIdentity alice = provisionCaller(catalog, "tenant-alpha", "alice", true);
            CallerIdentity bob = provisionCaller(catalog, "tenant-alpha", "bob", false);
            CallerIdentity mallory = provisionCaller(catalog, "tenant-beta", "mallory", false);

            Path gitDir = tempDir.resolve("git-config-lane");
            GitSchemaRegistryStore gitRegistry = GitSchemaRegistryStore.builder()
                    .repositoryDir(gitDir)
                    .build();
            ExternalBindingStore.registerSchemaInConfigStore(gitRegistry);

            Path profilesDir = tempDir.resolve("service-profiles");
            FileSystemServiceProfileRepository profileRepo =
                    new FileSystemServiceProfileRepository(profilesDir);
            profileRepo.save(serviceProfile(
                    "search-profile-v1",
                    "ingest",
                    "search-v1.internal",
                    9443,
                    Transport.TRANSPORT_TLS,
                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V1"));
            profileRepo.save(serviceProfile(
                    "search-profile-v2",
                    "ingest-tls",
                    "search-v2.internal",
                    9444,
                    Transport.TRANSPORT_TLS,
                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V2"));

            return wireRuntime(
                    jdbcUrl,
                    stackId,
                    s3Prefix,
                    catalog,
                    gitRegistry,
                    profileRepo,
                    alice,
                    bob,
                    mallory,
                    provider);
        }

        static Harness restart(
                Path tempDir,
                String jdbcUrl,
                String stackId,
                String s3Prefix,
                CallerIdentity alice,
                CallerIdentity bob,
                CallerIdentity mallory,
                SimulatedManagedSearchProvider provider) throws IOException {
            PostgresCatalog catalog = new PostgresCatalog(
                    new CatalogConfig(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), 8));
            GitSchemaRegistryStore gitRegistry = GitSchemaRegistryStore.builder()
                    .repositoryDir(tempDir.resolve("git-config-lane"))
                    .build();
            FileSystemServiceProfileRepository profileRepo =
                    new FileSystemServiceProfileRepository(tempDir.resolve("service-profiles"));
            return wireRuntime(
                    jdbcUrl,
                    stackId,
                    s3Prefix,
                    catalog,
                    gitRegistry,
                    profileRepo,
                    alice,
                    bob,
                    mallory,
                    provider);
        }

        private static Harness wireRuntime(
                String jdbcUrl,
                String stackId,
                String s3Prefix,
                PostgresCatalog catalog,
                GitSchemaRegistryStore gitRegistry,
                FileSystemServiceProfileRepository profileRepo,
                CallerIdentity alice,
                CallerIdentity bob,
                CallerIdentity mallory,
                SimulatedManagedSearchProvider provider) throws IOException {
            Clock clock = Clock.fixed(Instant.parse("2026-11-14T04:00:00Z"), ZoneOffset.UTC);
            BlobStoreStack stack = new BlobStoreStack(
                    stackId, new S3BlobStore(s3Client), BUCKET, s3Prefix);
            DocumentSaves.Drives drives = new DocumentSaves.Drives("default", Map.of("default", stack));
            DocumentSchemaAdmission.Resolver schemaResolver =
                    ExternalPublicationSchemas.compose(int64PayloadResolver());
            DocumentSaves saves = new DocumentSaves(
                    catalog,
                    drives,
                    schemaResolver,
                    DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS,
                    bytes -> () -> {},
                    clock);
            DocumentReads reads = new DocumentReads(
                    catalog,
                    drives,
                    DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS,
                    bytes -> () -> {},
                    clock);
            DocumentDeletes deletes = new DocumentDeletes(catalog, clock);
            DocumentLists lists = new DocumentLists(catalog);

            AtomicReference<CallerIdentity> activeCaller = new AtomicReference<>(alice);
            Map<String, CallerIdentity> byName = Map.of(
                    alice.caller().name(), alice,
                    bob.caller().name(), bob,
                    mallory.caller().name(), mallory);

            DocumentPurges purges = new DocumentPurges(
                    catalog,
                    drives,
                    (boundCaller, accountId) -> {
                        CallerIdentity id = byName.get(boundCaller.principalName());
                        boolean allowed = id != null
                                && id.purgeAdmin()
                                && boundCaller.accountIds().contains(accountId);
                        return new PurgeAuthorization.Decision(allowed, allowed);
                    },
                    clock);

            AccessionGrpcService grpcService = new AccessionGrpcService(
                    saves,
                    reads,
                    deletes,
                    lists,
                    purges,
                    authenticated -> byName.get(authenticated.caller().name()).repoCaller());

            String serverName = "ext-pub-it-" + UUID.randomUUID();
            Server server = InProcessServerBuilder.forName(serverName)
                    .directExecutor()
                    .addService(ServerInterceptors.intercept(grpcService, new ServerInterceptor() {
                        @Override
                        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                                ServerCall<ReqT, RespT> call,
                                Metadata headers,
                                ServerCallHandler<ReqT, RespT> next) {
                            CallerIdentity current = activeCaller.get();
                            return Contexts.interceptCall(
                                    io.grpc.Context.current()
                                            .withValue(CallerContexts.CALLER, current.caller())
                                            .withValue(
                                                    CallerContexts.AUTHENTICATED_CALLER,
                                                    current.authenticated()),
                                    call,
                                    headers,
                                    next);
                        }
                    }))
                    .build()
                    .start();
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName)
                    .directExecutor()
                    .build();

            Map<String, String> secretEnv = Map.of(
                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V1", SECRET_TOKEN_V1,
                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V2", SECRET_TOKEN_V2);
            EnvProfileCredentialResolver credentialResolver = new EnvProfileCredentialResolver(
                    Set.of(
                            new EnvProfileCredentialResolver.Binding(
                                    "search-profile-v1",
                                    "ingest",
                                    "search-v1.internal",
                                    9443,
                                    Transport.TRANSPORT_TLS,
                                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V1"),
                            new EnvProfileCredentialResolver.Binding(
                                    "search-profile-v1",
                                    "ingest",
                                    "decommissioned-host.internal",
                                    9999,
                                    Transport.TRANSPORT_TLS,
                                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V1"),
                            new EnvProfileCredentialResolver.Binding(
                                    "search-profile-v2",
                                    "ingest-tls",
                                    "search-v2.internal",
                                    9444,
                                    Transport.TRANSPORT_TLS,
                                    "env:PROTOMOLT_TEST_SEARCH_TOKEN_V2")),
                    secretEnv::get);

            ExternalBindingStore bindingStore = new ExternalBindingStore(
                    channel, profileRepo, credentialResolver, gitRegistry, "default", clock);
            ExternalProviderVerifier verifier = new ExternalProviderVerifier(
                    provider.trustSnapshot, true);
            ExternalPublicationCoordinator coordinator = new ExternalPublicationCoordinator(
                    channel, bindingStore, provider, verifier, "default", clock);

            return new Harness(
                    jdbcUrl,
                    stackId,
                    s3Prefix,
                    catalog,
                    gitRegistry,
                    profileRepo,
                    server,
                    channel,
                    bindingStore,
                    coordinator,
                    alice,
                    bob,
                    mallory,
                    activeCaller);
        }

        void useCaller(CallerIdentity identity) {
            activeCaller.set(identity);
        }

        ExternalSystemBinding saveActiveBinding(
                String accountId,
                String bindingId,
                String profileId,
                String endpointId,
                String dataset,
                String namespace,
                ExternalRetentionPolicy retentionPolicy,
                String operationId) {
            return bindingStore.saveBinding(new ExternalBindingStore.BindingSpec(
                    accountId,
                    bindingId,
                    "protomolt-managed-search",
                    profileId,
                    endpointId,
                    dataset,
                    namespace,
                    "managed-search/v1",
                    retentionPolicy,
                    Map.of("environment", "integration-test"),
                    operationId));
        }

        void saveSourceDocument(
                String operationId,
                NodeAddress address,
                long expectedRevision,
                String title,
                String... permittedPrincipals) {
            DocumentSecurity.Builder sec = DocumentSecurity.newBuilder();
            for (String p : permittedPrincipals) {
                sec.addPermissions(AccessRule.newBuilder()
                        .setIdentityType("service")
                        .setIdentity(p)
                        .setAccess(Access.ACCESS_READ));
                sec.addPermissions(AccessRule.newBuilder()
                        .setIdentityType("service")
                        .setIdentity(p)
                        .setAccess(Access.ACCESS_WRITE));
            }
            byte[] rawBytes = ("payload of " + title).getBytes(StandardCharsets.UTF_8);
            Document doc = Document.newBuilder()
                    .setDocId(address.getDocId())
                    .setOwnership(OwnershipContext.newBuilder()
                            .setAccountId(address.getAccountId())
                            .setDatasourceId("ds-1")
                            .setSecurity(sec.build()))
                    .setSearchMetadata(SearchMetadata.newBuilder()
                            .setTitle(title)
                            .setBody("searchable body of " + title))
                    .setStructuredData(Any.pack(Int64Value.of(42L)))
                    .setBlobBag(BlobBag.newBuilder()
                            .setBlob(Blob.newBuilder()
                                    .setBlobId("b1")
                                    .setSizeBytes(rawBytes.length)
                                    .setData(ByteString.copyFrom(rawBytes))))
                    .putParserResults("tika", ParserResult.newBuilder()
                            .setParserName("tika")
                            .setParserVersion("2.9")
                            .setStatus(ParseStatus.PARSE_STATUS_OK)
                            .build())
                    .build();

            ExpectedRevision expected = expectedRevision == 0L
                    ? ExpectedRevision.newBuilder().setNone(true).build()
                    : ExpectedRevision.newBuilder().setRevision(expectedRevision).build();

            stub.saveDocument(SaveDocumentRequest.newBuilder()
                    .setOperationId(operationId)
                    .setTarget(DocumentTarget.newBuilder().setAddress(address))
                    .setExpected(expected)
                    .setDocument(doc)
                    .addWrites(PartWrite.newBuilder()
                            .setSelector(PartSelector.newBuilder()
                                    .setPart(DocumentPart.DOCUMENT_PART_CORE))
                            .setAction(WriteAction.WRITE_ACTION_PUT))
                    .addWrites(PartWrite.newBuilder()
                            .setSelector(PartSelector.newBuilder()
                                    .setPart(DocumentPart.DOCUMENT_PART_BLOBS))
                            .setAction(WriteAction.WRITE_ACTION_PUT))
                    .addWrites(PartWrite.newBuilder()
                            .setSelector(PartSelector.newBuilder()
                                    .setPart(DocumentPart.DOCUMENT_PART_PARSED))
                            .setAction(WriteAction.WRITE_ACTION_PUT))
                    .setDrive("default")
                    .setOwner(SaveOwner.newBuilder()
                            .setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-1")))
                    .build());
        }

        private static CallerIdentity provisionCaller(
                PostgresCatalog catalog,
                String accountId,
                String principalName,
                boolean purgeAdmin) {
            UUID credId = UUID.randomUUID();
            catalog.registerCredential(ISSUER, credId, principalName);
            installIntakeGrant(catalog, accountId, credId, "ds-1");
            installIntakeGrant(
                    catalog,
                    accountId,
                    credId,
                    ExternalPublicationCoordinates.DEFAULT_BINDINGS_DATASOURCE_ID);
            installIntakeGrant(
                    catalog,
                    accountId,
                    credId,
                    ExternalPublicationCoordinates.DEFAULT_PUBLICATIONS_DATASOURCE_ID);

            Caller caller = Caller.scoped(principalName, Set.of());
            AuthenticatedCaller authenticated = new AuthenticatedCaller(
                    caller, Optional.of(new CredentialBinding(ISSUER, credId, 1L)));
            RepositoryCaller repoCaller = new RepositoryCaller(
                    principalName,
                    false,
                    Set.of(accountId),
                    Set.of(Principal.newBuilder()
                            .setIdentityType("service")
                            .setIdentity(principalName)
                            .build()),
                    Optional.of(new RepositoryCredentialBinding(ISSUER, credId, 1L)));
            return new CallerIdentity(caller, authenticated, repoCaller, purgeAdmin);
        }

        private static void installIntakeGrant(
                PostgresCatalog catalog, String accountId, UUID credId, String datasourceId) {
            catalog.installGrant(WriterGrant.newBuilder()
                    .setGrantId("grant-" + datasourceId + "-" + UUID.randomUUID())
                    .setAccountId(accountId)
                    .setCredential(WriterCredential.newBuilder()
                            .setIssuer(ISSUER)
                            .setCredentialId(credId.toString()))
                    .setIntake(IntakeOrigin.newBuilder().setDatasourceId(datasourceId))
                    .setAllowCreate(true)
                    .build());
        }

        private static DocumentSchemaAdmission.Resolver int64PayloadResolver() {
            String int64Url = "type.googleapis.com/" + Int64Value.getDescriptor().getFullName();
            var closure = DescriptorFingerprints.closure(Int64Value.getDescriptor());
            ByteString bytes = closure.toByteString();
            RepositorySchemaAsset metadata = RepositorySchemaAsset.newBuilder()
                    .setTypeUrl(int64Url)
                    .setArtifactSha256(ExternalOperationKeys.sha256Hex(bytes.toByteArray()))
                    .setSchema(PublicationSchemaCondition.newBuilder()
                            .setTypeName(Int64Value.getDescriptor().getFullName())
                            .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                    .setCompilation(SchemaCompilationProvenance.newBuilder()
                            .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                            .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                            .setUnknownCompilerReason("well-known type compiled into protobuf-java")
                            .setAdmissionRuntime(SchemaToolIdentity.newBuilder()
                                    .setName("protobuf-java")
                                    .setVersion("test")))
                    .build();
            DocumentSchemaAdmission.Definition def =
                    new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
            return selection -> int64Url.equals(selection.typeUrl()) ? def : null;
        }

        private static String replaceDatabaseInJdbcUrl(String baseJdbcUrl, String dbName) {
            int qIndex = baseJdbcUrl.indexOf('?');
            String withoutQuery = qIndex >= 0 ? baseJdbcUrl.substring(0, qIndex) : baseJdbcUrl;
            String query = qIndex >= 0 ? baseJdbcUrl.substring(qIndex) : "";
            int lastSlash = withoutQuery.lastIndexOf('/');
            return withoutQuery.substring(0, lastSlash + 1) + dbName + query;
        }

        @Override
        public void close() {
            channel.shutdownNow();
            server.shutdownNow();
            gitRegistry.close();
            catalog.close();
        }
    }

    /**
     * Stateful simulated managed-search provider issuing Ed25519-signed {@link SignedWorkRecord}
     * receipts and modeling distinct acceptance, catalog publication, serving visibility,
     * rejections, and lost replies.
     */
    private static final class SimulatedManagedSearchProvider implements ExternalProviderGateway {
        private static final Metadata.Key<String> AUTHORIZATION_KEY =
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        private static final Timestamp VERIFIED_TIME =
                Timestamp.newBuilder().setSeconds(1_794_628_800L).build();

        final TrustSnapshot trustSnapshot;
        private final RecordSigner receiptSigner;
        private final AtomicLong seqCounter = new AtomicLong(100L);
        private final AtomicLong epochCounter = new AtomicLong(10L);
        final AtomicInteger executeCalls = new AtomicInteger();
        final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();
        final Map<String, ExternalProviderEvidence> recordedByOperation = new ConcurrentHashMap<>();
        volatile Function<ProviderCallRequest, ProviderCallResult> nextExecuteBehavior;

        SimulatedManagedSearchProvider() {
            KeyPair pair = RecordKeys.generate();
            this.receiptSigner = new RecordSigner("provider-key-1", pair.getPrivate());
            this.trustSnapshot = TrustSnapshot.newBuilder()
                    .addIssuers(TrustedIssuer.newBuilder()
                            .setIssuer("managed-search-provider")
                            .addSubjectKinds("workflow-run")
                            .addKeys(TrustedKey.newBuilder()
                                    .setKeyId("provider-key-1")
                                    .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                                    .setPublicKey(ByteString.copyFrom(
                                            RecordKeys.rawPublicKey(pair.getPublic())))
                                    .setState(KeyState.KEY_STATE_ACTIVE)))
                    .build();
        }

        @Override
        public ProviderCallResult execute(ProviderCallRequest context) {
            executeCalls.incrementAndGet();
            String authHeader = context.auth().resolvedHeaders().get(AUTHORIZATION_KEY);
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                throw Status.UNAUTHENTICATED
                        .withDescription("Missing outbound Bearer credential header")
                        .asRuntimeException();
            }
            lastAuthorizationHeader.set(authHeader);

            Function<ProviderCallRequest, ProviderCallResult> custom = nextExecuteBehavior;
            if (custom != null) {
                nextExecuteBehavior = null;
                return custom.apply(context);
            }

            return switch (context.intent().getAction()) {
                case EXTERNAL_PUBLICATION_ACTION_PUBLISH -> {
                    long seq = seqCounter.incrementAndGet();
                    long epoch = epochCounter.incrementAndGet();
                    ExternalProviderEvidence evidence = buildPublishedEvidence(
                            context, 1L, seq, epoch, SHA_MANIFEST,
                            ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN);
                    recordedByOperation.put(context.operationId(), evidence);
                    yield new ProviderCallResult.Receipt(evidence);
                }
                case EXTERNAL_PUBLICATION_ACTION_DELETE -> {
                    ExternalProviderEvidence delEvidence = buildDeletedEvidence(context);
                    recordedByOperation.put(context.operationId(), delEvidence);
                    yield new ProviderCallResult.Receipt(delEvidence);
                }
                default -> throw new IllegalStateException("Unexpected action");
            };
        }

        @Override
        public ProviderStatusResult queryStatus(ProviderStatusQuery query) {
            String authHeader = query.auth().resolvedHeaders().get(AUTHORIZATION_KEY);
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                throw Status.UNAUTHENTICATED
                        .withDescription("Missing outbound Bearer credential header")
                        .asRuntimeException();
            }
            ExternalProviderEvidence recorded = recordedByOperation.get(query.operationId());
            if (recorded == null) {
                return new ProviderStatusResult.NoRecordedOutcome(
                        "Operation not yet observed by provider");
            }
            return new ProviderStatusResult.Receipt(recorded);
        }

        ProviderCallResult acceptOnly(
                ProviderCallRequest ctx, long acceptedVer, long acceptedSeq) {
            ByteString docKey = ByteString.copyFrom(ExternalPublicationCoordinates.logicalSourceKey(
                    ctx.intent().getSourceAddress()));
            ExternalProviderEvidence evidence = baseEvidenceBuilder(ctx)
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY)
                    .setManagedSearch(ManagedSearchIdentity.newBuilder()
                            .setHistoryId(ByteString.copyFrom(new byte[16]))
                            .setDocumentKey(docKey)
                            .setIndexKey(ByteString.copyFromUtf8(ctx.auth().binding().getDataset()))
                            .setAcceptedVersion(acceptedVer)
                            .setAcceptedSequence(acceptedSeq)
                            .setServingVisibility(
                                    ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN))
                    .build();
            recordedByOperation.put(ctx.operationId(), evidence);
            return new ProviderCallResult.Receipt(evidence);
        }

        void commitCatalogPublication(
                String operationId, long catalogEpoch, String afterManifestSha256) {
            ExternalProviderEvidence current = recordedByOperation.get(operationId);
            ExternalProviderEvidence promoted = current.toBuilder()
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED)
                    .setExternalVersion("epoch-" + catalogEpoch)
                    .setManagedSearch(current.getManagedSearch().toBuilder()
                            .setPublishedCatalogEpoch(catalogEpoch)
                            .setAfterManifestSha256(afterManifestSha256))
                    .build();
            recordedByOperation.put(operationId, promoted);
        }

        void markServingVisible(String operationId, ExternalServingVisibility visibility) {
            ExternalProviderEvidence current = recordedByOperation.get(operationId);
            ExternalProviderEvidence updated = current.toBuilder()
                    .setManagedSearch(current.getManagedSearch().toBuilder()
                            .setServingVisibility(visibility))
                    .build();
            recordedByOperation.put(operationId, updated);
        }

        void recordPublishedSilently(
                ProviderCallRequest ctx,
                long acceptedVer,
                long acceptedSeq,
                long epoch,
                String manifestSha) {
            recordedByOperation.put(
                    ctx.operationId(),
                    buildPublishedEvidence(
                            ctx,
                            acceptedVer,
                            acceptedSeq,
                            epoch,
                            manifestSha,
                            ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN));
        }

        void recordRejectedSilently(ProviderCallRequest ctx, String code) {
            ExternalProviderEvidence rej = baseEvidenceBuilder(ctx)
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED)
                    .setDiagnosticCode(code)
                    .build();
            recordedByOperation.put(ctx.operationId(), rej);
        }

        ProviderCallResult rejected(
                ProviderCallRequest ctx, String code, String reason) {
            ExternalProviderEvidence rej = baseEvidenceBuilder(ctx)
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_REJECTED)
                    .setDiagnosticCode(code)
                    .build();
            recordedByOperation.put(ctx.operationId(), rej);
            return new ProviderCallResult.Receipt(rej);
        }

        private ExternalProviderEvidence buildPublishedEvidence(
                ProviderCallRequest ctx,
                long acceptedVer,
                long acceptedSeq,
                long epoch,
                String manifestSha,
                ExternalServingVisibility visibility) {
            ByteString docKey = ByteString.copyFrom(ExternalPublicationCoordinates.logicalSourceKey(
                    ctx.intent().getSourceAddress()));
            return baseEvidenceBuilder(ctx)
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED)
                    .setExternalVersion("epoch-" + epoch)
                    .setManagedSearch(ManagedSearchIdentity.newBuilder()
                            .setHistoryId(ByteString.copyFrom(new byte[16]))
                            .setDocumentKey(docKey)
                            .setIndexKey(ByteString.copyFromUtf8(ctx.auth().binding().getDataset()))
                            .setAcceptedVersion(acceptedVer)
                            .setAcceptedSequence(acceptedSeq)
                            .setPublishedCatalogEpoch(epoch)
                            .setAfterManifestSha256(manifestSha)
                            .setServingVisibility(visibility))
                    .build();
        }

        private ExternalProviderEvidence buildDeletedEvidence(ProviderCallRequest ctx) {
            return baseEvidenceBuilder(ctx)
                    .setSettlement(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED)
                    .build();
        }

        private ExternalProviderEvidence.Builder baseEvidenceBuilder(
                ProviderCallRequest ctx) {
            String slugId = "rcpt-" + UUID.randomUUID().toString();
            WorkRecord manifest = WorkRecord.newBuilder()
                    .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                    .setRecordId(slugId)
                    .setIssuer("managed-search-provider")
                    .setKeyId("provider-key-1")
                    .setIssuedAt(VERIFIED_TIME)
                    .setSubject(RecordSubject.newBuilder()
                            .setKind("workflow-run")
                            .setWorkflowName("managed-search-publication")
                            .setWorkflowFingerprint(ctx.intentSha256())
                            .setRunId(slugId))
                    .setCompleteness(Completeness.newBuilder()
                            .setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE)
                            .setPolicyId("external-publication-receipt")
                            .setPolicyVersion("1.0")
                            .setPolicySha256("66".repeat(32)))
                    .build();
            SignedWorkRecord signed = receiptSigner.sign(manifest);
            ByteString signedBytes = signed.toByteString();
            String receiptSha = ExternalOperationKeys.sha256Hex(signedBytes.toByteArray());

            return ExternalProviderEvidence.newBuilder()
                    .setProvider(ctx.auth().binding().getProvider())
                    .setContractVersion(ctx.auth().binding().getContractVersion())
                    .setVerifiedServiceProfileId(ctx.auth().serviceProfile().getName())
                    .setVerifiedServiceEndpointId(ctx.auth().serviceEndpoint().getName())
                    .setCredentialBindingSha256(ctx.auth().credentialBindingSha256())
                    .setExternalObjectId(ctx.intent().getExternalObjectId())
                    .setRequestIntentSha256(ctx.intentSha256())
                    .setReceiptSha256(receiptSha)
                    .setProviderReceiptId(slugId)
                    .setSignedWorkRecordBytes(signedBytes)
                    .setVerifiedAt(VERIFIED_TIME);
        }
    }
}
