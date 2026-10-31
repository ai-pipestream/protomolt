package ai.protomolt.proto.repo.service.eval;

import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingPolicies;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.protomolt.proto.kafka.serde.ProtoMoltSerdeConfig;
import ai.protomolt.proto.repo.archive.ArchiveCoordinates;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.KafkaOutboxPublisher;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.UploadHttpServer;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.GetDriveRequest;
import ai.protomolt.proto.repo.v1.ListDrivesRequest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.PurgeDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionSummary;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.StreamedBody;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The release evaluation of the repository host. The production entry point runs in
 * its own JVM from environment variables against Postgres, RustFS and a Kafka-API
 * broker, with its lifecycle loops on, and one document and one archive entry are
 * followed through their whole life over real gRPC and HTTP: save with blobs, whole
 * and selective and streamed reads, a second revision with copy-forward and replay,
 * the streaming and HTTP upload doors, catalog listings, the tombstone, the purge
 * refusal, the archive's three doors in and stream out, the exact event sequence on
 * the topic, a scoped caller's account boundary, and a SIGTERM restart on the same
 * stores. The steps are ordered and build on each other; the first failure is the
 * finding.
 *
 * <p>Runs only through {@code :protomolt-repo-service:repoEval}.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RepoServiceEndToEndEval {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final RustFsContainer RUSTFS = new RustFsContainer();

    // Same broker image as the service's own Kafka lane.
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static final String OPERATOR = "eval-operator-token-" + UUID.randomUUID();
    private static final String READER_CREDENTIAL = "eval-reader-credential-" + UUID.randomUUID();
    private static final String READER_UPN = "reader@eval.example";
    private static final String ACCOUNT = "eval-account";
    private static final String OTHER_ACCOUNT = "eval-other-account";
    private static final String DATASOURCE = "ds-eval";
    private static final String INTAKE_DRIVE = "intake";
    private static final String ARCHIVE_DRIVE = "archive-drive";
    private static final String ARCHIVE = "records";
    private static final String ENTRY = "entry-1";
    private static final String TOPIC = "document-events-eval";

    private static final String DOC = "doc-life";
    private static final String DOC_STREAMED = "doc-streamed";
    private static final String DOC_HTTP = "doc-http";
    private static final String DOC_AFTER_RESTART = "doc-after-restart";

    private static final byte[] INLINE_BLOB = "inline blob bytes kept beside the core part".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STREAMED_BLOB = pseudoRandom(300 * 1024 + 17, 11L);
    private static final byte[] HTTP_BLOB = pseudoRandom(64 * 1024 + 3, 23L);
    private static final byte[] ENTRY_ORIGINAL = "the archived original text".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ENTRY_STREAMED = pseudoRandom(200 * 1024 + 5, 37L);
    private static final byte[] ENTRY_HTTP = pseudoRandom(2 * 1024 * 1024 + 321, 41L);

    @TempDir
    static Path directory;

    static Map<String, String> environment;
    static RepoHost host;
    static HttpClient http;
    static S3Client s3;

    // Facts the later steps check against.
    static SaveDocumentResponse firstSave;
    static DocumentManifest firstManifest;
    static DocumentManifest secondManifest;
    static long tombstoneRevision;
    static EntryInfo entryInfo;

    @BeforeAll
    static void boot() throws Exception {
        http = HttpClient.newHttpClient();
        s3 = S3Client.builder()
                .endpointOverride(RUSTFS.endpoint())
                .region(Region.of(RUSTFS.region()))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(RUSTFS.accessKey(), RUSTFS.secretKey())))
                .build();
        environment = environment();
        host = RepoHost.launch(environment, directory.resolve("repo-service-1.log"));
    }

    @AfterAll
    static void shutdown() throws Exception {
        if (host != null) host.close();
        if (s3 != null) s3.close();
    }

    /** The deployment's environment: every value a compose file would set, nothing injected. */
    private static Map<String, String> environment() throws IOException {
        var policy = AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder().setName("reader")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(READER_CREDENTIAL))
                        .addScopes("search-query"))
                .build();
        var bindings = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("reader").addAccountIds(ACCOUNT)
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity(READER_UPN)))
                .build();
        Path policyFile = directory.resolve("access-policy.binpb");
        Path bindingsFile = directory.resolve("account-bindings.binpb");
        Files.write(policyFile, policy.toByteArray());
        Files.write(bindingsFile, bindings.toByteArray());

        var env = new HashMap<String, String>();
        env.put("PROTOMOLT_API_TOKEN", OPERATOR);
        env.put("PROTOMOLT_ACCESS_POLICY", policyFile.toString());
        env.put(AccountBindingPolicies.ENV_ACCOUNT_BINDINGS, bindingsFile.toString());
        env.put(RepoServiceConfig.ENV_GRPC_PORT, "0");
        env.put(RepoServiceConfig.ENV_HTTP_PORT, Integer.toString(freePort()));
        env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
        env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
        env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
        env.put(RepoServiceConfig.ENV_S3_ENDPOINT, RUSTFS.endpoint().toString());
        env.put(RepoServiceConfig.ENV_S3_REGION, RUSTFS.region());
        env.put(RepoServiceConfig.ENV_S3_ACCESS_KEY, RUSTFS.accessKey());
        env.put(RepoServiceConfig.ENV_S3_SECRET_KEY, RUSTFS.secretKey());
        env.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "eval-documents");
        env.put(RepoServiceConfig.ENV_SEED_ACCOUNT_ID, ACCOUNT);
        env.put(RepoServiceConfig.ENV_LIFECYCLE_ENABLED, "true");
        // The relay's idle back-off; short so the topic reflects a commit within a second.
        env.put(RepoServiceConfig.ENV_PURGE_INTERVAL_MS, "250");
        env.put(RepoServiceConfig.ENV_KAFKA_BOOTSTRAP_SERVERS, REDPANDA.getBootstrapServers());
        env.put(RepoServiceConfig.ENV_KAFKA_TOPIC, TOPIC);
        return env;
    }

    // ------------------------------------------------------------------
    // 1. Boot
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void theHostBootsFromItsEnvironmentAndRefusesTheUnauthenticated() {
        assertThat(as(OPERATOR, HealthGrpc.newBlockingStub(host.channel))
                .check(HealthCheckRequest.getDefaultInstance()).getStatus())
                .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
        assertCode(() -> DriveServiceGrpc.newBlockingStub(host.channel)
                .listDrives(ListDrivesRequest.newBuilder().setAccountId(ACCOUNT).build()), Status.Code.UNAUTHENTICATED);
        assertCode(() -> as("not-a-credential", DocumentServiceGrpc.newBlockingStub(host.channel))
                .listDocuments(list(ACCOUNT)), Status.Code.UNAUTHENTICATED);

        // The seeded account's drives exist before the first request.
        var drives = as(OPERATOR, DriveServiceGrpc.newBlockingStub(host.channel))
                .listDrives(ListDrivesRequest.newBuilder().setAccountId(ACCOUNT).build()).getDrivesList();
        assertThat(drives).extracting(Drive::getName).contains(INTAKE_DRIVE, "pipeline");
        assertThat(drives).allSatisfy(drive -> assertThat(drive.getProvider()).isEqualTo("s3"));
    }

    // ------------------------------------------------------------------
    // 2. Save and read
    // ------------------------------------------------------------------

    @Test
    @Order(2)
    void aDocumentWithBlobsIsSavedAndReadsBackWholeSelectivelyAndStreamed() throws Exception {
        var documents = operatorDocuments();
        var document = document(DOC, "first title")
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("inline").setMimeType("text/plain").setFilename("inline.txt")
                        .setSizeBytes(INLINE_BLOB.length).setData(ByteString.copyFrom(INLINE_BLOB))))
                .build();
        var request = save("op-" + DOC + "-1", address(DOC), document, ExpectedRevision.newBuilder().setNone(true).build(),
                DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS);
        firstSave = documents.saveDocument(request);
        assertThat(firstSave.getReplayed()).isFalse();
        assertThat(firstSave.getAccession().getRevision()).isEqualTo(1);
        firstManifest = firstSave.getAccession().getManifest();
        assertThat(firstManifest.getPartsList()).extracting(entry -> entry.getSelector().getPart())
                .containsExactlyInAnyOrder(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS);

        // Whole read: the blob comes back as a claim check, never as inline bytes.
        GetDocumentResponse whole = documents.getDocument(get(DOC));
        assertThat(whole.getManifest().getRevision()).isEqualTo(1);
        assertThat(whole.getDocument().getSearchMetadata().getTitle()).isEqualTo("first title");
        assertThat(whole.getDocument().getBlobBag().getBlob().hasStorageRef()).isTrue();
        assertThat(whole.getDocument().getBlobBag().getBlob().getData().isEmpty()).isTrue();

        // Selective read: CORE alone carries no blob bag at all.
        GetDocumentResponse coreOnly = documents.getDocument(get(DOC).toBuilder()
                .setSelection(select(DocumentPart.DOCUMENT_PART_CORE)).build());
        assertThat(coreOnly.getDocument().hasBlobBag()).isFalse();
        assertThat(coreOnly.getDocument().getSearchMetadata().getTitle()).isEqualTo("first title");

        // Streamed read: manifest, then every selected part in full, then completion.
        var frames = new ArrayList<ReadDocumentResponse>();
        documents.readDocument(ReadDocumentRequest.newBuilder().setAddress(address(DOC)).build())
                .forEachRemaining(frames::add);
        assertThat(frames.getFirst().hasManifest()).isTrue();
        assertThat(frames.getLast().hasComplete()).isTrue();
        Map<DocumentPart, ByteString> parts = reassemble(frames);
        for (PartManifestEntry entry : frames.getFirst().getManifest().getPartsList()) {
            ByteString bytes = parts.get(entry.getSelector().getPart());
            assertThat(bytes).as("part %s was streamed", entry.getSelector().getPart()).isNotNull();
            assertThat(bytes.size()).isEqualTo(entry.getSizeBytes());
            assertThat(sha256(bytes.toByteArray())).isEqualTo(entry.getSha256());
        }

        // The blob's bytes come back exactly over ReadBlob.
        assertThat(readBlob(documents, DOC, "inline")).isEqualTo(INLINE_BLOB);

        // And they are objects in the store the drive names.
        Drive intake = as(OPERATOR, DriveServiceGrpc.newBlockingStub(host.channel)).getDrive(GetDriveRequest.newBuilder()
                .setAccountId(ACCOUNT).setName(INTAKE_DRIVE).build()).getDrive();
        assertThat(intake.getBucket()).isNotBlank();
        List<S3Object> objects = s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(intake.getBucket()).prefix(prefixOf(intake)).build())
                .contents().stream().toList();
        assertThat(objects).as("the drive's objects in RustFS").isNotEmpty();
    }

    @Test
    @Order(3)
    void aSecondRevisionCopiesUntouchedPartsForwardAndReplaysItsReceipt() {
        var documents = operatorDocuments();
        var request = save("op-" + DOC + "-2", address(DOC), document(DOC, "second title").build(),
                ExpectedRevision.newBuilder().setRevision(1).build(), DocumentPart.DOCUMENT_PART_CORE);
        SaveDocumentResponse second = documents.saveDocument(request);
        assertThat(second.getAccession().getRevision()).isEqualTo(2);
        secondManifest = second.getAccession().getManifest();

        PartManifestEntry blobsBefore = part(firstManifest, DocumentPart.DOCUMENT_PART_BLOBS);
        PartManifestEntry blobsAfter = part(secondManifest, DocumentPart.DOCUMENT_PART_BLOBS);
        assertThat(blobsAfter.getWrittenInRevision()).as("BLOBS was copied forward, not rewritten").isEqualTo(1);
        assertThat(blobsAfter.getSha256()).isEqualTo(blobsBefore.getSha256());
        assertThat(part(secondManifest, DocumentPart.DOCUMENT_PART_CORE).getWrittenInRevision()).isEqualTo(2);

        GetDocumentResponse head = documents.getDocument(get(DOC));
        assertThat(head.getDocument().getSearchMetadata().getTitle()).isEqualTo("second title");
        assertThat(head.getDocument().getBlobBag().getBlob().getBlobId()).isEqualTo("inline");
        assertThat(readBlob(documents, DOC, "inline")).isEqualTo(INLINE_BLOB);

        // The same operation again is the same receipt, marked replayed, and no third revision.
        SaveDocumentResponse replay = documents.saveDocument(request);
        assertThat(replay.getReplayed()).isTrue();
        assertThat(replay.getAccession()).isEqualTo(second.getAccession());
        assertThat(documents.getManifest(GetManifestRequest.newBuilder().setAddress(address(DOC)).build())
                .getManifest().getRevision()).isEqualTo(2);

        // A stale expected revision is a conflict, not a fourth write.
        assertCode(() -> documents.saveDocument(save("op-" + DOC + "-stale", address(DOC),
                document(DOC, "stale").build(), ExpectedRevision.newBuilder().setRevision(1).build(),
                DocumentPart.DOCUMENT_PART_CORE)), Status.Code.ABORTED);
    }

    // ------------------------------------------------------------------
    // 3. The other two doors in
    // ------------------------------------------------------------------

    @Test
    @Order(4)
    void theStreamingDoorAndTheHttpDoorBothLandBlobsThatReadBack() throws Exception {
        var documents = operatorDocuments();

        // gRPC client stream: header with the save, then data frames.
        var streamedDocument = document(DOC_STREAMED, "streamed")
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("payload").setMimeType("application/pdf").setFilename("report.pdf")
                        .setSizeBytes(STREAMED_BLOB.length)))
                .build();
        var header = UploadHeader.newBuilder()
                .setStreamed(StreamedBody.getDefaultInstance())
                .setSave(save("op-" + DOC_STREAMED, address(DOC_STREAMED), streamedDocument,
                        ExpectedRevision.newBuilder().setNone(true).build(),
                        DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS))
                .setBlobId("payload")
                .setDeclaredLength(STREAMED_BLOB.length)
                .setExpectedSha256(sha256(STREAMED_BLOB))
                .build();
        UploadDocumentResponse streamed = upload(as(OPERATOR, DocumentServiceGrpc.newStub(host.channel)), header, STREAMED_BLOB);
        assertThat(streamed.hasSaved()).as("%s", streamed).isTrue();
        assertThat(streamed.getSaved().getAccession().getRevision()).isEqualTo(1);
        assertThat(readBlob(documents, DOC_STREAMED, "payload")).isEqualTo(STREAMED_BLOB);

        // HTTP POST with the protobuf answer.
        String sha256 = sha256(HTTP_BLOB);
        HttpResponse<byte[]> posted = http.send(HttpRequest.newBuilder(URI.create(httpBase()
                        + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + INTAKE_DRIVE + "&filename=posted.bin&doc_id=" + DOC_HTTP
                        + "&blob_id=posted&operation_id=op-" + DOC_HTTP))
                .header("api_token", OPERATOR)
                .header("Accept", "application/proto")
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(HTTP_BLOB)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(posted.statusCode()).as(new String(posted.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        UploadDocumentResponse answer = UploadDocumentResponse.parseFrom(posted.body());
        assertThat(answer.hasSaved()).as("%s", answer).isTrue();
        assertThat(answer.getSaved().getAccession().getRevision()).isEqualTo(1);
        assertThat(readBlob(documents, DOC_HTTP, "posted")).isEqualTo(HTTP_BLOB);

        // The same POST again is the replay of the same revision, not a second one.
        HttpResponse<byte[]> again = http.send(HttpRequest.newBuilder(URI.create(httpBase()
                        + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                        + "&drive=" + INTAKE_DRIVE + "&filename=posted.bin&doc_id=" + DOC_HTTP
                        + "&blob_id=posted&operation_id=op-" + DOC_HTTP))
                .header("api_token", OPERATOR)
                .header("Accept", "application/proto")
                .header("X-Content-Sha256", sha256)
                .POST(HttpRequest.BodyPublishers.ofByteArray(HTTP_BLOB)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(again.statusCode()).isEqualTo(200);
        UploadDocumentResponse replayed = UploadDocumentResponse.parseFrom(again.body());
        assertThat(replayed.hasReplayed() || (replayed.hasSaved() && replayed.getSaved().getReplayed()))
                .as("%s", replayed).isTrue();
        assertThat(documents.getManifest(GetManifestRequest.newBuilder().setAddress(address(DOC_HTTP)).build())
                .getManifest().getRevision()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // 4. The catalog
    // ------------------------------------------------------------------

    @Test
    @Order(5)
    void theCatalogListsPagesFiltersAndRecordsOutcomes() {
        var documents = operatorDocuments();

        // Three documents, paged two at a time, with the lookup projection filled.
        List<DocumentSummary> all = page(documents, list(ACCOUNT).toBuilder().setPageSize(2).build());
        assertThat(all).extracting(summary -> summary.getAddress().getDocId())
                .containsExactlyInAnyOrder(DOC, DOC_STREAMED, DOC_HTTP);
        DocumentSummary life = all.stream().filter(s -> s.getAddress().getDocId().equals(DOC)).findFirst().orElseThrow();
        assertThat(life.getRevision()).isEqualTo(2);
        assertThat(life.getTombstone()).isFalse();
        assertThat(life.getLookup().getTitle()).isEqualTo("second title");
        assertThat(life.getLookup().getDatasourceId()).isEqualTo(DATASOURCE);

        // A filter is answered by the catalog.
        ListDocumentsResponse filtered = documents.listDocuments(list(ACCOUNT).toBuilder()
                .setFilter(DocumentFilter.newBuilder().setTitle("streamed")).build());
        assertThat(filtered.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId())
                .containsExactly(DOC_STREAMED);
        assertThat(documents.listDocuments(list(OTHER_ACCOUNT)).getDocumentsList()).isEmpty();

        // Revisions and outcomes agree with the receipts.
        List<RevisionSummary> revisions = documents.listRevisions(ListRevisionsRequest.newBuilder()
                .setAddress(address(DOC)).setPageSize(10).build()).getRevisionsList();
        assertThat(revisions).extracting(RevisionSummary::getRevision).containsExactlyInAnyOrder(1L, 2L);
        assertThat(revisions).extracting(RevisionSummary::getOperationId)
                .containsExactlyInAnyOrder("op-" + DOC + "-1", "op-" + DOC + "-2");
        assertThat(documents.getManifest(GetManifestRequest.newBuilder().setAddress(address(DOC))
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getManifest())
                .isEqualTo(firstManifest);
        assertThat(documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(ACCOUNT).setOperationId("op-" + DOC + "-1").build()).getCommitted().getAccession())
                .isEqualTo(firstSave.getAccession());
        assertThat(documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(ACCOUNT).setOperationId("op-never-ran").build()).hasNone()).isTrue();
    }

    // ------------------------------------------------------------------
    // 5. Tombstone and purge
    // ------------------------------------------------------------------

    @Test
    @Order(6)
    void deletingLeavesATombstoneThatHidesTheHeadAndKeepsHistory() {
        var documents = operatorDocuments();
        var delete = DeleteDocumentRequest.newBuilder().setOperationId("op-" + DOC + "-delete")
                .setAddress(address(DOC)).setExpected(ExpectedRevision.newBuilder().setRevision(2))
                .setReason("retired by the evaluation").build();
        var deleted = documents.deleteDocument(delete);
        tombstoneRevision = deleted.getAccession().getRevision();
        assertThat(tombstoneRevision).isEqualTo(3);
        assertThat(deleted.getAccession().getManifest().getTombstone()).isTrue();

        assertCode(() -> documents.getDocument(get(DOC)), Status.Code.NOT_FOUND);
        GetDocumentResponse history = documents.getDocument(get(DOC).toBuilder()
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build());
        assertThat(history.getDocument().getSearchMetadata().getTitle()).isEqualTo("first title");
        assertThat(readBlob(documents, DOC, "inline", 1)).isEqualTo(INLINE_BLOB);

        assertThat(page(documents, list(ACCOUNT))).extracting(summary -> summary.getAddress().getDocId())
                .containsExactlyInAnyOrder(DOC_STREAMED, DOC_HTTP);
        List<DocumentSummary> withTombstones = page(documents, list(ACCOUNT).toBuilder().setIncludeTombstoned(true).build());
        assertThat(withTombstones.stream().filter(s -> s.getAddress().getDocId().equals(DOC)).findFirst().orElseThrow()
                .getTombstone()).isTrue();
        List<RevisionSummary> revisions = documents.listRevisions(ListRevisionsRequest.newBuilder()
                .setAddress(address(DOC)).setPageSize(10).build()).getRevisionsList();
        assertThat(revisions).hasSize(3);
        assertThat(revisions.stream().filter(RevisionSummary::getTombstone)).hasSize(1);

        // The same delete replays; a second delete on top of the tombstone is a conflict.
        assertThat(documents.deleteDocument(delete).getReplayed()).isTrue();
        assertCode(() -> documents.deleteDocument(delete.toBuilder().setOperationId("op-" + DOC + "-delete-2")
                .setExpected(ExpectedRevision.newBuilder().setRevision(3)).build()), Status.Code.ABORTED);
    }

    @Test
    @Order(7)
    void purgeThroughTheServedHostIsRefusedByItsFixedDecision() {
        // RepoV2Wiring hands DocumentPurges a decision that grants nobody; the host
        // therefore refuses every purge. This pins that behaviour so a change shows up.
        var documents = operatorDocuments();
        assertCode(() -> documents.purgeDocument(PurgeDocumentRequest.newBuilder()
                .setPurgeId("purge-" + DOC).setAddress(address(DOC)).setReason("evaluation").build()),
                Status.Code.PERMISSION_DENIED);
        assertThat(documents.listRevisions(ListRevisionsRequest.newBuilder()
                .setAddress(address(DOC)).setPageSize(10).build()).getRevisionsCount()).isEqualTo(3);
    }

    // ------------------------------------------------------------------
    // 6. The archive over v2
    // ------------------------------------------------------------------

    @Test
    @Order(8)
    void theArchiveTakesAnEntryThroughAllThreeDoorsAndStreamsItBack() throws Exception {
        var drives = as(OPERATOR, DriveServiceGrpc.newBlockingStub(host.channel));
        var archives = as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(host.channel));
        drives.createDrive(CreateDriveRequest.newBuilder().setName(ARCHIVE_DRIVE).setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_CUSTOM).build());
        archives.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setName(ARCHIVE).setAccountId(ACCOUNT).setDriveName(ARCHIVE_DRIVE)
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());

        // Door one: unary PutEntry.
        var put = archives.putEntry(PutEntryRequest.newBuilder().setAddress(entry()).setTitle("Records entry")
                .setFilename("original.txt").setContentType("text/plain")
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain"))
                        .setData(ByteString.copyFrom(ENTRY_ORIGINAL)))
                .setOperationId("archive-put-1").build());
        assertThat(put.getVersion()).isEqualTo(1);
        assertThat(put.getReplayed()).isFalse();

        // Door two: the gRPC client stream, chunked smaller than the body.
        UploadRenditionResponse streamed = uploadRendition(as(OPERATOR, ArchiveServiceGrpc.newStub(host.channel)),
                "streamed", "application/pdf", ENTRY_STREAMED);
        assertThat(streamed.getVersion()).isEqualTo(2);
        assertThat(streamed.getSha256()).isEqualTo(sha256(ENTRY_STREAMED));

        // Door three: HTTP POST on the archive route, a multi-MiB body, protobuf receipt.
        String httpSha = sha256(ENTRY_HTTP);
        HttpResponse<byte[]> posted = http.send(HttpRequest.newBuilder(URI.create(httpBase()
                        + UploadHttpServer.ARCHIVE_UPLOAD_PATH + "?account_id=" + ACCOUNT + "&archive=" + ARCHIVE
                        + "&entry_id=" + ENTRY + "&rendition=scan"))
                .header("api_token", OPERATOR)
                .header("Content-Type", "image/tiff")
                .header("X-Content-Sha256", httpSha)
                .POST(HttpRequest.BodyPublishers.ofByteArray(ENTRY_HTTP)).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(posted.statusCode()).as(new String(posted.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        UploadRenditionResponse viaHttp = UploadRenditionResponse.parseFrom(posted.body());
        assertThat(viaHttp.getVersion()).isEqualTo(3);
        assertThat(viaHttp.getSha256()).isEqualTo(httpSha);
        assertThat(viaHttp.getReplayed()).isFalse();

        // Out: the unary read carries every rendition's bytes at the head version.
        var entry = archives.getEntry(GetEntryRequest.newBuilder().setAddress(entry()).build());
        assertThat(entry.getInfo().getCurrentVersion()).isEqualTo(3);
        assertThat(entry.getInfo().getTitle()).isEqualTo("Records entry");
        entryInfo = entry.getInfo();
        assertThat(entry.getRenditionsList()).extracting(r -> r.getRendition().getName())
                .containsExactlyInAnyOrder("original", "streamed", "scan");
        assertThat(renditionData(entry.getRenditionsList(), "original")).isEqualTo(ENTRY_ORIGINAL);
        assertThat(renditionData(entry.getRenditionsList(), "streamed")).isEqualTo(ENTRY_STREAMED);
        assertThat(renditionData(entry.getRenditionsList(), "scan")).isEqualTo(ENTRY_HTTP);

        // Out: the streamed read of the large rendition, header then data.
        assertThat(readRendition(archives, "scan")).isEqualTo(ENTRY_HTTP);
        assertThat(readRendition(archives, "streamed")).isEqualTo(ENTRY_STREAMED);

        // The manifest, the versions, the listing and the counters agree.
        var manifest = archives.getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(entry()).build());
        assertThat(manifest.getManifest().getVersion()).isEqualTo(3);
        assertThat(manifest.getManifest().getRenditionsList()).extracting(RenditionManifestEntry::getSizeBytes)
                .containsExactlyInAnyOrder((long) ENTRY_ORIGINAL.length, (long) ENTRY_STREAMED.length, (long) ENTRY_HTTP.length);
        var versions = archives.listVersions(ListVersionsRequest.newBuilder().setAddress(entry()).setLimit(10).build());
        // The listing answers with summaries; the manifests come from GetEntryManifest.
        assertThat(versions.getSummariesList()).extracting(v -> v.getVersion()).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(versions.getSummariesList()).allSatisfy(v -> assertThat(v.getDeleted()).isFalse());
        var entries = archives.listEntries(ListEntriesRequest.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).build());
        assertThat(entries.getTotalCount()).isEqualTo(1);
        assertThat(entries.getEntriesList()).extracting(info -> info.getAddress().getEntryId()).containsExactly(ENTRY);
        var stats = archives.getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId(ACCOUNT).setArchive(ARCHIVE).build()).getStats();
        assertThat(stats.getEntries()).isEqualTo(1);
        assertThat(stats.getVersions()).isEqualTo(3);
        assertThat(stats.getCurrentBytes()).isEqualTo(ENTRY_ORIGINAL.length + ENTRY_STREAMED.length + ENTRY_HTTP.length);
    }

    // ------------------------------------------------------------------
    // 7. The events
    // ------------------------------------------------------------------

    @Test
    @Order(9)
    void theTopicCarriesExactlyTheCommittedRevisionsInOrderPerAddress() {
        Map<String, List<DocumentEvent>> byKey = consumeByKey(Duration.ofSeconds(60));

        // The document's life: two accessions, then the tombstone, keyed by its canonical address.
        List<DocumentEvent> life = byKey.get(CatalogRecords.eventKey(address(DOC)));
        assertThat(life).as("events for %s in %s", DOC, byKey.keySet()).isNotNull();
        assertThat(life).extracting(DocumentEvent::getRevision).containsExactly(1L, 2L, 3L);
        assertThat(life).extracting(DocumentEvent::getPreviousRevision).containsExactly(0L, 1L, 2L);
        assertThat(life.get(0).hasAccessioned()).isTrue();
        assertThat(life.get(1).hasAccessioned()).isTrue();
        assertThat(life.get(2).hasTombstoned()).isTrue();
        assertThat(life.get(2).getTombstoned().getReason()).isEqualTo("retired by the evaluation");
        assertThat(life).extracting(DocumentEvent::getOperationId)
                .containsExactly("op-" + DOC + "-1", "op-" + DOC + "-2", "op-" + DOC + "-delete");
        assertThat(life).extracting(DocumentEvent::getEventId).doesNotHaveDuplicates();

        // The other two doors each committed one revision, and the replays added none.
        assertThat(byKey.get(CatalogRecords.eventKey(address(DOC_STREAMED))))
                .extracting(DocumentEvent::getRevision).containsExactly(1L);
        assertThat(byKey.get(CatalogRecords.eventKey(address(DOC_HTTP))))
                .extracting(DocumentEvent::getRevision).containsExactly(1L);

        // The archive entry is a v2 document too: three accessions, one per rendition landed.
        List<DocumentEvent> archived = byKey.get(CatalogRecords.eventKey(ArchiveCoordinates.address(entry())));
        assertThat(archived).as("archive entry events in %s", byKey.keySet()).isNotNull();
        assertThat(archived).extracting(DocumentEvent::getRevision).containsExactly(1L, 2L, 3L);
        assertThat(archived).allSatisfy(event -> assertThat(event.hasAccessioned()).isTrue());

        // No purge happened, so no purge event exists; and nothing left the account.
        for (var events : byKey.values()) {
            for (DocumentEvent event : events) {
                assertThat(event.hasPurgeMarked() || event.hasPurgeCompleted()).as("%s", event).isFalse();
                assertThat(event.getAddress().getAccountId()).isEqualTo(ACCOUNT);
            }
        }
    }

    // ------------------------------------------------------------------
    // 8. A scoped caller
    // ------------------------------------------------------------------

    @Test
    @Order(10)
    void aBoundReaderSeesOnlyWhatItsAccountAndAclAllow() {
        var reader = as(READER_CREDENTIAL, DocumentServiceGrpc.newBlockingStub(host.channel));
        // The saved documents carry a READ rule for the reader's identity; the one the HTTP
        // door created carries no rule, so the same account hides it from the reader.
        assertThat(page(reader, list(ACCOUNT))).extracting(summary -> summary.getAddress().getDocId())
                .containsExactly(DOC_STREAMED);
        assertThat(reader.getDocument(get(DOC_STREAMED)).getDocument().getSearchMetadata().getTitle()).isEqualTo("streamed");
        assertThat(readBlob(reader, DOC_STREAMED, "payload")).isEqualTo(STREAMED_BLOB);
        assertCode(() -> reader.getDocument(get(DOC_HTTP)), Status.Code.PERMISSION_DENIED);
        assertCode(() -> reader.listDocuments(list(OTHER_ACCOUNT)), Status.Code.PERMISSION_DENIED);
        assertCode(() -> reader.saveDocument(save("op-reader-write", address("doc-by-reader"),
                document("doc-by-reader", "no grant").build(), ExpectedRevision.newBuilder().setNone(true).build(),
                DocumentPart.DOCUMENT_PART_CORE)), Status.Code.PERMISSION_DENIED);
    }

    // ------------------------------------------------------------------
    // 9. Restart
    // ------------------------------------------------------------------

    @Test
    @Order(11)
    void aSigtermRestartOnTheSameStoresKeepsEverythingAndKeepsRelaying() throws Exception {
        host.close();
        host = RepoHost.launch(environment, directory.resolve("repo-service-2.log"));
        var documents = operatorDocuments();
        var archives = as(OPERATOR, ArchiveServiceGrpc.newBlockingStub(host.channel));

        assertCode(() -> documents.getDocument(get(DOC)), Status.Code.NOT_FOUND);
        assertThat(documents.getManifest(GetManifestRequest.newBuilder().setAddress(address(DOC))
                .setRevision(RevisionSelector.newBuilder().setRevision(2)).build()).getManifest()).isEqualTo(secondManifest);
        assertThat(documents.getDocument(get(DOC).toBuilder()
                .setRevision(RevisionSelector.newBuilder().setRevision(2)).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("second title");
        assertThat(readBlob(documents, DOC, "inline", 1)).isEqualTo(INLINE_BLOB);
        assertThat(readBlob(documents, DOC_STREAMED, "payload")).isEqualTo(STREAMED_BLOB);
        assertThat(readBlob(documents, DOC_HTTP, "posted")).isEqualTo(HTTP_BLOB);
        assertThat(documents.listRevisions(ListRevisionsRequest.newBuilder().setAddress(address(DOC)).setPageSize(10).build())
                .getRevisionsCount()).isEqualTo(3);
        var entry = archives.getEntry(GetEntryRequest.newBuilder().setAddress(entry()).build());
        assertThat(entry.getInfo()).isEqualTo(entryInfo);
        assertThat(renditionData(entry.getRenditionsList(), "scan")).isEqualTo(ENTRY_HTTP);

        // The outbox was drained before the stop: nothing waits for the relay.
        assertThat(pendingOutboxEvents()).isZero();

        // The restarted host's relay carries a new commit to the topic.
        documents.saveDocument(save("op-" + DOC_AFTER_RESTART, address(DOC_AFTER_RESTART),
                document(DOC_AFTER_RESTART, "after restart").build(),
                ExpectedRevision.newBuilder().setNone(true).build(), DocumentPart.DOCUMENT_PART_CORE));
        Map<String, List<DocumentEvent>> byKey = consumeByKey(Duration.ofSeconds(60));
        assertThat(byKey.get(CatalogRecords.eventKey(address(DOC_AFTER_RESTART))))
                .extracting(DocumentEvent::getOperationId).containsExactly("op-" + DOC_AFTER_RESTART);
        assertThat(pendingOutboxEvents()).isZero();
    }

    // ------------------------------------------------------------------
    // Request shapes
    // ------------------------------------------------------------------

    private static DocumentServiceGrpc.DocumentServiceBlockingStub operatorDocuments() {
        return as(OPERATOR, DocumentServiceGrpc.newBlockingStub(host.channel));
    }

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                .withDeadlineAfter(60, TimeUnit.SECONDS);
    }

    private static String httpBase() {
        return "http://127.0.0.1:" + host.httpPort;
    }

    private static NodeAddress address(String doc) {
        return NodeAddress.newBuilder().setAccountId(ACCOUNT).setDocId(doc)
                .setGraphId("intake:" + ACCOUNT).setGraphAddressId(DATASOURCE).build();
    }

    private static EntryAddress entry() {
        return EntryAddress.newBuilder().setAccountId(ACCOUNT).setArchive(ARCHIVE).setEntryId(ENTRY).build();
    }

    private static Document.Builder document(String doc, String title) {
        return Document.newBuilder().setDocId(doc)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId(DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity(READER_UPN)
                                .setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(title).setBody("body of " + doc));
    }

    private static SaveDocumentRequest save(String operation, NodeAddress address, Document document,
            ExpectedRevision expected, DocumentPart... parts) {
        var request = SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(expected)
                .setDocument(document)
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)));
        for (DocumentPart part : parts) {
            request.addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                    .setAction(WriteAction.WRITE_ACTION_PUT));
        }
        return request.build();
    }

    private static GetDocumentRequest get(String doc) {
        return GetDocumentRequest.newBuilder().setAddress(address(doc)).build();
    }

    private static PartSelection select(DocumentPart... parts) {
        var selection = PartSelection.newBuilder();
        for (DocumentPart part : parts) selection.addSelectors(PartSelector.newBuilder().setPart(part));
        return selection.build();
    }

    private static ListDocumentsRequest list(String account) {
        return ListDocumentsRequest.newBuilder().setAccountId(account).setPageSize(100).build();
    }

    private static PartManifestEntry part(DocumentManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(entry -> entry.getSelector().getPart() == part)
                .findFirst().orElseThrow(() -> new AssertionError(part + " missing from " + manifest));
    }

    private static List<DocumentSummary> page(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            ListDocumentsRequest first) {
        var all = new ArrayList<DocumentSummary>();
        var request = first;
        while (true) {
            ListDocumentsResponse page = documents.listDocuments(request);
            all.addAll(page.getDocumentsList());
            if (page.getNextPageToken().isEmpty()) return all;
            request = request.toBuilder().setPageToken(page.getNextPageToken()).build();
        }
    }

    private static byte[] readBlob(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String doc, String blobId) {
        return readBlob(documents, doc, blobId, 0);
    }

    private static byte[] readBlob(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String doc, String blobId,
            long revision) {
        var request = ReadBlobRequest.newBuilder().setAddress(address(doc)).setBlobId(blobId);
        if (revision > 0) request.setRevision(RevisionSelector.newBuilder().setRevision(revision));
        var out = new ByteArrayOutputStream();
        long declared = -1;
        var frames = documents.readBlob(request.build());
        while (frames.hasNext()) {
            ReadBlobResponse frame = frames.next();
            if (frame.hasHeader()) {
                declared = frame.getHeader().getTotalLength();
                assertThat(frame.getHeader().getRevision()).isPositive();
            } else {
                out.writeBytes(frame.getData().toByteArray());
            }
        }
        assertThat(out.size()).as("blob %s/%s length matches its header", doc, blobId).isEqualTo(declared);
        return out.toByteArray();
    }

    private static Map<DocumentPart, ByteString> reassemble(List<ReadDocumentResponse> frames) {
        var parts = new LinkedHashMap<DocumentPart, ByteString>();
        for (ReadDocumentResponse frame : frames) {
            if (!frame.hasPartData()) continue;
            DocumentPart part = frame.getPartData().getSelector().getPart();
            ByteString sofar = parts.getOrDefault(part, ByteString.EMPTY);
            assertThat(frame.getPartData().getOffset()).as("frames arrive in order").isEqualTo(sofar.size());
            parts.put(part, sofar.concat(frame.getPartData().getData()));
        }
        return parts;
    }

    private static UploadDocumentResponse upload(DocumentServiceGrpc.DocumentServiceStub stub, UploadHeader header,
            byte[] body) throws Exception {
        var done = new CompletableFuture<UploadDocumentResponse>();
        StreamObserver<UploadDocumentRequest> requests = stub.uploadDocument(new StreamObserver<>() {
            private UploadDocumentResponse last;
            @Override public void onNext(UploadDocumentResponse value) { last = value; }
            @Override public void onError(Throwable t) { done.completeExceptionally(t); }
            @Override public void onCompleted() { done.complete(last); }
        });
        requests.onNext(UploadDocumentRequest.newBuilder().setHeader(header).build());
        for (int at = 0; at < body.length && !done.isDone(); at += 48 * 1024) {
            requests.onNext(UploadDocumentRequest.newBuilder()
                    .setData(ByteString.copyFrom(body, at, Math.min(48 * 1024, body.length - at))).build());
        }
        if (!done.isDone()) requests.onCompleted();
        return done.get(60, TimeUnit.SECONDS);
    }

    private static UploadRenditionResponse uploadRendition(ArchiveServiceGrpc.ArchiveServiceStub stub, String name,
            String mediaType, byte[] body) throws Exception {
        var done = new CompletableFuture<UploadRenditionResponse>();
        StreamObserver<UploadRenditionRequest> requests = stub.uploadRendition(new StreamObserver<>() {
            @Override public void onNext(UploadRenditionResponse value) { done.complete(value); }
            @Override public void onError(Throwable t) { done.completeExceptionally(t); }
            @Override public void onCompleted() { }
        });
        requests.onNext(UploadRenditionRequest.newBuilder().setHeader(UploadRenditionHeader.newBuilder()
                .setAddress(entry())
                .setRendition(RenditionDescriptor.newBuilder().setName(name).setMediaType(mediaType))
                .setSizeBytes(body.length)
                .setExpectedSha256(sha256(body))).build());
        for (int at = 0; at < body.length && !done.isDone(); at += 32 * 1024) {
            requests.onNext(UploadRenditionRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(body, at, Math.min(32 * 1024, body.length - at))).build());
        }
        if (!done.isDone()) requests.onCompleted();
        return done.get(60, TimeUnit.SECONDS);
    }

    private static byte[] readRendition(ArchiveServiceGrpc.ArchiveServiceBlockingStub archives, String rendition) {
        var out = new ByteArrayOutputStream();
        long declared = -1;
        var frames = archives.readRendition(ReadRenditionRequest.newBuilder().setAddress(entry()).setRendition(rendition).build());
        while (frames.hasNext()) {
            ReadRenditionResponse frame = frames.next();
            if (frame.hasHeader()) {
                declared = frame.getHeader().getSizeBytes();
                assertThat(frame.getHeader().getWholeObject()).isTrue();
            } else {
                out.writeBytes(frame.getData().toByteArray());
            }
        }
        assertThat(out.size()).as("rendition %s length matches its header", rendition).isEqualTo(declared);
        return out.toByteArray();
    }

    private static byte[] renditionData(List<RenditionContent> renditions, String name) {
        return renditions.stream().filter(r -> r.getRendition().getName().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError(name + " missing from " + renditions)).getData().toByteArray();
    }

    /**
     * Reads the whole topic from its start with the validating serde and groups the events by
     * record key, in record order. The read stops once a full quiet second passes after the
     * deadline for the first record, so a late or extra record is seen rather than missed.
     */
    private static Map<String, List<DocumentEvent>> consumeByKey(Duration patience) {
        var deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, KafkaOutboxPublisher.documentEventDescriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE, DocumentEvent.getDescriptor().getFullName(),
                ProtoMoltSerdeConfig.VALIDATE_ON_READ, true), false);
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "repo-eval-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var byKey = new LinkedHashMap<String, List<DocumentEvent>>();
        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + patience.toNanos();
            int quietPolls = 0;
            while (System.nanoTime() < deadline && quietPolls < 3) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    // Only settle once something has arrived; before that keep waiting for the relay.
                    if (!byKey.isEmpty()) quietPolls++;
                    continue;
                }
                quietPolls = 0;
                for (ConsumerRecord<String, Message> record : records) {
                    assertThat(record.value()).as("the validating deserializer returns the generated event type")
                            .isInstanceOf(DocumentEvent.class);
                    byKey.computeIfAbsent(record.key(), key -> new ArrayList<>()).add((DocumentEvent) record.value());
                }
            }
        }
        assertThat(byKey).as("events arrived on %s within %s", TOPIC, patience).isNotEmpty();
        return byKey;
    }

    private static int pendingOutboxEvents() throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT count(*) FROM repo_catalog.catalog_outbox WHERE relayed_at IS NULL");
                var result = query.executeQuery()) {
            assertThat(result.next()).isTrue();
            return result.getInt(1);
        }
    }

    private static String prefixOf(Drive drive) {
        String prefix = drive.getPrefix();
        int start = 0;
        while (start < prefix.length() && prefix.charAt(start) == '/') start++;
        return prefix.substring(start);
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] pseudoRandom(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static void assertCode(ThrowingCallable call, Status.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).as(failure.getStatus().toString()).isEqualTo(code));
    }
}
