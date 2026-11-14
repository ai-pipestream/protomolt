package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.emit.parquet.ParquetRendererProvider;
import ai.protomolt.proto.emit.parquet.v1.ParquetMaskStrategy;
import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.repo.archive.ArchiveCoordinates;
import ai.protomolt.proto.repo.archive.RepositorySinkRenderers;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.sink.v1.ArchiveRenditionSource;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.DocumentPartSource;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.ProcessingStateSource;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkServiceGrpc;
import ai.protomolt.proto.repo.sink.v1.SaveAsCapability;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end public {@code Save as...} Parquet rendition qualification suite over
 * {@link RepoServices}, PostgreSQL 18, LocalStack S3, gRPC, and the Connect HTTP gateway.
 */
@Testcontainers
class ParquetSaveAsRenditionIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String OPERATOR = "synthetic-parquet-it-operator-token";
    private static final String READER_TOKEN = "synthetic-parquet-it-reader-token";
    private static final String WRITER_TOKEN = "synthetic-parquet-it-writer-token";
    private static final String ARCHIVE = "parquet-archive";
    private static final String ARCHIVE_DRIVE = "parquet-archive-drive";
    private static final String DEFAULT_DATASOURCE = "ds-archive";
    private static final String BATCH_PROVIDER_ID = "ai.protomolt.emit.parquet.it.batch_items";

    private static final String IT_PROTO = """
            syntax = "proto3";
            package pq.saveas.it;

            import "google/protobuf/any.proto";
            import "google/protobuf/struct.proto";
            import "google/protobuf/timestamp.proto";
            import "ai/protomolt/proto/meta/v1/metadata.proto";
            import "ai/protomolt/proto/projection/v1/projection.proto";
            import "ai/protomolt/proto/validate/v1/validate.proto";

            enum ExportStatus {
              EXPORT_STATUS_UNSPECIFIED = 0;
              EXPORT_STATUS_VERIFIED = 1;
            }

            message Observation {
              string obs_id = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
              uint64 high_counter = 2;
              fixed64 max_counter = 3;
              optional string comment = 4;
              string contact_email = 5 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
              optional string secret_token = 6 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "secret"}];
              ExportStatus status = 7;
              google.protobuf.Timestamp recorded_at = 8;
              google.protobuf.Struct attributes = 9;
              string unprojected_detail = 10;
            }

            message RecursiveTree {
              string label = 1;
              RecursiveTree child = 2;
            }

            message EmptyPayload {}

            message AnyCarrier {
              string carrier_id = 1;
              google.protobuf.Any raw_any = 2;
            }

            message LineItem {
              string sku = 1;
              int64 units = 2;
              string buyer_email = 3 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
            }

            message InvoiceDocument {
              string invoice_id = 1 [(ai.protomolt.proto.validate.v1.field).required = true];
              repeated LineItem line_items = 2;
            }

            message InvoiceLineParquetRow {
              option (ai.protomolt.proto.projection.v1.sources) = {
                source: "pq.saveas.it.LineItem"
              };
              string sku = 1 [
                (ai.protomolt.proto.validate.v1.field).required = true,
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "sku" } }
              ];
              int64 units = 2 [
                (ai.protomolt.proto.validate.v1.field) = { required: true, int64: { gt: 0 } },
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "units" } }
              ];
              string buyer_email = 3 [
                (ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"},
                (ai.protomolt.proto.projection.v1.from) = { paths: { path: "buyer_email" } }
              ];
            }
            """;

    private static FileDescriptor itFile;
    private static Descriptor observationDesc;
    private static Descriptor recursiveDesc;
    private static Descriptor emptyDesc;
    private static Descriptor anyCarrierDesc;
    private static Descriptor invoiceDesc;
    private static Descriptor lineItemDesc;
    private static MessageProjection invoiceLineProjection;

    private String accountId;
    private RepoServiceConfig repoConfig;
    private AccessPolicyCallers accessPolicy;
    private RepoServices services;
    private ManagedChannel repoChannel;
    private String httpBase;

    @BeforeAll
    static void compileTestProtos() throws Exception {
        String metaProto = readResource("ai/protomolt/proto/meta/v1/metadata.proto");
        String projectionProto = readResource("ai/protomolt/proto/projection/v1/projection.proto");
        String validateProto = readResource("ai/protomolt/proto/validate/v1/validate.proto");
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/protomolt/proto/meta/v1/metadata.proto", metaProto, "meta")
                .add("ai/protomolt/proto/projection/v1/projection.proto", projectionProto, "proj")
                .add("ai/protomolt/proto/validate/v1/validate.proto", validateProto, "val")
                .add("pq/saveas/it/saveas_it.proto", IT_PROTO, "test")
                .build());
        FileDescriptorSet set =
                FileDescriptorSet.parseFrom(compiled.descriptorSet().toByteArray());
        Map<String, FileDescriptor> linked = linkDescriptorSet(set);
        itFile = linked.get("pq/saveas/it/saveas_it.proto");
        observationDesc = itFile.findMessageTypeByName("Observation");
        recursiveDesc = itFile.findMessageTypeByName("RecursiveTree");
        emptyDesc = itFile.findMessageTypeByName("EmptyPayload");
        anyCarrierDesc = itFile.findMessageTypeByName("AnyCarrier");
        invoiceDesc = itFile.findMessageTypeByName("InvoiceDocument");
        lineItemDesc = itFile.findMessageTypeByName("LineItem");

        DescriptorRegistry registry = DescriptorRegistry.create();
        for (FileDescriptor fd : linked.values()) {
            registry.registerFile(fd);
        }
        Descriptor rowDesc = itFile.findMessageTypeByName("InvoiceLineParquetRow");
        invoiceLineProjection = MessageProjection.forTarget(rowDesc, registry).orElseThrow();
    }

    @BeforeEach
    void setUp() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        accountId = "pq-it-acct-" + suffix;
        String prefix = "it-pq-" + suffix;
        repoConfig = new RepoServiceConfig(
                0,
                new LedgerConfig(
                        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                prefix,
                0,
                null,
                null,
                0,
                0L)
                .withManagedStorage(new ManagedStoragePolicy(
                        prefix + "-orig", prefix + "-realm", true));

        accessPolicy = new AccessPolicyCallers(AccessPolicy.newBuilder()
                .addPrincipals(principal("reader", READER_TOKEN))
                .addPrincipals(principal("writer", WRITER_TOKEN))
                .build());

        // Start with RepoServices.defaultSaveAsRenderers() to prove ServiceLoader discovery of
        // ParquetRendererProvider.PROVIDER_ID works out of the box, plus one declared repeated
        // projection profile.
        ParquetRendererProvider batchProjected = ParquetRendererProvider.projectedRepeated(
                BATCH_PROVIDER_ID,
                "pq.saveas.it.InvoiceDocument#line_items",
                invoiceDesc,
                invoiceDesc.findFieldByName("line_items"),
                invoiceLineProjection);
        List<RendererProvider> providers = new ArrayList<>();
        providers.add(new ParquetRendererProvider());
        providers.add(batchProjected);
        RepositorySinkRenderers renderers = RepositorySinkRenderers.of(
                providers, Set.of(ParquetRendererProvider.PROVIDER_ID, BATCH_PROVIDER_ID));

        services = RepoServices.build(repoConfig)
                .withAdmittedSchemas(List.of(itFile))
                .withAccountBindings(AccountBindingResolver.of(defaultAccountBindings()))
                .withRenderers(renderers);

        // Also verify RepoServices.defaultSaveAsRenderers() discovers the installed single-message
        // Parquet provider via ServiceLoader.
        assertThat(RepoServices.defaultSaveAsRenderers().registry().capabilities())
                .extracting(RendererCapability::getProviderId)
                .containsExactly(ParquetRendererProvider.PROVIDER_ID);

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
    }

    @AfterEach
    void tearDown() {
        if (repoChannel != null) {
            repoChannel.shutdownNow();
            repoChannel = null;
        }
        if (services != null) {
            services.close();
            services = null;
        }
    }

    @Test
    void capabilityDiscoveryAndSaveAsParquetOverGrpcAndConnectPreserveProvenanceAndSourceState(
            @TempDir Path dir) throws Exception {
        EntryAddress obsEntry = entry("obs-entry-1");
        DynamicMessage obsPayload = buildObservation(
                "obs-77",
                Long.MIN_VALUE, // 2^63
                -1L, // 2^64 - 1
                null,
                "scientist@example.com",
                "sec-xyz-999");

        // Publish authoritative protobuf rendition v1.
        PublishRepositorySinkResponse seeded =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId("seed-obs-1")
                                .setSource(processingSource(obsPayload, "run-obs-1", "capture"))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(obsEntry)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("state")
                                                .setMediaType("application/x-protobuf")))
                                .build());
        assertThat(seeded.getJob().getProvenance().getAuthoritativeProtobuf()).isTrue();
        byte[] originalSourceBytes = readAllRenditionBytes(obsEntry, "state");
        assertThat(originalSourceBytes).isEqualTo(obsPayload.toByteArray());

        SinkSource obsRepoSource = archiveRenditionSource(obsEntry, "state");

        // 1. Discover Save-as capabilities over BOTH gRPC and the Connect HTTP gateway.
        ListSaveAsCapabilitiesResponse grpcCaps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(obsRepoSource)
                                .build());
        SaveAsCapability singleCap = findCapability(grpcCaps, ParquetRendererProvider.PROVIDER_ID);
        assertThat(singleCap.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);
        assertThat(singleCap.getCapability().getMediaType())
                .isEqualTo("application/vnd.apache.parquet");
        assertThat(singleCap.getCapability().getExtension()).isEqualTo("parquet");

        // Closed options_descriptors returned by ListSaveAsCapabilities must self-sufficiently
        // link ParquetRendererOptions without any external schema registry!
        FileDescriptorSet optionsClosure =
                FileDescriptorSet.parseFrom(singleCap.getOptionsDescriptors());
        Map<String, FileDescriptor> linkedOptions = linkDescriptorSet(optionsClosure);
        Descriptor linkedOptsDesc = linkedOptions.values().stream()
                .flatMap(fd -> fd.getMessageTypes().stream())
                .filter(d -> d.getFullName().equals(singleCap.getCapability().getOptionsType()))
                .findFirst()
                .orElseThrow();
        assertThat(DescriptorFingerprints.fingerprintOf(linkedOptsDesc))
                .isEqualTo(singleCap.getCapability().getOptionsFingerprint());

        // Batch repeated-projection provider expects InvoiceDocument, so it reports INCOMPATIBLE
        // for Observation with a diagnostic reason.
        SaveAsCapability batchCapOnObs = findCapability(grpcCaps, BATCH_PROVIDER_ID);
        assertThat(batchCapOnObs.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE);
        assertThat(batchCapOnObs.getIncompatibilityReason())
                .contains("expects source type 'pq.saveas.it.InvoiceDocument'");

        HttpResponse<byte[]> connectCapsResp = connectUnary(
                READER_TOKEN,
                RepositorySinkServiceGrpc.getListSaveAsCapabilitiesMethod().getFullMethodName(),
                ListSaveAsCapabilitiesRequest.newBuilder().setSource(obsRepoSource).build());
        assertThat(connectCapsResp.statusCode()).isEqualTo(200);
        SaveAsCapability connectSingleCap = findCapability(
                ListSaveAsCapabilitiesResponse.parseFrom(connectCapsResp.body()),
                ParquetRendererProvider.PROVIDER_ID);
        assertThat(connectSingleCap.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);

        // 2. Execute Save-as Parquet over Connect HTTP gateway with column projection + masking.
        ParquetRendererOptions saveOptions = ParquetRendererOptions.newBuilder()
                .addColumns("obs_id")
                .addColumns("high_counter")
                .addColumns("max_counter")
                .addColumns("comment")
                .addColumns("contact_email")
                .addColumns("secret_token")
                .addColumns("status")
                .addColumns("recorded_at")
                .addColumns("attributes")
                .addMaskedSensitivityClasses("pii")
                .addMaskedSensitivityClasses("secret")
                .setMaskStrategy(ParquetMaskStrategy.PARQUET_MASK_STRATEGY_REDACT)
                .setMappingId("obs-columnar-v1")
                .setPolicyId("public-redacted")
                .build();

        ConversionSpec conversion = conversionSpecFrom(
                singleCap.getCapability(), saveOptions, "obs-columnar-v1", "public-redacted");

        PublishRepositorySinkRequest publishParquetReq = PublishRepositorySinkRequest.newBuilder()
                .setOperationId("publish-obs-parquet-1")
                .setSource(obsRepoSource)
                .setConversion(conversion)
                .setDestination(SinkDestination.newBuilder()
                        .setAddress(obsEntry)
                        .setExpectedDestinationVersion(1L)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("observation_parquet")
                                .setMediaType("application/vnd.apache.parquet")))
                .build();

        HttpResponse<byte[]> connectPublishResp = connectUnary(
                WRITER_TOKEN,
                RepositorySinkServiceGrpc.getPublishRepositorySinkMethod().getFullMethodName(),
                publishParquetReq);
        assertThat(connectPublishResp.statusCode()).isEqualTo(200);
        PublishRepositorySinkResponse published =
                PublishRepositorySinkResponse.parseFrom(connectPublishResp.body());
        assertThat(published.getJob().getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);

        RenditionProvenance prov = published.getJob().getProvenance();
        assertThat(prov.getDestinationVersion()).isEqualTo(2L);
        assertThat(prov.getSourceRevision()).isEqualTo(1L);
        assertThat(prov.getAuthoritativeProtobuf()).isFalse();
        assertThat(prov.getProviderId()).isEqualTo(ParquetRendererProvider.PROVIDER_ID);
        assertThat(prov.getProviderVersion()).isEqualTo(ParquetRendererProvider.PROVIDER_VERSION);
        assertThat(prov.getSourceProfile())
                .isEqualTo(ParquetRendererProvider.SINGLE_DOCUMENT_SOURCE_PROFILE);
        assertThat(prov.getOptionsType())
                .isEqualTo(ParquetRendererOptions.getDescriptor().getFullName());
        assertThat(prov.getOptionsFingerprint())
                .isEqualTo(singleCap.getCapability().getOptionsFingerprint());
        assertThat(prov.getOptionsSha256()).hasSize(64);
        assertThat(prov.getMappingId()).isEqualTo("obs-columnar-v1");
        assertThat(prov.getPolicyId()).isEqualTo("public-redacted");
        assertThat(prov.getOutputMediaType()).isEqualTo("application/vnd.apache.parquet");
        assertThat(prov.getLossDescription()).contains("non-authoritative").contains("UINT_64");

        // 3. Read back the committed .parquet rendition via ArchiveService.ReadRendition and
        // parse with ParquetFileReader + ParquetReader<Group>.
        byte[] parquetBytes = readAllRenditionBytes(obsEntry, "observation_parquet");
        Path parquetFile = dir.resolve("observation.parquet");
        Files.write(parquetFile, parquetBytes);

        try (ParquetFileReader footer = ParquetFileReader.open(new LocalInputFile(parquetFile))) {
            MessageType schema = footer.getFooter().getFileMetaData().getSchema();
            assertThat(schema.containsField("unprojected_detail")).isFalse();
            assertThat(schema.getType("high_counter").getLogicalTypeAnnotation())
                    .isEqualTo(LogicalTypeAnnotation.intType(64, false));
            assertThat(schema.getType("max_counter").getLogicalTypeAnnotation())
                    .isEqualTo(LogicalTypeAnnotation.intType(64, false));
        }

        List<Group> rows = readAllGroups(parquetFile);
        assertThat(rows).hasSize(1);
        Group row = rows.getFirst();
        assertThat(row.getString("obs_id", 0)).isEqualTo("obs-77");
        assertThat(Long.toUnsignedString(row.getLong("high_counter", 0)))
                .isEqualTo("9223372036854775808");
        assertThat(Long.toUnsignedString(row.getLong("max_counter", 0)))
                .isEqualTo("18446744073709551615");
        assertThat(row.getFieldRepetitionCount("comment")).isZero();
        assertThat(row.getString("contact_email", 0)).isEqualTo("***");
        assertThat(row.getString("secret_token", 0)).isEqualTo("***");
        assertThat(row.getBinary("status", 0).toStringUsingUTF8())
                .isEqualTo("EXPORT_STATUS_VERIFIED");
        assertThat(row.getLong("recorded_at", 0)).isEqualTo(1_720_000_000_000_789L);
        assertThat(row.getBinary("attributes", 0).toStringUsingUTF8())
                .contains("\"site\":\"alpha\"");

        // 4. Verify ListRenditionProvenance reports both authoritative state and non-authoritative
        // Parquet rendition, and original protobuf rendition bytes are 100% unchanged!
        ListRenditionProvenanceResponse provList =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listRenditionProvenance(ListRenditionProvenanceRequest.newBuilder()
                                .setAddress(obsEntry)
                                .build());
        assertThat(provList.getCurrentEntryVersion()).isEqualTo(2L);
        assertThat(provList.getEntriesList()).hasSize(2);
        assertThat(readAllRenditionBytes(obsEntry, "state")).isEqualTo(originalSourceBytes);

        // 5. Selecting the derived non-authoritative .parquet rendition as an
        // ArchiveRenditionSource MUST fail closed!
        SinkSource derivedParquetAsSource = archiveRenditionSource(obsEntry, "observation_parquet");
        assertThatThrownBy(() -> as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                        .setSource(derivedParquetAsSource)
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isIn(Status.Code.FAILED_PRECONDITION, Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void unpackedAnyOccurrenceIsCompatibleWhileOpaqueAnyRecursiveAndEmptySourcesReportIncompatible(
            @TempDir Path dir) throws Exception {
        // 1. Save a v2 Document whose structured_data (google.protobuf.Any) packs Observation.
        EntryAddress docBackingEntry = entry("doc-any-occurrence-1");
        NodeAddress docAddress = ArchiveCoordinates.address(docBackingEntry);
        DynamicMessage obs = buildObservation(
                "obs-from-any", 42L, 84L, "unpacked-any-note", "user@example.com", null);
        Any packedObs = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + observationDesc.getFullName())
                .setValue(obs.toByteString())
                .build();
        Document v2Doc = Document.newBuilder()
                .setDocId(docBackingEntry.getEntryId())
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(DEFAULT_DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder()
                                .addPermissions(AccessRule.newBuilder()
                                        .setIdentityType("group")
                                        .setIdentity("pq-team")
                                        .setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Telemetry Carrier"))
                .setStructuredData(packedObs)
                .build();
        SaveDocumentResponse savedDoc =
                as(OPERATOR, DocumentServiceGrpc.newBlockingStub(repoChannel))
                        .saveDocument(SaveDocumentRequest.newBuilder()
                                .setOperationId("save-v2-doc-any-1")
                                .setTarget(DocumentTarget.newBuilder().setAddress(docAddress))
                                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                                .setDocument(v2Doc)
                                .addWrites(PartWrite.newBuilder()
                                        .setSelector(PartSelector.newBuilder()
                                                .setPart(DocumentPart.DOCUMENT_PART_CORE))
                                        .setAction(WriteAction.WRITE_ACTION_PUT))
                                .setOwner(SaveOwner.newBuilder()
                                        .setIntake(IntakeOrigin.newBuilder()
                                                .setDatasourceId(DEFAULT_DATASOURCE)))
                                .setDrive(ARCHIVE_DRIVE)
                                .build());
        assertThat(savedDoc.getAccession().getRevision()).isEqualTo(1L);

        // Selecting DOCUMENT_PART_CORE directly (which contains opaque Any fields like
        // structured_data) reports INCOMPATIBLE for single-message Parquet.
        SinkSource wholeCoreDocSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setDocumentPart(DocumentPartSource.newBuilder()
                                .setAddress(docAddress)
                                .setRevision(RevisionSelector.newBuilder().setRevision(1L))
                                .setSelector(PartSelector.newBuilder()
                                        .setPart(DocumentPart.DOCUMENT_PART_CORE))))
                .build();
        ListSaveAsCapabilitiesResponse coreCaps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(wholeCoreDocSource)
                                .build());
        assertThat(findCapability(coreCaps, ParquetRendererProvider.PROVIDER_ID).getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE);

        // Selecting the unpacked occurrence `occurrence_field_path = "structured_data"` resolves
        // Observation's retained schema and reports COMPATIBLE!
        SinkSource unpackedOccurrenceSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setDocumentPart(DocumentPartSource.newBuilder()
                                .setAddress(docAddress)
                                .setRevision(RevisionSelector.newBuilder().setRevision(1L))
                                .setSelector(PartSelector.newBuilder()
                                        .setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .setOccurrenceFieldPath("structured_data")))
                .build();
        ListSaveAsCapabilitiesResponse unpackedCaps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(unpackedOccurrenceSource)
                                .build());
        SaveAsCapability unpackedParquetCap =
                findCapability(unpackedCaps, ParquetRendererProvider.PROVIDER_ID);
        assertThat(unpackedParquetCap.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);

        // Save unpacked occurrence as a Parquet rendition on a target archive entry.
        EntryAddress targetEntry = entry("unpacked-any-target");
        ParquetRendererOptions removePiiOpts = ParquetRendererOptions.newBuilder()
                .addMaskedSensitivityClasses("pii")
                .setMaskStrategy(ParquetMaskStrategy.PARQUET_MASK_STRATEGY_REMOVE)
                .build();
        PublishRepositorySinkResponse unpackedPublished =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId("publish-unpacked-any-parquet")
                                .setSource(unpackedOccurrenceSource)
                                .setConversion(conversionSpecFrom(
                                        unpackedParquetCap.getCapability(), removePiiOpts, "", ""))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(targetEntry)
                                        .setCreateOnly(true)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("structured_parquet")
                                                .setMediaType("application/vnd.apache.parquet")))
                                .build());
        assertThat(unpackedPublished.getJob().getProvenance().getSourceOccurrenceFieldPath())
                .isEqualTo("structured_data");

        Path unpackedParquetFile = dir.resolve("unpacked-occurrence.parquet");
        Files.write(
                unpackedParquetFile,
                readAllRenditionBytes(targetEntry, "structured_parquet"));
        Group unpackedRow = readAllGroups(unpackedParquetFile).getFirst();
        assertThat(unpackedRow.getString("obs_id", 0)).isEqualTo("obs-from-any");
        assertThat(unpackedRow.getString("comment", 0)).isEqualTo("unpacked-any-note");
        assertThat(unpackedRow.getString("contact_email", 0)).isEmpty();

        // 2. Recursive, Empty, and AnyCarrier processing sources report INCOMPATIBLE in
        // ListSaveAsCapabilities and fail closed with FAILED_PRECONDITION on PublishRepositorySink.
        DynamicMessage recursiveMsg = DynamicMessage.newBuilder(recursiveDesc)
                .setField(recursiveDesc.findFieldByName("label"), "root")
                .build();
        assertIncompatibleProcessingSource(recursiveMsg, "Recursive message schema");

        DynamicMessage emptyMsg = DynamicMessage.getDefaultInstance(emptyDesc);
        assertIncompatibleProcessingSource(emptyMsg, "zero columns");

        DynamicMessage anyCarrierMsg = DynamicMessage.newBuilder(anyCarrierDesc)
                .setField(anyCarrierDesc.findFieldByName("carrier_id"), "c-1")
                .build();
        assertIncompatibleProcessingSource(anyCarrierMsg, "google.protobuf.Any");
    }

    @Test
    void declaredRepeatedProjectionRendersMultiRowParquetAndRefusalsCommitZeroArtifacts(
            @TempDir Path dir) throws Exception {
        EntryAddress invoiceEntry = entry("invoice-entry-1");
        FieldDescriptor lineItemsField = invoiceDesc.findFieldByName("line_items");
        DynamicMessage validInvoice = DynamicMessage.newBuilder(invoiceDesc)
                .setField(invoiceDesc.findFieldByName("invoice_id"), "inv-2026-01")
                .addRepeatedField(
                        lineItemsField, buildLineItem("SKU-ALPHA", 3L, "alice@example.com"))
                .addRepeatedField(
                        lineItemsField, buildLineItem("SKU-BETA", 9L, "bob@example.com"))
                .build();

        // Seed authoritative InvoiceDocument rendition v1.
        as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                        .setOperationId("seed-invoice-1")
                        .setSource(processingSource(validInvoice, "run-inv-1", "ingest"))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(invoiceEntry)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("state")
                                        .setMediaType("application/x-protobuf")))
                        .build());

        SinkSource invoiceSource = archiveRenditionSource(invoiceEntry, "state");
        ListSaveAsCapabilitiesResponse caps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(invoiceSource)
                                .build());

        // Single-message provider AND declared repeated-projection provider are BOTH compatible!
        assertThat(findCapability(caps, ParquetRendererProvider.PROVIDER_ID).getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);
        SaveAsCapability batchCap = findCapability(caps, BATCH_PROVIDER_ID);
        assertThat(batchCap.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);

        ParquetRendererOptions maskPii = ParquetRendererOptions.newBuilder()
                .addMaskedSensitivityClasses("pii")
                .build();
        PublishRepositorySinkResponse batchPublished =
                as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                                .setOperationId("publish-invoice-lines-parquet")
                                .setSource(invoiceSource)
                                .setConversion(conversionSpecFrom(
                                        batchCap.getCapability(), maskPii, "invoice-lines-v1", ""))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(invoiceEntry)
                                        .setExpectedDestinationVersion(1L)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("line_items_parquet")
                                                .setMediaType("application/vnd.apache.parquet")))
                                .build());
        assertThat(batchPublished.getJob().getProvenance().getDestinationVersion()).isEqualTo(2L);
        assertThat(batchPublished.getJob().getProvenance().getAuthoritativeProtobuf()).isFalse();

        Path linesParquet = dir.resolve("line-items.parquet");
        Files.write(linesParquet, readAllRenditionBytes(invoiceEntry, "line_items_parquet"));
        List<Group> rows = readAllGroups(linesParquet);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString("sku", 0)).isEqualTo("SKU-ALPHA");
        assertThat(rows.get(0).getLong("units", 0)).isEqualTo(3L);
        assertThat(rows.get(0).getString("buyer_email", 0)).isEqualTo("***");
        assertThat(rows.get(1).getString("sku", 0)).isEqualTo("SKU-BETA");
        assertThat(rows.get(1).getLong("units", 0)).isEqualTo(9L);

        // 1. Fail-closed row validation refusal: second LineItem has units = 0 (violates gt: 0).
        DynamicMessage badRowInvoice = DynamicMessage.newBuilder(invoiceDesc)
                .setField(invoiceDesc.findFieldByName("invoice_id"), "inv-bad-row")
                .addRepeatedField(
                        lineItemsField, buildLineItem("SKU-OK", 2L, "alice@example.com"))
                .addRepeatedField(
                        lineItemsField, buildLineItem("SKU-ZERO", 0L, "bob@example.com"))
                .build();
        EntryAddress badTarget = entry("bad-invoice-target");
        assertThatThrownBy(() -> as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                        .setOperationId("publish-bad-invoice-lines")
                        .setSource(processingSource(badRowInvoice, "run-bad-1", "ingest"))
                        .setConversion(conversionSpecFrom(
                                batchCap.getCapability(),
                                ParquetRendererOptions.getDefaultInstance(),
                                "",
                                ""))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(badTarget)
                                .setCreateOnly(true)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("lines")
                                        .setMediaType("application/vnd.apache.parquet")))
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertEntryDoesNotExist(badTarget);

        // 2. Output byte limit refusal (`max_output_bytes = 64`) fails closed with
        // RESOURCE_EXHAUSTED and commits zero versions!
        ParquetRendererOptions tinyCapOptions =
                ParquetRendererOptions.newBuilder().setMaxOutputBytes(64L).build();
        EntryAddress limitTarget = entry("limit-exceeded-target");
        assertThatThrownBy(() -> as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                        .setOperationId("publish-limit-exceeded")
                        .setSource(invoiceSource)
                        .setConversion(conversionSpecFrom(
                                batchCap.getCapability(), tinyCapOptions, "", ""))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(limitTarget)
                                .setCreateOnly(true)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("lines")
                                        .setMediaType("application/vnd.apache.parquet")))
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        RepositorySinkJob limitJob =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                                .setAccountId(accountId)
                                .setOperationId("publish-limit-exceeded")
                                .build())
                        .getJob();
        assertThat(limitJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED);
        assertThat(limitJob.getBytesCommitted()).isZero();
        assertEntryDoesNotExist(limitTarget);

        // 3. Cooperative job cancellation aborts in-flight Parquet render cleanly with zero
        // committed destination bytes!
        CountDownLatch enteredRender = new CountDownLatch(1);
        RendererProvider cancellableParquet =
                cancellableParquetWrapper("ai.protomolt.emit.parquet.it.cancellable", enteredRender);
        services.withRenderers(RepositorySinkRenderers.of(
                List.of(cancellableParquet),
                Set.of(cancellableParquet.capability().getProviderId())));

        EntryAddress cancelledTarget = entry("cancelled-parquet-target");
        String cancelOpId = "publish-cancelled-parquet";
        as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                        .setOperationId(cancelOpId)
                        .setAsync(true)
                        .setSource(invoiceSource)
                        .setConversion(conversionSpecFrom(
                                cancellableParquet.capability(),
                                ParquetRendererOptions.getDefaultInstance(),
                                "",
                                ""))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(cancelledTarget)
                                .setCreateOnly(true)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("cancelled_parquet")
                                        .setMediaType("application/vnd.apache.parquet")))
                        .build());
        assertThat(enteredRender.await(10, TimeUnit.SECONDS)).isTrue();

        as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .cancelRepositorySinkJob(CancelRepositorySinkJobRequest.newBuilder()
                        .setAccountId(accountId)
                        .setOperationId(cancelOpId)
                        .build());

        RepositorySinkJob cancelledJob = awaitTerminalJob(READER_TOKEN, cancelOpId);
        assertThat(cancelledJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED);
        assertThat(cancelledJob.getBytesCommitted()).isZero();
        assertEntryDoesNotExist(cancelledTarget);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void assertIncompatibleProcessingSource(
            DynamicMessage message, String expectedReasonSubstring) {
        SinkSource src = processingSource(
                message, "run-incompat-" + UUID.randomUUID(), "probe");
        ListSaveAsCapabilitiesResponse caps =
                as(READER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                        .listSaveAsCapabilities(ListSaveAsCapabilitiesRequest.newBuilder()
                                .setSource(src)
                                .build());
        SaveAsCapability cap = findCapability(caps, ParquetRendererProvider.PROVIDER_ID);
        assertThat(cap.getStatus())
                .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE);
        assertThat(cap.getIncompatibilityReason()).contains(expectedReasonSubstring);

        EntryAddress neverCreated = entry("never-created-" + UUID.randomUUID());
        assertThatThrownBy(() -> as(WRITER_TOKEN, RepositorySinkServiceGrpc.newBlockingStub(repoChannel))
                .publishRepositorySink(PublishRepositorySinkRequest.newBuilder()
                        .setOperationId("refused-" + UUID.randomUUID())
                        .setSource(src)
                        .setConversion(conversionSpecFrom(
                                cap.getCapability(),
                                ParquetRendererOptions.getDefaultInstance(),
                                "",
                                ""))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(neverCreated)
                                .setCreateOnly(true)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("parquet")
                                        .setMediaType("application/vnd.apache.parquet")))
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertEntryDoesNotExist(neverCreated);
    }

    private void assertEntryDoesNotExist(EntryAddress address) {
        assertThatThrownBy(() -> as(READER_TOKEN, ArchiveServiceGrpc.newBlockingStub(repoChannel))
                .getEntryManifest(GetEntryManifestRequest.newBuilder()
                        .setAddress(address)
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(ex -> assertThat(((StatusRuntimeException) ex).getStatus().getCode())
                        .isEqualTo(Status.Code.NOT_FOUND));
    }

    private static RendererProvider cancellableParquetWrapper(
            String providerId, CountDownLatch enteredRender) {
        ParquetRendererProvider delegate = new ParquetRendererProvider();
        RendererCapability cap = delegate.capability().toBuilder()
                .setProviderId(providerId)
                .build();
        return new RendererProvider() {
            @Override
            public RendererCapability capability() {
                return cap;
            }

            @Override
            public Descriptor optionsDescriptor() {
                return delegate.optionsDescriptor();
            }

            @Override
            public void check(Message input, Message options) throws RenderFailure {
                delegate.check(input, options);
            }

            @Override
            public void render(
                    Message input,
                    Message options,
                    OutputStream output,
                    Cancellation cancellation) throws IOException {
                enteredRender.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (System.nanoTime() < deadline) {
                    cancellation.check();
                    try {
                        Thread.sleep(20L);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new RenderFailure(
                                RenderFailure.Kind.CANCELLED, "Render interrupted", interrupted);
                    }
                }
                delegate.render(input, options, output, cancellation);
            }
        };
    }

    private static ConversionSpec conversionSpecFrom(
            RendererCapability cap,
            ParquetRendererOptions options,
            String mappingId,
            String policyId) {
        ConversionSpec.Builder builder = ConversionSpec.newBuilder()
                .setProviderId(cap.getProviderId())
                .setProviderVersion(cap.getProviderVersion())
                .setSourceProfile(cap.getSourceProfile())
                .setMediaType(cap.getMediaType())
                .setExtension(cap.getExtension())
                .setOptionsType(cap.getOptionsType())
                .setOptionsFingerprint(cap.getOptionsFingerprint())
                .setOptions(options.toByteString());
        if (!mappingId.isEmpty()) {
            builder.setMappingId(mappingId);
        }
        if (!policyId.isEmpty()) {
            builder.setPolicyId(policyId);
        }
        return builder.build();
    }

    private static SaveAsCapability findCapability(
            ListSaveAsCapabilitiesResponse response, String providerId) {
        return response.getCapabilitiesList().stream()
                .filter(c -> c.getCapability().getProviderId().equals(providerId))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Missing capability " + providerId + " in " + response.getCapabilitiesList()));
    }

    private SinkSource archiveRenditionSource(EntryAddress address, String renditionName) {
        return SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setArchiveRendition(ArchiveRenditionSource.newBuilder()
                                .setAddress(address)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName(renditionName))))
                .build();
    }

    private SinkSource processingSource(
            DynamicMessage message, String runId, String stepName) {
        Descriptor desc = message.getDescriptorForType();
        FileDescriptorSet closure = DescriptorFingerprints.closure(desc);
        return SinkSource.newBuilder()
                .setProcessingState(ProcessingStateSource.newBuilder()
                        .setAccountId(accountId)
                        .setMessageType(desc.getFullName())
                        .setSchemaFingerprint(DescriptorFingerprints.fingerprint(closure))
                        .setDescriptors(closure.toByteString())
                        .setPayload(message.toByteString())
                        .setRunId(runId)
                        .setStepName(stepName))
                .build();
    }

    private static DynamicMessage buildObservation(
            String obsId,
            long highCounter,
            long maxCounter,
            String comment,
            String email,
            String secret) throws Exception {
        Descriptor tsDesc = observationDesc.findFieldByName("recorded_at").getMessageType();
        Descriptor structDesc = observationDesc.findFieldByName("attributes").getMessageType();
        DynamicMessage.Builder structBuilder = DynamicMessage.newBuilder(structDesc);
        com.google.protobuf.util.JsonFormat.parser()
                .merge("{\"site\":\"alpha\",\"calibrated\":true}", structBuilder);

        DynamicMessage.Builder b = DynamicMessage.newBuilder(observationDesc)
                .setField(observationDesc.findFieldByName("obs_id"), obsId)
                .setField(observationDesc.findFieldByName("high_counter"), highCounter)
                .setField(observationDesc.findFieldByName("max_counter"), maxCounter)
                .setField(observationDesc.findFieldByName("contact_email"), email)
                .setField(
                        observationDesc.findFieldByName("status"),
                        observationDesc.findFieldByName("status")
                                .getEnumType()
                                .findValueByName("EXPORT_STATUS_VERIFIED"))
                .setField(
                        observationDesc.findFieldByName("recorded_at"),
                        DynamicMessage.newBuilder(tsDesc)
                                .setField(tsDesc.findFieldByName("seconds"), 1_720_000_000L)
                                .setField(tsDesc.findFieldByName("nanos"), 789_000)
                                .build())
                .setField(observationDesc.findFieldByName("attributes"), structBuilder.build())
                .setField(
                        observationDesc.findFieldByName("unprojected_detail"),
                        "internal-unprojected");
        if (comment != null) {
            b.setField(observationDesc.findFieldByName("comment"), comment);
        }
        if (secret != null) {
            b.setField(observationDesc.findFieldByName("secret_token"), secret);
        }
        return b.build();
    }

    private static DynamicMessage buildLineItem(String sku, long units, String email) {
        return DynamicMessage.newBuilder(lineItemDesc)
                .setField(lineItemDesc.findFieldByName("sku"), sku)
                .setField(lineItemDesc.findFieldByName("units"), units)
                .setField(lineItemDesc.findFieldByName("buyer_email"), email)
                .build();
    }

    private AccountBindings defaultAccountBindings() {
        AclIdentity group = AclIdentity.newBuilder()
                .setIdentityType("group")
                .setIdentity("pq-team")
                .build();
        AccountBinding reader = AccountBinding.newBuilder()
                .setPrincipal("reader")
                .addAccountIds(accountId)
                .addAclIdentities(group)
                .build();
        AccountBinding writer = AccountBinding.newBuilder()
                .setPrincipal("writer")
                .addAccountIds(accountId)
                .addAclIdentities(group)
                .addIntakeWriters(IntakeWriterScope.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(DEFAULT_DATASOURCE)
                        .setAllowCreate(true))
                .build();
        return AccountBindings.newBuilder()
                .addBindings(reader)
                .addBindings(writer)
                .build();
    }

    private RepositorySinkJob awaitTerminalJob(String token, String operationId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
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

    private static List<Group> readAllGroups(Path file) throws IOException {
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = new GroupBuilder(new LocalInputFile(file)).build()) {
            Group current;
            while ((current = reader.read()) != null) {
                rows.add(current);
            }
        }
        return rows;
    }

    private static String readResource(String path) throws IOException {
        try (InputStream in =
                     ParquetSaveAsRenditionIT.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Map<String, FileDescriptor> linkDescriptorSet(FileDescriptorSet set)
            throws Exception {
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            buildFileDescriptor(proto, byName, built);
        }
        return built;
    }

    private static FileDescriptor buildFileDescriptor(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> byName,
            Map<String, FileDescriptor> built) throws Exception {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        FileDescriptor[] deps = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            deps[i] = buildFileDescriptor(byName.get(proto.getDependency(i)), byName, built);
        }
        FileDescriptor linked = FileDescriptor.buildFrom(proto, deps);
        built.put(proto.getName(), linked);
        return linked;
    }

    private static final class GroupBuilder extends ParquetReader.Builder<Group> {
        private GroupBuilder(InputFile file) {
            super(file);
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return new GroupReadSupport();
        }
    }
}
