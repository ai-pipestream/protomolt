package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.renderer.GrpcRendererProvider;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RenderRequest;
import ai.protomolt.proto.emit.v1.RenderResponse;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.emit.v1.RendererServiceGrpc;
import ai.protomolt.proto.repo.accession.V2RepositoryFixture;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.archive.v1.ArchiveEntryFacts;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
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
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerDetails;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration tests for {@link RepositorySink} and {@link RepositorySinkGrpcService}
 * backed by PostgreSQL, LocalStack S3, {@link DocumentArchive}, colocated {@link RendererProvider}s,
 * and remote {@link RendererServiceGrpc} channels.
 */
@Testcontainers
class RepositorySinkIT {

    private static final String ACCOUNT = "acct-sink-it";
    private static final String ARCHIVE = "reports";
    private static final String DATASOURCE = "ds-sink";
    private static final int LARGE_MARKDOWN_BYTES = 1_572_864; // 1.5 MiB (> 1 MiB single upload chunk)

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    private static Descriptor noteV1;
    private static Descriptor noteV2;
    private static final AtomicReference<DocumentSchemaAdmission.Definition> ACTIVE_NOTE_SCHEMA =
            new AtomicReference<>();
    private static final AtomicBoolean NOTE_SCHEMA_AVAILABLE = new AtomicBoolean(true);

    private static final FaultInjectingInterceptor FAULTS = new FaultInjectingInterceptor();
    private static final SlowRenderController SLOW_RENDER = new SlowRenderController();

    private static V2RepositoryFixture repo;
    private static ManagedChannel documentsChannel;
    private static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private static Server remoteRendererServer;
    private static ManagedChannel remoteRendererChannel;
    private static RepositorySink sinkEngine;
    private static Server sinkAndArchiveServer;
    private static ManagedChannel sinkAndArchiveChannel;
    private static ArchiveServiceGrpc.ArchiveServiceBlockingStub archive;
    private static RepositorySinkServiceGrpc.RepositorySinkServiceBlockingStub sink;

    @BeforeAll
    static void boot() throws Exception {
        noteV1 = buildNoteDescriptor(false);
        noteV2 = buildNoteDescriptor(true);
        ACTIVE_NOTE_SCHEMA.set(dynamicSchemaDefinition(noteV1));

        DocumentSchemaAdmission.Definition factsDef =
                BuiltinDocumentSchema.definition(ArchiveEntryFacts.getDescriptor());
        String factsTypeUrl = "type.googleapis.com/" + ArchiveEntryFacts.getDescriptor().getFullName();
        String noteTypeUrl = "type.googleapis.com/" + noteV1.getFullName();
        DocumentSchemaAdmission.Resolver searchResolver = V2RepositoryFixture.searchMetadataResolver();

        repo = V2RepositoryFixture.builder(POSTGRES, LOCALSTACK, "repo-sink-it")
                .resolver(selection -> {
                    if (factsTypeUrl.equals(selection.typeUrl())) {
                        return factsDef;
                    }
                    if (noteTypeUrl.equals(selection.typeUrl())) {
                        if (!NOTE_SCHEMA_AVAILABLE.get()) {
                            throw new DocumentCandidateAdmission.SchemaUnavailable(
                                    "Live schema registry disabled for retained-schema verification", null);
                        }
                        return ACTIVE_NOTE_SCHEMA.get();
                    }
                    return searchResolver.select(selection);
                })
                .build();

        documentsChannel = InProcessChannelBuilder.forName("repo-sink-it")
                .intercept(FAULTS)
                .build();
        documents = DocumentServiceGrpc.newBlockingStub(documentsChannel);

        // Remote gRPC RendererService for SearchMetadata -> large streamed Markdown (> 1 MiB)
        String remoteName = InProcessServerBuilder.generateName();
        RendererCapability remoteCap = RendererCapability.newBuilder()
                .setProviderId("remote.search.markdown")
                .setProviderVersion("2.1.0")
                .setSourceProfile("ai.protomolt.proto.repo.v1.SearchMetadata")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .setMediaType("text/markdown")
                .setExtension("md")
                .setLossDescription("Projects SearchMetadata into a human-readable Markdown dossier; protobuf structure omitted")
                .setMaxInputBytes(2 * 1024 * 1024)
                .setMaxOutputBytes(4 * 1024 * 1024)
                .build();
        remoteRendererServer = InProcessServerBuilder.forName(remoteName)
                .addService(new RemoteMarkdownRendererService(remoteCap))
                .build()
                .start();
        remoteRendererChannel = InProcessChannelBuilder.forName(remoteName).build();
        GrpcRendererProvider remoteMarkdownProvider = new GrpcRendererProvider(
                remoteCap,
                SearchMetadata.getDescriptor(),
                Empty.getDescriptor(),
                remoteRendererChannel,
                Duration.ofSeconds(15),
                4 * 1024 * 1024);

        RendererProvider colocatedSummaryMd = new ColocatedSearchSummaryRenderer();
        RendererProvider colocatedSlowMd = new SlowCancellableSearchRenderer(SLOW_RENDER);
        RendererProvider colocatedOversize = new OversizeSearchRenderer();
        RendererProvider noteHtmlRenderer = new DynamicNoteHtmlRenderer();

        RepositorySinkRenderers renderers = RepositorySinkRenderers.of(
                List.of(
                        remoteMarkdownProvider,
                        colocatedSummaryMd,
                        colocatedSlowMd,
                        colocatedOversize,
                        noteHtmlRenderer),
                Set.of(
                        remoteCap.getProviderId(),
                        colocatedSummaryMd.capability().getProviderId(),
                        colocatedSlowMd.capability().getProviderId(),
                        colocatedOversize.capability().getProviderId(),
                        noteHtmlRenderer.capability().getProviderId()));

        ArchiveBindings bindings = (account, name) -> ARCHIVE.equals(name)
                ? Optional.of(new ArchiveBinding(
                        account, name, DATASOURCE, "", VersioningPolicy.VERSIONING_POLICY_RETAINED))
                : Optional.empty();
        DocumentArchiveGrpcService archiveGrpc =
                new DocumentArchiveGrpcService(documentsChannel, bindings, Clock.systemUTC());
        DocumentArchive archiveEngine = archiveGrpc.archive();
        sinkEngine = new RepositorySink(
                archiveEngine,
                new RenditionUploads(archiveEngine, documentsChannel),
                new RenditionReads(archiveEngine),
                renderers,
                Clock.systemUTC());

        String archiveServerName = InProcessServerBuilder.generateName();
        sinkAndArchiveServer = InProcessServerBuilder.forName(archiveServerName)
                .addService(archiveGrpc)
                .addService(new DocumentArchiveMutationGrpcService(archiveEngine))
                .addService(new RepositorySinkGrpcService(sinkEngine))
                .build()
                .start();
        sinkAndArchiveChannel = InProcessChannelBuilder.forName(archiveServerName).build();
        archive = ArchiveServiceGrpc.newBlockingStub(sinkAndArchiveChannel);
        sink = RepositorySinkServiceGrpc.newBlockingStub(sinkAndArchiveChannel);
    }

    @AfterAll
    static void shutdown() {
        if (sinkAndArchiveChannel != null) {
            sinkAndArchiveChannel.shutdownNow();
        }
        if (sinkAndArchiveServer != null) {
            sinkAndArchiveServer.shutdownNow();
        }
        if (sinkEngine != null) {
            sinkEngine.close();
        }
        if (remoteRendererChannel != null) {
            remoteRendererChannel.shutdownNow();
        }
        if (remoteRendererServer != null) {
            remoteRendererServer.shutdownNow();
        }
        if (documentsChannel != null) {
            documentsChannel.shutdownNow();
        }
        if (repo != null) {
            repo.close();
        }
    }

    @Test
    void publishesProcessingStateDirectlyWithoutPriorRevisionAndReprojectsViaArchiveRenditionSource() {
        EntryAddress target = newEntry();
        SearchMetadata state = SearchMetadata.newBuilder()
                .setTitle("Q4 Revenue Analysis")
                .setBody("All projections verified against ledger.")
                .setAuthor("analytics-engine")
                .build();
        SinkSource processingSource = processingStateSource(state, "run-900", "step-summarize");

        // 1. Discover capabilities on direct processing state without authorizing any write.
        ListSaveAsCapabilitiesResponse capabilities = sink.listSaveAsCapabilities(
                ListSaveAsCapabilitiesRequest.newBuilder().setSource(processingSource).build());
        assertThat(capabilities.getResolvedSourceRevision()).isZero();
        assertThat(capabilities.getSourceSchemaType())
                .isEqualTo(SearchMetadata.getDescriptor().getFullName());
        assertThat(capabilities.getCapabilitiesList())
                .filteredOn(c -> c.getStatus() == SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE)
                .extracting(c -> c.getCapability().getProviderId())
                .contains("colocated.search.summary", "remote.search.markdown");
        assertThat(revisions(target)).isZero();

        // 2. Publish processing state directly as an authoritative protobuf rendition (no prior revision fabricated).
        PublishRepositorySinkResponse directResp = sink.publishRepositorySink(
                PublishRepositorySinkRequest.newBuilder()
                        .setSource(processingSource)
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(target)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("state")
                                        .setMediaType(RepositorySink.PROTOBUF_MEDIA_TYPE))
                                .setCreateOnly(true))
                        .build());
        RepositorySinkJob directJob = directResp.getJob();
        assertThat(directJob.getState()).isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        RenditionProvenance directProv = directJob.getProvenance();
        assertThat(directProv.getSourceRevision()).isZero();
        assertThat(directProv.getDestinationVersion()).isEqualTo(1L);
        assertThat(directProv.getAuthoritativeProtobuf()).isTrue();
        assertThat(directProv.getProcessingRunId()).isEqualTo("run-900");
        assertThat(directProv.getProcessingStepName()).isEqualTo("step-summarize");
        assertThat(directProv.getLossDescription()).isEmpty();
        assertThat(directProv.getBytesRendered()).isEqualTo(state.getSerializedSize());
        assertThat(directProv.getBytesCommitted()).isEqualTo(state.getSerializedSize());
        assertThat(readRenditionBytes(target, "state", "", 0L)).isEqualTo(state.toByteString());

        // 3. Re-project that authoritative protobuf rendition through remote gRPC RendererService (> 1 MiB multi-chunk upload).
        SinkSource renditionSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setArchiveRendition(ArchiveRenditionSource.newBuilder()
                                .setAddress(target)
                                .setRevision(RevisionSelector.newBuilder().setCurrent(true))
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("state")
                                        .setMediaType(RepositorySink.PROTOBUF_MEDIA_TYPE))))
                .build();
        ConversionSpec remoteMarkdownSpec = ConversionSpec.newBuilder()
                .setProviderId("remote.search.markdown")
                .setProviderVersion("2.1.0")
                .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .build();

        PublishRepositorySinkResponse derivedResp = sink.publishRepositorySink(
                PublishRepositorySinkRequest.newBuilder()
                        .setSource(renditionSource)
                        .setConversion(remoteMarkdownSpec)
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(target)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("markdown")
                                        .setSubKey("dossier")
                                        .setMediaType("text/markdown"))
                                .setExpectedDestinationVersion(1L))
                        .build());
        RepositorySinkJob derivedJob = derivedResp.getJob();
        assertThat(derivedJob.getState()).isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        assertThat(derivedJob.getFrozenSourceRevision()).isEqualTo(1L);
        RenditionProvenance derivedProv = derivedJob.getProvenance();
        assertThat(derivedProv.getSourceRevision()).isEqualTo(1L);
        assertThat(derivedProv.getDestinationVersion()).isEqualTo(2L);
        assertThat(derivedProv.getAuthoritativeProtobuf()).isFalse();
        assertThat(derivedProv.getProviderId()).isEqualTo("remote.search.markdown");
        assertThat(derivedProv.getLossDescription()).contains("protobuf structure omitted");
        assertThat(derivedProv.getBytesRendered()).isGreaterThan(LARGE_MARKDOWN_BYTES);
        assertThat(derivedProv.getBytesCommitted()).isEqualTo(derivedProv.getBytesRendered());

        ByteString markdownBytes = readRenditionBytes(target, "markdown", "dossier", 2L);
        assertThat(markdownBytes.size()).isEqualTo((int) derivedProv.getBytesCommitted());
        assertThat(markdownBytes.substring(0, 64).toStringUtf8()).startsWith("# Q4 Revenue Analysis\n");

        // Both renditions remain readable on version 2, and ListRenditionProvenance exposes both with accurate flags.
        VersionManifest v2Manifest = archive.getEntryManifest(
                GetEntryManifestRequest.newBuilder().setAddress(target).setVersion(2L).build()).getManifest();
        assertThat(v2Manifest.getRenditionsList())
                .extracting(r -> ArchiveCoordinates.blobId(r.getRendition().getName(), r.getRendition().getSubKey()))
                .contains("state", "markdown/dossier");

        ListRenditionProvenanceResponse provList = sink.listRenditionProvenance(
                ListRenditionProvenanceRequest.newBuilder().setAddress(target).setVersion(2L).build());
        assertThat(provList.getCurrentEntryVersion()).isEqualTo(2L);
        assertThat(provList.getEntriesList()).hasSize(2);
        assertThat(provList.getEntriesList())
                .filteredOn(e -> e.getRendition().getName().equals("markdown"))
                .singleElement()
                .satisfies(e -> {
                    assertThat(e.getProvenance().getAuthoritativeProtobuf()).isFalse();
                    assertThat(e.getProvenance().getSourceRevision()).isEqualTo(1L);
                    // Target entry bumped to v2 when "markdown/dossier" landed, so source_entry v1 < current v2!
                    assertThat(e.getCurrentSourceRevision()).isEqualTo(2L);
                    assertThat(e.getSourceStale()).isTrue();
                });

        // 4. Attempting to treat the derived Markdown rendition as a protobuf ArchiveRenditionSource is refused!
        SinkSource derivedAsSource = SinkSource.newBuilder()
                .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                        .setArchiveRendition(ArchiveRenditionSource.newBuilder()
                                .setAddress(target)
                                .setRevision(RevisionSelector.newBuilder().setRevision(2L))
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("markdown")
                                        .setSubKey("dossier")
                                        .setMediaType("text/markdown"))))
                .build();
        StatusRuntimeException refused = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> sink.listSaveAsCapabilities(
                        ListSaveAsCapabilitiesRequest.newBuilder().setSource(derivedAsSource).build()));
        assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void rendersHistoricalAndCurrentStructuredOccurrencesFromRetainedSchemaAssetsWhenLiveRegistryIsOffline()
            throws Exception {
        EntryAddress sourceEntry = newEntry();
        NodeAddress sourceDocAddress = ArchiveCoordinates.address(sourceEntry);
        EntryAddress targetEntry = newEntry();

        try {
            NOTE_SCHEMA_AVAILABLE.set(true);
            ACTIVE_NOTE_SCHEMA.set(dynamicSchemaDefinition(noteV1));
            DynamicMessage v1Payload = DynamicMessage.newBuilder(noteV1)
                    .setField(noteV1.findFieldByName("headline"), "Initial Draft")
                    .build();
            SaveDocumentResponse r1 = saveStructuredNote(sourceEntry, 0L, v1Payload);
            assertThat(r1.getAccession().getRevision()).isEqualTo(1L);

            // Evolve Note schema (v2 adds field 2: summary) and save revision 2.
            ACTIVE_NOTE_SCHEMA.set(dynamicSchemaDefinition(noteV2));
            DynamicMessage v2Payload = DynamicMessage.newBuilder(noteV2)
                    .setField(noteV2.findFieldByName("headline"), "Final Release")
                    .setField(noteV2.findFieldByName("summary"), "Approved by architecture board")
                    .build();
            SaveDocumentResponse r2 = saveStructuredNote(sourceEntry, 1L, v2Payload);
            assertThat(r2.getAccession().getRevision()).isEqualTo(2L);

            // Shut off live schema resolution completely: RepositorySink MUST rely on retained revision schemas!
            NOTE_SCHEMA_AVAILABLE.set(false);

            SinkSource rev1Source = SinkSource.newBuilder()
                    .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                            .setDocumentPart(DocumentPartSource.newBuilder()
                                    .setAddress(sourceDocAddress)
                                    .setRevision(RevisionSelector.newBuilder().setRevision(1L))
                                    .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                    .setOccurrenceFieldPath("structured_data")))
                    .build();
            ListSaveAsCapabilitiesResponse rev1Caps = sink.listSaveAsCapabilities(
                    ListSaveAsCapabilitiesRequest.newBuilder().setSource(rev1Source).build());
            assertThat(rev1Caps.getResolvedSourceRevision()).isEqualTo(1L);
            assertThat(rev1Caps.getSourceSchemaFingerprint())
                    .isEqualTo(DescriptorFingerprints.fingerprintOf(noteV1));
            SaveAsCapability htmlCap = rev1Caps.getCapabilitiesList().stream()
                    .filter(c -> c.getCapability().getProviderId().equals("colocated.note.html"))
                    .findFirst()
                    .orElseThrow();
            assertThat(htmlCap.getStatus())
                    .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);

            // Render pinned revision 1 -> HTML rendition "html/rev1" on targetEntry.
            ConversionSpec noteHtmlSpec = ConversionSpec.newBuilder()
                    .setProviderId("colocated.note.html")
                    .setProviderVersion("1.0.0")
                    .setSourceProfile("test.sink.Note")
                    .setMediaType("text/html")
                    .setExtension("html")
                    .setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .build();
            PublishRepositorySinkResponse pubRev1 = sink.publishRepositorySink(
                    PublishRepositorySinkRequest.newBuilder()
                            .setSource(rev1Source)
                            .setConversion(noteHtmlSpec)
                            .setDestination(SinkDestination.newBuilder()
                                    .setAddress(targetEntry)
                                    .setRendition(RenditionDescriptor.newBuilder()
                                            .setName("html")
                                            .setSubKey("rev1")
                                            .setMediaType("text/html")))
                            .build());
            assertThat(pubRev1.getJob().getProvenance().getSourceRevision()).isEqualTo(1L);
            assertThat(pubRev1.getJob().getProvenance().getSchemaFingerprint())
                    .isEqualTo(DescriptorFingerprints.fingerprintOf(noteV1));
            assertThat(readRenditionBytes(targetEntry, "html", "rev1", 0L).toStringUtf8())
                    .isEqualTo("<article><h1>Initial Draft</h1></article>");

            // Render current (revision 2) -> HTML rendition "html/current" on targetEntry.
            SinkSource currentSource = SinkSource.newBuilder()
                    .setRepositoryRevision(RepositoryRevisionSource.newBuilder()
                            .setDocumentPart(DocumentPartSource.newBuilder()
                                    .setAddress(sourceDocAddress)
                                    .setRevision(RevisionSelector.newBuilder().setCurrent(true))
                                    .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                    .setOccurrenceFieldPath("structured_data")))
                    .build();
            PublishRepositorySinkResponse pubCurrent = sink.publishRepositorySink(
                    PublishRepositorySinkRequest.newBuilder()
                            .setSource(currentSource)
                            .setConversion(noteHtmlSpec)
                            .setDestination(SinkDestination.newBuilder()
                                    .setAddress(targetEntry)
                                    .setRendition(RenditionDescriptor.newBuilder()
                                            .setName("html")
                                            .setSubKey("current")
                                            .setMediaType("text/html")))
                            .build());
            assertThat(pubCurrent.getJob().getFrozenSourceRevision()).isEqualTo(2L);
            assertThat(pubCurrent.getJob().getProvenance().getSchemaFingerprint())
                    .isEqualTo(DescriptorFingerprints.fingerprintOf(noteV2));
            assertThat(readRenditionBytes(targetEntry, "html", "current", 0L).toStringUtf8())
                    .isEqualTo("<article><h1>Final Release</h1><p>Approved by architecture board</p></article>");

            // Inspect provenance on targetEntry: "html/rev1" came from sourceDocAddress rev 1 while sourceDocAddress
            // is at rev 2 -> source_stale == true; "html/current" came from rev 2 -> source_stale == false!
            ListRenditionProvenanceResponse targetProv = sink.listRenditionProvenance(
                    ListRenditionProvenanceRequest.newBuilder().setAddress(targetEntry).build());
            assertThat(targetProv.getEntriesList()).hasSize(2);
            assertThat(targetProv.getEntriesList())
                    .filteredOn(e -> e.getRendition().getSubKey().equals("rev1"))
                    .singleElement()
                    .satisfies(e -> {
                        assertThat(e.getProvenance().getSourceRevision()).isEqualTo(1L);
                        assertThat(e.getCurrentSourceRevision()).isEqualTo(2L);
                        assertThat(e.getSourceStale()).isTrue();
                    });
            assertThat(targetProv.getEntriesList())
                    .filteredOn(e -> e.getRendition().getSubKey().equals("current"))
                    .singleElement()
                    .satisfies(e -> {
                        assertThat(e.getProvenance().getSourceRevision()).isEqualTo(2L);
                        assertThat(e.getCurrentSourceRevision()).isEqualTo(2L);
                        assertThat(e.getSourceStale()).isFalse();
                    });
        } finally {
            NOTE_SCHEMA_AVAILABLE.set(true);
            ACTIVE_NOTE_SCHEMA.set(dynamicSchemaDefinition(noteV1));
        }
    }

    @Test
    void enforcesDestinationPreconditionsRetryIdempotencyCancellationAndOversizeGuardrails()
            throws Exception {
        EntryAddress target = newEntry();
        SearchMetadata v1State = SearchMetadata.newBuilder().setTitle("Version One").build();
        ConversionSpec summaryMd = summaryConversionSpec();

        String opId = UUID.randomUUID().toString();
        PublishRepositorySinkRequest initial = PublishRepositorySinkRequest.newBuilder()
                .setOperationId(opId)
                .setSource(processingStateSource(v1State, "run-1", "step-1"))
                .setConversion(summaryMd)
                .setDestination(SinkDestination.newBuilder()
                        .setAddress(target)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("summary")
                                .setMediaType("text/markdown"))
                        .setCreateOnly(true))
                .build();

        PublishRepositorySinkResponse first = sink.publishRepositorySink(initial);
        assertThat(first.getJob().getState()).isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        assertThat(first.getJob().getProvenance().getReplayed()).isFalse();
        assertThat(first.getJob().getProvenance().getDestinationVersion()).isEqualTo(1L);

        // 1. Retrying the exact same operation_id + intent replays the recorded outcome without adding a revision.
        PublishRepositorySinkResponse replay = sink.publishRepositorySink(initial);
        assertThat(replay.getJob().getProvenance().getReplayed()).isTrue();
        assertThat(replay.getJob().getProvenance().getDestinationVersion()).isEqualTo(1L);
        assertThat(revisions(target)).isEqualTo(1);

        // 2. Reusing operation_id with altered intent is refused with ALREADY_EXISTS.
        PublishRepositorySinkRequest altered = initial.toBuilder()
                .setSource(processingStateSource(
                        SearchMetadata.newBuilder().setTitle("Different").build(), "run-1", "step-1"))
                .build();
        StatusRuntimeException reuseConflict = catchThrowableOfType(
                StatusRuntimeException.class, () -> sink.publishRepositorySink(altered));
        assertThat(reuseConflict.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);

        // 3. A distinct operation with create_only == true against an existing entry fails with ABORTED
        // and records conflict == true on the durable job.
        String createConflictOpId = UUID.randomUUID().toString();
        StatusRuntimeException createConflictErr = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> sink.publishRepositorySink(
                        initial.toBuilder().setOperationId(createConflictOpId).build()));
        assertThat(createConflictErr.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
        RepositorySinkJob createConflictJob = sink.getRepositorySinkJob(
                        GetRepositorySinkJobRequest.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setOperationId(createConflictOpId)
                                .build())
                .getJob();
        assertThat(createConflictJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED);
        assertThat(createConflictJob.getFailure().getConflict()).isTrue();
        assertThat(createConflictJob.getFailure().getGrpcCode())
                .isEqualTo(Status.Code.ABORTED.value());
        assertThat(revisions(target)).isEqualTo(1);

        // 4. Stale expected_destination_version fails with ABORTED and records conflict == true without writing a new revision.
        String versionConflictOpId = UUID.randomUUID().toString();
        StatusRuntimeException versionConflictErr = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> sink.publishRepositorySink(
                        PublishRepositorySinkRequest.newBuilder()
                                .setOperationId(versionConflictOpId)
                                .setSource(processingStateSource(v1State, "run-1", "step-2"))
                                .setConversion(summaryMd)
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(target)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("summary")
                                                .setMediaType("text/markdown"))
                                        .setExpectedDestinationVersion(99L))
                                .build()));
        assertThat(versionConflictErr.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
        RepositorySinkJob versionConflictJob = sink.getRepositorySinkJob(
                        GetRepositorySinkJobRequest.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setOperationId(versionConflictOpId)
                                .build())
                .getJob();
        assertThat(versionConflictJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED);
        assertThat(versionConflictJob.getFailure().getConflict()).isTrue();
        assertThat(revisions(target)).isEqualTo(1);

        // 5. Oversize output (provider max_output_bytes == 64 KiB, provider writes 128 KiB) triggers RESOURCE_EXHAUSTED before any bytes commit.
        EntryAddress oversizeTarget = newEntry();
        String oversizeOpId = UUID.randomUUID().toString();
        StatusRuntimeException oversizeErr = catchThrowableOfType(
                StatusRuntimeException.class,
                () -> sink.publishRepositorySink(
                        PublishRepositorySinkRequest.newBuilder()
                                .setOperationId(oversizeOpId)
                                .setSource(processingStateSource(v1State, "run-oversize", "step-oversize"))
                                .setConversion(ConversionSpec.newBuilder()
                                        .setProviderId("colocated.search.oversize")
                                        .setProviderVersion("1.0.0")
                                        .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                                        .setMediaType("application/octet-stream")
                                        .setExtension("bin")
                                        .setOptionsType(Empty.getDescriptor().getFullName())
                                        .setOptionsFingerprint(
                                                DescriptorFingerprints.fingerprintOf(Empty.getDescriptor())))
                                .setDestination(SinkDestination.newBuilder()
                                        .setAddress(oversizeTarget)
                                        .setRendition(RenditionDescriptor.newBuilder()
                                                .setName("huge")
                                                .setMediaType("application/octet-stream")))
                                .build()));
        assertThat(oversizeErr.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        RepositorySinkJob oversizeJob = sink.getRepositorySinkJob(
                        GetRepositorySinkJobRequest.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setOperationId(oversizeOpId)
                                .build())
                .getJob();
        assertThat(oversizeJob.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED);
        assertThat(oversizeJob.getFailure().getGrpcCode())
                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED.value());
        assertThat(revisions(oversizeTarget)).isZero();

        // 6. Async submission + live cancellation stops rendering and commits zero revisions.
        EntryAddress cancelledTarget = newEntry();
        SLOW_RENDER.reset();
        String cancelOpId = UUID.randomUUID().toString();
        PublishRepositorySinkResponse asyncQueued = sink.publishRepositorySink(
                PublishRepositorySinkRequest.newBuilder()
                        .setOperationId(cancelOpId)
                        .setAsync(true)
                        .setSource(processingStateSource(v1State, "run-slow", "step-slow"))
                        .setConversion(ConversionSpec.newBuilder()
                                .setProviderId("colocated.search.slow")
                                .setProviderVersion("1.0.0")
                                .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                                .setMediaType("text/markdown")
                                .setExtension("md")
                                .setOptionsType(Empty.getDescriptor().getFullName())
                                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor())))
                        .setDestination(SinkDestination.newBuilder()
                                .setAddress(cancelledTarget)
                                .setRendition(RenditionDescriptor.newBuilder()
                                        .setName("slow")
                                        .setMediaType("text/markdown")))
                        .build());
        assertThat(asyncQueued.getJob().getOperationId()).isEqualTo(cancelOpId);
        assertThat(SLOW_RENDER.awaitStarted()).isTrue();

        sink.cancelRepositorySinkJob(CancelRepositorySinkJobRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setOperationId(cancelOpId)
                .build());
        assertThat(SLOW_RENDER.awaitObservedCancel()).isTrue();

        RepositorySinkJob terminalCancelled = awaitTerminalJob(cancelOpId);
        assertThat(terminalCancelled.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED);
        assertThat(revisions(cancelledTarget)).isZero();
    }

    @Test
    void reconcilesLostUploadCommitReplyThroughUnresolvedToSucceededWithoutDuplicateRevision() {
        EntryAddress target = newEntry();
        SearchMetadata state = SearchMetadata.newBuilder()
                .setTitle("Reconciled Commit")
                .setBody("Must land exactly once even if transport drops commit reply.")
                .build();
        String opId = UUID.randomUUID().toString();
        PublishRepositorySinkRequest request = PublishRepositorySinkRequest.newBuilder()
                .setOperationId(opId)
                .setSource(processingStateSource(state, "run-reconcile", "step-reconcile"))
                .setConversion(summaryConversionSpec())
                .setDestination(SinkDestination.newBuilder()
                        .setAddress(target)
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("summary")
                                .setMediaType("text/markdown"))
                        .setCreateOnly(true))
                .build();

        // Drop 1 UploadDocument saved reply AFTER server commit, AND fail 2 post-drop GetOperationOutcome
        // lookups (1 during publish failure handling, 1 so we can inspect UNRESOLVED directly before reconciliation!).
        FAULTS.dropCommittedUploadRepliesAndFailSubsequentOutcomes(1, 2);

        StatusRuntimeException uncertainErr = catchThrowableOfType(
                StatusRuntimeException.class, () -> sink.publishRepositorySink(request));
        assertThat(uncertainErr.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(revisions(target)).isEqualTo(1);

        // First GetRepositorySinkJob sees GetOperationOutcome still unavailable -> returns UNRESOLVED job!
        RepositorySinkJob unresolved = sink.getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setOperationId(opId)
                .build()).getJob();
        assertThat(unresolved.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED);
        assertThat(unresolved.getFailure().getRetryable()).isTrue();

        // Next GetRepositorySinkJob reconciles via GetOperationOutcome -> SUCCEEDED with destination_version == 1!
        RepositorySinkJob reconciled = sink.getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                .setAccountId(ACCOUNT)
                .setOperationId(opId)
                .build()).getJob();
        assertThat(reconciled.getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        assertThat(reconciled.getProvenance().getDestinationVersion()).isEqualTo(1L);
        assertThat(revisions(target)).isEqualTo(1);

        // Retrying PublishRepositorySink with the same operation_id replays the reconciled SUCCEEDED outcome.
        PublishRepositorySinkResponse replayed = sink.publishRepositorySink(request);
        assertThat(replayed.getJob().getState())
                .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED);
        assertThat(replayed.getJob().getProvenance().getReplayed()).isTrue();
        assertThat(revisions(target)).isEqualTo(1);
        assertThat(readRenditionBytes(target, "summary", "", 1L).toStringUtf8())
                .isEqualTo("# Reconciled Commit\nMust land exactly once even if transport drops commit reply.\n");
    }

    // -------------------------------------------------------------------------
    // Helpers & test renderer fixtures
    // -------------------------------------------------------------------------

    private static EntryAddress newEntry() {
        return EntryAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setArchive(ARCHIVE)
                .setEntryId("entry-" + UUID.randomUUID())
                .build();
    }

    private static SinkSource processingStateSource(SearchMetadata state, String runId, String stepName) {
        return SinkSource.newBuilder()
                .setProcessingState(ProcessingStateSource.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setRunId(runId)
                        .setStepName(stepName)
                        .setMessageType(SearchMetadata.getDescriptor().getFullName())
                        .setSchemaFingerprint(DescriptorFingerprints.fingerprintOf(SearchMetadata.getDescriptor()))
                        .setDescriptors(DescriptorFingerprints.closure(SearchMetadata.getDescriptor()).toByteString())
                        .setPayload(state.toByteString()))
                .build();
    }

    private static ConversionSpec summaryConversionSpec() {
        return ConversionSpec.newBuilder()
                .setProviderId("colocated.search.summary")
                .setProviderVersion("1.0.0")
                .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .build();
    }

    private static ByteString readRenditionBytes(
            EntryAddress address, String rendition, String subKey, long version) {
        ReadRenditionRequest.Builder req = ReadRenditionRequest.newBuilder()
                .setAddress(address)
                .setRendition(rendition)
                .setSubKey(subKey);
        if (version > 0L) {
            req.setVersion(version);
        }
        List<ByteString> chunks = new ArrayList<>();
        archive.readRendition(req.build()).forEachRemaining(frame -> {
            if (frame.hasData()) {
                chunks.add(frame.getData());
            }
        });
        return ByteString.copyFrom(chunks);
    }

    private static int revisions(EntryAddress address) {
        try {
            return documents.listRevisions(ListRevisionsRequest.newBuilder()
                            .setAddress(ArchiveCoordinates.address(address))
                            .setPageSize(100)
                            .build())
                    .getRevisionsCount();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return 0;
            }
            throw e;
        }
    }

    private static RepositorySinkJob awaitTerminalJob(String operationId) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            RepositorySinkJob job = sink.getRepositorySinkJob(GetRepositorySinkJobRequest.newBuilder()
                            .setAccountId(ACCOUNT)
                            .setOperationId(operationId)
                            .build())
                    .getJob();
            if (job.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED
                    || job.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                    || job.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED
                    || job.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED) {
                return job;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("Timed out waiting for terminal job state on " + operationId);
    }

    private static SaveDocumentResponse saveStructuredNote(
            EntryAddress entry, long expectedRevision, DynamicMessage notePayload) {
        Any packed = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + notePayload.getDescriptorForType().getFullName())
                .setValue(notePayload.toByteString())
                .build();
        Document doc = Document.newBuilder()
                .setDocId(entry.getEntryId())
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(entry.getAccountId())
                        .setDatasourceId(DATASOURCE))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Structured Note " + entry.getEntryId()))
                .setStructuredData(packed)
                .build();
        ExpectedRevision expected = expectedRevision == 0L
                ? ExpectedRevision.newBuilder().setNone(true).build()
                : ExpectedRevision.newBuilder().setRevision(expectedRevision).build();
        return documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .setTarget(DocumentTarget.newBuilder().setAddress(ArchiveCoordinates.address(entry)))
                .setExpected(expected)
                .setDocument(doc)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .setDrive(V2RepositoryFixture.DEFAULT_DRIVE)
                .setOwner(SaveOwner.newBuilder()
                        .setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)))
                .build());
    }

    private static Descriptor buildNoteDescriptor(boolean includeSummary) throws Exception {
        DescriptorProto.Builder msg = DescriptorProto.newBuilder()
                .setName("Note")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("headline")
                        .setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        if (includeSummary) {
            msg.addField(FieldDescriptorProto.newBuilder()
                    .setName("summary")
                    .setNumber(2)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName("test/sink/note.proto")
                .setPackage("test.sink")
                .setSyntax("proto3")
                .addMessageType(msg)
                .build();
        FileDescriptor file = FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
        return file.findMessageTypeByName("Note");
    }

    private static DocumentSchemaAdmission.Definition dynamicSchemaDefinition(Descriptor descriptor) {
        var closure = DescriptorFingerprints.closure(descriptor);
        ByteString bytes = closure.toByteString();
        SchemaCompilationProvenance compilation = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED)
                .setKnownCompiler(SchemaCompilerDetails.newBuilder()
                        .setTool(SchemaToolIdentity.newBuilder().setName("protoc").setVersion("test")))
                .setAdmissionRuntime(BuiltinDocumentSchema.admissionRuntime())
                .build();
        RepositorySchemaAsset metadata = RepositorySchemaAsset.newBuilder()
                .setTypeUrl("type.googleapis.com/" + descriptor.getFullName())
                .setArtifactSha256(RenditionReads.sha256Hex(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder()
                        .setTypeName(descriptor.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(compilation)
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    private static final class RemoteMarkdownRendererService
            extends RendererServiceGrpc.RendererServiceImplBase {
        private final RendererCapability capability;

        private RemoteMarkdownRendererService(RendererCapability capability) {
            this.capability = capability;
        }

        @Override
        public void render(RenderRequest request, StreamObserver<RenderResponse> responseObserver) {
            try {
                SearchMetadata input = SearchMetadata.parseFrom(request.getInput());
                String header = "# " + input.getTitle() + "\n" + input.getBody() + "\n";
                byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
                byte[] body = new byte[LARGE_MARKDOWN_BYTES + headerBytes.length];
                System.arraycopy(headerBytes, 0, body, 0, headerBytes.length);
                for (int i = headerBytes.length; i < body.length; i++) {
                    body[i] = (byte) ((i % 79 == 78) ? '\n' : 'x');
                }
                responseObserver.onNext(RenderResponse.newBuilder()
                        .setCapability(capability)
                        .setContent(ByteString.copyFrom(body))
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
            }
        }
    }

    private static final class ColocatedSearchSummaryRenderer implements RendererProvider {
        @Override
        public RendererCapability capability() {
            return RendererCapability.newBuilder()
                    .setProviderId("colocated.search.summary")
                    .setProviderVersion("1.0.0")
                    .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                    .setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .setMediaType("text/markdown")
                    .setExtension("md")
                    .setLossDescription("Renders title and body into Markdown; omits structural protobuf fields")
                    .setMaxInputBytes(1024 * 1024)
                    .setMaxOutputBytes(2 * 1024 * 1024)
                    .build();
        }

        @Override
        public Descriptor optionsDescriptor() {
            return Empty.getDescriptor();
        }

        @Override
        public void check(Message input, Message options) throws RenderFailure {
            if (!SearchMetadata.getDescriptor().getFullName()
                    .equals(input.getDescriptorForType().getFullName())) {
                throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED, "Requires SearchMetadata");
            }
        }

        @Override
        public void render(
                Message input, Message options, OutputStream output, Cancellation cancellation)
                throws IOException {
            cancellation.check();
            var titleField = input.getDescriptorForType().findFieldByName("title");
            var bodyField = input.getDescriptorForType().findFieldByName("body");
            String title = titleField != null ? String.valueOf(input.getField(titleField)) : "";
            String body = bodyField != null ? String.valueOf(input.getField(bodyField)) : "";
            String text = body.isEmpty() ? ("# " + title + "\n") : ("# " + title + "\n" + body + "\n");
            output.write(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class OversizeSearchRenderer implements RendererProvider {
        @Override
        public RendererCapability capability() {
            return RendererCapability.newBuilder()
                    .setProviderId("colocated.search.oversize")
                    .setProviderVersion("1.0.0")
                    .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                    .setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .setMediaType("application/octet-stream")
                    .setExtension("bin")
                    .setLossDescription("Synthetic oversize test output")
                    .setMaxInputBytes(1024 * 1024)
                    .setMaxOutputBytes(65_536) // 64 KiB cap; render writes 128 KiB -> RESOURCE_EXHAUSTED
                    .build();
        }

        @Override
        public Descriptor optionsDescriptor() {
            return Empty.getDescriptor();
        }

        @Override
        public void check(Message input, Message options) throws RenderFailure {
            if (!SearchMetadata.getDescriptor().getFullName()
                    .equals(input.getDescriptorForType().getFullName())) {
                throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED, "Requires SearchMetadata");
            }
        }

        @Override
        public void render(
                Message input, Message options, OutputStream output, Cancellation cancellation)
                throws IOException {
            byte[] block = new byte[65_536];
            output.write(block);
            output.write(block);
        }
    }

    private static final class SlowRenderController {
        private volatile CountDownLatch started = new CountDownLatch(1);
        private volatile CountDownLatch observedCancel = new CountDownLatch(1);

        void reset() {
            started = new CountDownLatch(1);
            observedCancel = new CountDownLatch(1);
        }

        void markStarted() {
            started.countDown();
        }

        void markObservedCancel() {
            observedCancel.countDown();
        }

        boolean awaitStarted() throws InterruptedException {
            return started.await(5, TimeUnit.SECONDS);
        }

        boolean awaitObservedCancel() throws InterruptedException {
            return observedCancel.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class SlowCancellableSearchRenderer implements RendererProvider {
        private final SlowRenderController controller;

        private SlowCancellableSearchRenderer(SlowRenderController controller) {
            this.controller = controller;
        }

        @Override
        public RendererCapability capability() {
            return RendererCapability.newBuilder()
                    .setProviderId("colocated.search.slow")
                    .setProviderVersion("1.0.0")
                    .setSourceProfile(SearchMetadata.getDescriptor().getFullName())
                    .setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .setMediaType("text/markdown")
                    .setExtension("md")
                    .setLossDescription("Slow cancellable test renderer")
                    .setMaxInputBytes(1024 * 1024)
                    .setMaxOutputBytes(1024 * 1024)
                    .build();
        }

        @Override
        public Descriptor optionsDescriptor() {
            return Empty.getDescriptor();
        }

        @Override
        public void check(Message input, Message options) throws RenderFailure {
            if (!SearchMetadata.getDescriptor().getFullName()
                    .equals(input.getDescriptorForType().getFullName())) {
                throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED, "Requires SearchMetadata");
            }
        }

        @Override
        public void render(
                Message input, Message options, OutputStream output, Cancellation cancellation)
                throws IOException {
            controller.markStarted();
            for (int i = 0; i < 400; i++) {
                if (cancellation.cancelled() || Context.current().isCancelled()) {
                    controller.markObservedCancel();
                    cancellation.check();
                }
                try {
                    Thread.sleep(20L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    controller.markObservedCancel();
                    throw new RenderFailure(RenderFailure.Kind.CANCELLED, "Interrupted", interrupted);
                }
            }
            output.write("# unexpectedly completed\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class DynamicNoteHtmlRenderer implements RendererProvider {
        @Override
        public RendererCapability capability() {
            return RendererCapability.newBuilder()
                    .setProviderId("colocated.note.html")
                    .setProviderVersion("1.0.0")
                    .setSourceProfile("test.sink.Note")
                    .setOptionsType(Empty.getDescriptor().getFullName())
                    .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                    .setMediaType("text/html")
                    .setExtension("html")
                    .setLossDescription("Renders Note headline and optional summary into HTML article; omits protobuf tags")
                    .setMaxInputBytes(1024 * 1024)
                    .setMaxOutputBytes(1024 * 1024)
                    .build();
        }

        @Override
        public Descriptor optionsDescriptor() {
            return Empty.getDescriptor();
        }

        @Override
        public void check(Message input, Message options) throws RenderFailure {
            Descriptor desc = input.getDescriptorForType();
            if (!"test.sink.Note".equals(desc.getFullName())
                    || desc.findFieldByName("headline") == null) {
                throw new RenderFailure(RenderFailure.Kind.UNSUPPORTED, "Requires test.sink.Note");
            }
        }

        @Override
        public void render(
                Message input, Message options, OutputStream output, Cancellation cancellation)
                throws IOException {
            cancellation.check();
            var headlineField = input.getDescriptorForType().findFieldByName("headline");
            var summaryField = input.getDescriptorForType().findFieldByName("summary");
            String headline = headlineField != null ? String.valueOf(input.getField(headlineField)) : "";
            String summary = summaryField != null ? String.valueOf(input.getField(summaryField)) : "";
            String html = summary.isEmpty()
                    ? ("<article><h1>" + headline + "</h1></article>")
                    : ("<article><h1>" + headline + "</h1><p>" + summary + "</p></article>");
            output.write(html.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class FaultInjectingInterceptor implements ClientInterceptor {
        private final AtomicInteger dropUploadReplies = new AtomicInteger();
        private final AtomicInteger failOutcomeCallsAfterDrop = new AtomicInteger();
        private final AtomicBoolean droppedAtLeastOnce = new AtomicBoolean();

        void dropCommittedUploadRepliesAndFailSubsequentOutcomes(int dropCount, int outcomeFailuresAfterDrop) {
            droppedAtLeastOnce.set(false);
            failOutcomeCallsAfterDrop.set(outcomeFailuresAfterDrop);
            dropUploadReplies.set(dropCount);
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            String fullMethod = method.getFullMethodName();
            if (fullMethod.endsWith("/GetOperationOutcome")
                    && droppedAtLeastOnce.get()
                    && getAndDecrementPositive(failOutcomeCallsAfterDrop)) {
                return new ClientCall<>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        responseListener.onClose(
                                Status.UNAVAILABLE.withDescription("Simulated outcome lookup outage"),
                                new Metadata());
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void halfClose() {}

                    @Override
                    public void sendMessage(ReqT message) {}
                };
            }
            if (fullMethod.endsWith("/UploadDocument") && getAndDecrementPositive(dropUploadReplies)) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        super.start(
                                new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                        responseListener) {
                                    private boolean swallowedMessage;

                                    @Override
                                    public void onMessage(RespT message) {
                                        if (message instanceof UploadDocumentResponse resp
                                                && (resp.hasSaved() || resp.hasReplayed())) {
                                            // Server committed! Swallow the terminal saved reply so client sees transport drop.
                                            swallowedMessage = true;
                                            droppedAtLeastOnce.set(true);
                                            return;
                                        }
                                        super.onMessage(message);
                                    }

                                    @Override
                                    public void onClose(Status status, Metadata trailers) {
                                        if (swallowedMessage && status.isOk()) {
                                            super.onClose(
                                                    Status.UNAVAILABLE.withDescription(
                                                            "Simulated dropped UploadDocument commit reply"),
                                                    trailers);
                                        } else {
                                            super.onClose(status, trailers);
                                        }
                                    }
                                },
                                headers);
                    }
                };
            }
            return next.newCall(method, callOptions);
        }

        private static boolean getAndDecrementPositive(AtomicInteger counter) {
            while (true) {
                int current = counter.get();
                if (current <= 0) {
                    return false;
                }
                if (counter.compareAndSet(current, current - 1)) {
                    return true;
                }
            }
        }
    }
}
