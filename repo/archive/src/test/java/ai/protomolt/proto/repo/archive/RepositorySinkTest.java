package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.renderer.RendererRegistry;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.ProcessingStateSource;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.SaveAsCapability;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.sink.v1.SourcePartDigest;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pure unit tests for {@link RepositorySinkKeys}, {@link RepositorySinkProvenanceCodec},
 * {@link RepositorySinkRenderers}, {@link InMemoryRepositorySinkJobStore}, and in-process
 * {@link RepositorySink} validation and capability discovery.
 */
class RepositorySinkTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-11-14T00:00:00Z"), ZoneOffset.UTC);

    private ManagedChannel dummyChannel;

    @BeforeEach
    void setUp() {
        dummyChannel = InProcessChannelBuilder.forName("sink-unit-unused").directExecutor().build();
    }

    @AfterEach
    void tearDown() {
        dummyChannel.shutdownNow();
    }

    @Test
    void intentHashIgnoresOperationIdAsyncAndMetadataOrderWhileBindingEverySemanticField() {
        PublishRepositorySinkRequest first = samplePublishRequest("acct-1")
                .toBuilder()
                .setOperationId("op-1")
                .setAsync(false)
                .setDestination(sampleDestination("acct-1").toBuilder()
                        .putMetadata("z", "last")
                        .putMetadata("a", "first")
                        .build())
                .build();
        PublishRepositorySinkRequest reordered = samplePublishRequest("acct-1")
                .toBuilder()
                .setOperationId("op-2")
                .setAsync(true)
                .setDestination(sampleDestination("acct-1").toBuilder()
                        .putMetadata("a", "first")
                        .putMetadata("z", "last")
                        .build())
                .build();

        String sha = RepositorySinkKeys.intentSha256(first);
        assertThat(RepositorySinkKeys.intentSha256(reordered)).isEqualTo(sha);

        String derivedId = RepositorySinkKeys.resolveOperationId(
                first.toBuilder().clearOperationId().build(), "acct-1", sha);
        assertThat(derivedId).isEqualTo(RepositorySinkKeys.resolveOperationId(
                reordered.toBuilder().clearOperationId().build(), "acct-1", sha));

        // Changing expected_destination_version, create_only, or conversion alters the intent hash.
        PublishRepositorySinkRequest pinnedVersion = first.toBuilder()
                .setDestination(first.getDestination().toBuilder().setExpectedDestinationVersion(2L))
                .build();
        assertThat(RepositorySinkKeys.intentSha256(pinnedVersion)).isNotEqualTo(sha);

        PublishRepositorySinkRequest createOnly = first.toBuilder()
                .setDestination(first.getDestination().toBuilder().setCreateOnly(true))
                .build();
        assertThat(RepositorySinkKeys.intentSha256(createOnly)).isNotEqualTo(sha);
    }

    @Test
    void provenanceAndRetainedDescriptorsRoundTripOnBlobAndParsedSearchMetadata() {
        ByteString closure = DescriptorFingerprints.closure(SearchMetadata.getDescriptor()).toByteString();
        RenditionProvenance provenance = RenditionProvenance.newBuilder()
                .setOperationId("op-prov-1")
                .setSourceRevision(0L)
                .addSourceDigests(SourcePartDigest.newBuilder()
                        .setIdentity("step:extract")
                        .setSha256("a".repeat(64))
                        .setSizeBytes(42L))
                .setSchemaType(SearchMetadata.getDescriptor().getFullName())
                .setSchemaFingerprint(DescriptorFingerprints.fingerprintOf(SearchMetadata.getDescriptor()))
                .setRetainedDescriptorSha256(RenditionReads.sha256Hex(closure))
                .setAuthoritativeProtobuf(true)
                .setOutputMediaType(RepositorySink.PROTOBUF_MEDIA_TYPE)
                .setBytesRendered(42L)
                .setBytesCommitted(42L)
                .setOutputSha256("b".repeat(64))
                .setDestinationAddress(EntryAddress.newBuilder()
                        .setAccountId("acct-1")
                        .setArchive("reports")
                        .setEntryId("e-1"))
                .setDestinationRendition(RenditionDescriptor.newBuilder()
                        .setName("state")
                        .setMediaType(RepositorySink.PROTOBUF_MEDIA_TYPE))
                .setDestinationVersion(1L)
                .setCommittedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .build();

        Blob blob = Blob.newBuilder()
                .setBlobId("state")
                .setSizeBytes(42L)
                .setMetadata(Struct.newBuilder().putAllFields(
                        RepositorySinkProvenanceCodec.blobMetadataFields(provenance, closure)))
                .setWritten(BlobWrite.newBuilder()
                        .setRevision(7L)
                        .setCommittedAt(Timestamp.newBuilder().setSeconds(1_700_000_999L))
                        .setStep(WriteProvenance.newBuilder().setModuleId("sink")))
                .build();

        RenditionProvenance decodedBlobProv =
                RepositorySinkProvenanceCodec.readBlobProvenance(blob).orElseThrow();
        assertThat(decodedBlobProv.getDestinationVersion()).isEqualTo(7L);
        assertThat(decodedBlobProv.getCommittedAt().getSeconds()).isEqualTo(1_700_000_999L);
        assertThat(decodedBlobProv.getAuthoritativeProtobuf()).isTrue();

        RepositorySinkProvenanceCodec.RetainedBlobSchema retained =
                RepositorySinkProvenanceCodec.readBlobRetainedSchema(blob, 1024 * 1024).orElseThrow();
        assertThat(retained.messageType()).isEqualTo(SearchMetadata.getDescriptor().getFullName());
        assertThat(retained.descriptorSetBytes()).isEqualTo(closure);

        SearchMetadata search = SearchMetadata.newBuilder()
                .putAllMetadata(RepositorySinkProvenanceCodec.parsedSearchMetadata(provenance, closure))
                .build();
        RenditionProvenance decodedParsedProv = RepositorySinkProvenanceCodec.readParsedProvenance(
                        search, 5L, Timestamp.newBuilder().setSeconds(1_700_000_555L).build(), 42L)
                .orElseThrow();
        assertThat(decodedParsedProv.getDestinationVersion()).isEqualTo(5L);
        assertThat(decodedParsedProv.getCommittedAt().getSeconds()).isEqualTo(1_700_000_555L);
    }

    @Test
    void listSaveAsCapabilitiesEvaluatesProvidersWithoutTouchingStorage() {
        RendererProvider markdownProvider = markdownSearchMetadataProvider("md.search.v1");
        RendererProvider docOnlyProvider = documentOnlyProvider("md.doc.v1");
        RepositorySinkRenderers renderers = RepositorySinkRenderers.of(
                List.of(markdownProvider, docOnlyProvider),
                Set.of("md.search.v1", "md.doc.v1"));

        ArchiveBindings bindings = (acct, name) -> Optional.of(new ArchiveBinding(
                acct, name, "ds-archive", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(dummyChannel, bindings, FIXED_CLOCK);
        try (RepositorySink sink = new RepositorySink(
                archive,
                new RenditionUploads(archive, dummyChannel),
                new RenditionReads(archive),
                renderers,
                FIXED_CLOCK)) {
            SearchMetadata payload = SearchMetadata.newBuilder()
                    .setTitle("Quarterly Metrics")
                    .setBody("All numbers verified")
                    .build();
            ListSaveAsCapabilitiesResponse response = sink.listSaveAsCapabilities(
                    ListSaveAsCapabilitiesRequest.newBuilder()
                            .setSource(processingStateSource("acct-1", payload))
                            .build());

            assertThat(response.getResolvedSourceRevision()).isZero();
            assertThat(response.getSourceSchemaType())
                    .isEqualTo(SearchMetadata.getDescriptor().getFullName());
            assertThat(response.getCapabilitiesList()).hasSize(2);

            Map<String, SaveAsCapability> byId = Map.of(
                    response.getCapabilities(0).getCapability().getProviderId(),
                    response.getCapabilities(0),
                    response.getCapabilities(1).getCapability().getProviderId(),
                    response.getCapabilities(1));

            SaveAsCapability compatible = byId.get("md.search.v1");
            assertThat(compatible.getStatus())
                    .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);
            assertThat(compatible.getIncompatibilityReason()).isEmpty();
            assertThat(ClosedDescriptorSet.load(
                            compatible.getOptionsDescriptors(),
                            new ClosedDescriptorSet.Limits(1024 * 1024, 32, 128, 16)))
                    .isNotEmpty();

            SaveAsCapability incompatible = byId.get("md.doc.v1");
            assertThat(incompatible.getStatus())
                    .isEqualTo(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE);
            assertThat(incompatible.getIncompatibilityReason()).contains("Requires Document");
        }
    }

    @Test
    void terminalJobLookupPropagatesOutcomeUnavailabilityWithoutChangingTheReceipt() {
        ArchiveBindings bindings = (acct, name) -> Optional.of(new ArchiveBinding(
                acct, name, "ds-archive", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(dummyChannel, bindings, FIXED_CLOCK);
        var store = new InMemoryRepositorySinkJobStore();
        var failed = new RepositorySinkJobStore.StoredJob(RepositorySinkJob.newBuilder()
                .setAccountId("acct-1").setOperationId("unavailable-outcome")
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED).build(),
                samplePublishRequest("acct-1"), null);
        store.insertOrGet(failed);
        try (RepositorySink sink = new RepositorySink(
                "reader", archive, new RenditionUploads(archive, dummyChannel),
                new RenditionReads(archive), RepositorySinkRenderers::none, store,
                RepositorySinkAuthorizer.PASS_THROUGH, () -> RepositorySink.LeaseConfig.DEFAULT,
                FIXED_CLOCK)) {
            assertThatThrownBy(() -> sink.getJob(GetRepositorySinkJobRequest.newBuilder()
                            .setAccountId("acct-1").setOperationId("unavailable-outcome").build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
            assertThat(store.find("acct-1", "unavailable-outcome").orElseThrow()).isEqualTo(failed);
        }
    }

    @Test
    void publishRefusesAccountMismatchAndReservedMetadataBeforeTouchingDownstreamChannel() {
        ArchiveBindings bindings = (acct, name) -> Optional.of(new ArchiveBinding(
                acct, name, "ds-archive", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(dummyChannel, bindings, FIXED_CLOCK);
        try (RepositorySink sink = new RepositorySink(
                archive,
                new RenditionUploads(archive, dummyChannel),
                new RenditionReads(archive),
                RepositorySinkRenderers.none(),
                FIXED_CLOCK)) {
            PublishRepositorySinkRequest crossAccount = PublishRepositorySinkRequest.newBuilder()
                    .setSource(processingStateSource("acct-source", SearchMetadata.getDefaultInstance()))
                    .setDestination(sampleDestination("acct-dest"))
                    .build();
            assertThatThrownBy(() -> sink.publish(crossAccount))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(e.getStatus().getDescription()).contains("acct-source");
                    });

            PublishRepositorySinkRequest reservedMeta = samplePublishRequest("acct-1").toBuilder()
                    .setDestination(sampleDestination("acct-1").toBuilder()
                            .putMetadata("archive.classification", "spoofed"))
                    .build();
            assertThatThrownBy(() -> sink.publish(reservedMeta))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

            PublishRepositorySinkRequest convertedIntoParsed = samplePublishRequest("acct-1").toBuilder()
                    .setConversion(ConversionSpec.newBuilder()
                            .setProviderId("md.search.v1")
                            .setProviderVersion("1.0.0")
                            .setSourceProfile("search-metadata")
                            .setMediaType("text/markdown")
                            .setExtension("md")
                            .setOptionsType(Empty.getDescriptor().getFullName())
                            .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor())))
                    .setDestination(sampleDestination("acct-1").toBuilder()
                            .setRendition(RenditionDescriptor.newBuilder()
                                    .setName("parsed")
                                    .setMediaType("text/markdown")))
                    .build();
            assertThatThrownBy(() -> sink.publish(convertedIntoParsed))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        }
    }

    @Test
    void inMemoryJobStoreInsertOrGetPreservesFirstAdmittedRecord() {
        InMemoryRepositorySinkJobStore store = new InMemoryRepositorySinkJobStore();
        PublishRepositorySinkRequest req = samplePublishRequest("acct-1");
        RepositorySinkJob queued = RepositorySinkJob.newBuilder()
                .setAccountId("acct-1")
                .setOperationId("op-1")
                .setIntentSha256("a".repeat(64))
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED)
                .setDestinationAddress(req.getDestination().getAddress())
                .setDestinationRendition(req.getDestination().getRendition())
                .setCreatedAt(Timestamp.newBuilder().setSeconds(100))
                .setUpdatedAt(Timestamp.newBuilder().setSeconds(100))
                .build();
        RepositorySinkJobStore.StoredJob first =
                new RepositorySinkJobStore.StoredJob(queued, req, null);
        RepositorySinkJobStore.StoredJob duplicate = new RepositorySinkJobStore.StoredJob(
                queued.toBuilder().setIntentSha256("b".repeat(64)).build(), req, null);

        assertThat(store.insertOrGet(first)).isSameAs(first);
        assertThat(store.insertOrGet(duplicate)).isSameAs(first);
        assertThat(store.find("acct-1", "op-1")).contains(first);

        // RendererRegistry wrapper auto-populates google.protobuf.Empty descriptor when omitted.
        RendererProvider md = markdownSearchMetadataProvider("md.search.v1");
        RepositorySinkRenderers wrapped = RepositorySinkRenderers.of(
                new RendererRegistry(List.of(md), Set.of("md.search.v1")), Map.of());
        assertThat(wrapped.optionsDescriptor("md.search.v1")).contains(Empty.getDescriptor());
    }

    @Test
    void sharedJobStoreEnforcesLeaseEpochsAndPreventsCrossInstanceOrphanClobbering()
            throws Exception {
        InMemoryRepositorySinkJobStore sharedStore = new InMemoryRepositorySinkJobStore();
        AtomicReference<Instant> nowRef =
                new AtomicReference<>(Instant.parse("2026-11-14T00:00:00Z"));
        Clock sharedClock = new Clock() {
            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return nowRef.get();
            }
        };

        CountDownLatch renderStarted = new CountDownLatch(1);
        CountDownLatch releaseRender = new CountDownLatch(1);
        RendererProvider blockingMarkdown = blockingMarkdownProvider(
                "md.blocking.v1", renderStarted, releaseRender);
        RepositorySinkRenderers renderers = RepositorySinkRenderers.of(
                List.of(blockingMarkdown), Set.of("md.blocking.v1"));

        ArchiveBindings bindings = (acct, name) -> Optional.of(new ArchiveBinding(
                acct, name, "ds-archive", "", VersioningPolicy.VERSIONING_POLICY_RETAINED));
        DocumentArchive archive = new DocumentArchive(dummyChannel, bindings, sharedClock);
        RepositorySink.LeaseConfig fastLease = new RepositorySink.LeaseConfig(
                Duration.ofSeconds(5),
                Duration.ofMillis(20),
                Duration.ofMillis(500));

        try (RepositorySink instanceA = new RepositorySink(
                        "owner-A",
                        archive,
                        new RenditionUploads(archive, dummyChannel),
                        new RenditionReads(archive),
                        () -> renderers,
                        sharedStore,
                        RepositorySinkAuthorizer.PASS_THROUGH,
                        () -> fastLease,
                        sharedClock);
                RepositorySink instanceB = new RepositorySink(
                        "owner-B",
                        archive,
                        new RenditionUploads(archive, dummyChannel),
                        new RenditionReads(archive),
                        () -> renderers,
                        sharedStore,
                        RepositorySinkAuthorizer.PASS_THROUGH,
                        () -> fastLease,
                        sharedClock)) {
            PublishRepositorySinkRequest asyncReq = PublishRepositorySinkRequest.newBuilder()
                    .setOperationId("shared-op-1")
                    .setAsync(true)
                    .setSource(processingStateSource(
                            "acct-1", SearchMetadata.newBuilder().setTitle("Live").build()))
                    .setConversion(ConversionSpec.newBuilder()
                            .setProviderId("md.blocking.v1")
                            .setProviderVersion("1.0.0")
                            .setSourceProfile("search-metadata")
                            .setMediaType("text/markdown")
                            .setExtension("md")
                            .setOptionsType(Empty.getDescriptor().getFullName())
                            .setOptionsFingerprint(
                                    DescriptorFingerprints.fingerprintOf(Empty.getDescriptor())))
                    .setDestination(SinkDestination.newBuilder()
                            .setAddress(EntryAddress.newBuilder()
                                    .setAccountId("acct-1")
                                    .setArchive("reports")
                                    .setEntryId("entry-shared-1"))
                            .setRendition(RenditionDescriptor.newBuilder()
                                    .setName("summary_md")
                                    .setMediaType("text/markdown")))
                    .build();

            instanceA.publish(asyncReq);
            assertThat(renderStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // Polling GetRepositorySinkJob on instanceB while instanceA holds a live lease MUST
            // return RUNNING without calling GetOperationOutcome on dummyChannel or marking FAILED.
            RepositorySinkJob polledFromB = instanceB.getJob(GetRepositorySinkJobRequest.newBuilder()
                            .setAccountId("acct-1")
                            .setOperationId("shared-op-1")
                            .build())
                    .getJob();
            assertThat(polledFromB.getState())
                    .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING);
            RepositorySinkJobStore.StoredJob rawWhileRunning =
                    sharedStore.find("acct-1", "shared-op-1").orElseThrow();
            assertThat(rawWhileRunning.lease().ownerId()).isEqualTo("owner-A");
            assertThat(rawWhileRunning.lease().leaseEpoch()).isEqualTo(1L);

            // Calling CancelRepositorySinkJob on instanceB records durable cancel_requested;
            // instanceA's heartbeat observes it, interrupts rendering before commit, and settles
            // the job to CANCELLED without touching the downstream upload channel.
            CancelRepositorySinkJobResponse cancelResp =
                    instanceB.cancelJob(CancelRepositorySinkJobRequest.newBuilder()
                            .setAccountId("acct-1")
                            .setOperationId("shared-op-1")
                            .setReason("cancelled-from-peer-B")
                            .build());
            assertThat(cancelResp.getJob().getState())
                    .isEqualTo(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED);
            assertThat(cancelResp.getJob().getBytesCommitted()).isZero();
            assertThat(cancelResp.getJob().getFailure().getMessage())
                    .isEqualTo("cancelled-from-peer-B");

            // Verify stale owner with epoch 1 is fenced out when lease expires and epoch 2 is claimed.
            PublishRepositorySinkRequest secondReq =
                    samplePublishRequest("acct-1").toBuilder().setOperationId("op-fence-1").build();
            Instant t0 = nowRef.get();
            RepositorySinkJob runningFenceJob = RepositorySinkJob.newBuilder()
                    .setAccountId("acct-1")
                    .setOperationId("op-fence-1")
                    .setIntentSha256("c".repeat(64))
                    .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING)
                    .setDestinationAddress(secondReq.getDestination().getAddress())
                    .setDestinationRendition(secondReq.getDestination().getRendition())
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(t0.getEpochSecond()))
                    .setUpdatedAt(Timestamp.newBuilder().setSeconds(t0.getEpochSecond()))
                    .build();
            RepositorySinkJobStore.StoredJob epoch1Record = new RepositorySinkJobStore.StoredJob(
                    runningFenceJob,
                    secondReq,
                    null,
                    new RepositorySinkJobStore.JobLease("owner-A", 1L, t0.plusSeconds(5)),
                    false,
                    "");
            sharedStore.insertOrGet(epoch1Record);

            // Before expiry, owner-B cannot claim the active lease.
            assertThat(sharedStore.tryClaimExpiredOrUnresolved(
                            "acct-1", "op-fence-1", 1L, "owner-B", t0.plusSeconds(2), t0.plusSeconds(7)))
                    .isEmpty();

            // Advance past expiration: owner-B claims epoch 2, fencing out stale owner-A (epoch 1).
            Instant expiredNow = t0.plusSeconds(6);
            RepositorySinkJobStore.StoredJob claimedEpoch2 = sharedStore
                    .tryClaimExpiredOrUnresolved(
                            "acct-1",
                            "op-fence-1",
                            1L,
                            "owner-B",
                            expiredNow,
                            expiredNow.plusSeconds(5))
                    .orElseThrow();
            assertThat(claimedEpoch2.lease().ownerId()).isEqualTo("owner-B");
            assertThat(claimedEpoch2.lease().leaseEpoch()).isEqualTo(2L);

            assertThat(sharedStore.renewLease(
                            "acct-1", "op-fence-1", "owner-A", 1L, expiredNow.plusSeconds(10)))
                    .isEmpty();
            assertThat(sharedStore
                            .prepareCommit(epoch1Record, "owner-A", 1L)
                            .status())
                    .isEqualTo(RepositorySinkJobStore.PreCommitStatus.LEASE_LOST);
            assertThat(sharedStore.transitionOwned(epoch1Record, "owner-A", 1L)).isFalse();
        }
    }

    private static PublishRepositorySinkRequest samplePublishRequest(String accountId) {
        SearchMetadata state = SearchMetadata.newBuilder().setTitle("Summary").build();
        return PublishRepositorySinkRequest.newBuilder()
                .setSource(processingStateSource(accountId, state))
                .setDestination(sampleDestination(accountId))
                .build();
    }

    private static SinkSource processingStateSource(String accountId, Message payload) {
        Descriptor desc = payload.getDescriptorForType();
        return SinkSource.newBuilder()
                .setProcessingState(ProcessingStateSource.newBuilder()
                        .setAccountId(accountId)
                        .setRunId("run-1")
                        .setStepName("step-1")
                        .setMessageType(desc.getFullName())
                        .setSchemaFingerprint(DescriptorFingerprints.fingerprintOf(desc))
                        .setDescriptors(DescriptorFingerprints.closure(desc).toByteString())
                        .setPayload(payload.toByteString()))
                .build();
    }

    private static SinkDestination sampleDestination(String accountId) {
        return SinkDestination.newBuilder()
                .setAddress(EntryAddress.newBuilder()
                        .setAccountId(accountId)
                        .setArchive("reports")
                        .setEntryId("entry-1"))
                .setRendition(RenditionDescriptor.newBuilder()
                        .setName("state")
                        .setMediaType(RepositorySink.PROTOBUF_MEDIA_TYPE))
                .build();
    }

    private static RendererProvider markdownSearchMetadataProvider(String providerId) {
        RendererCapability capability = RendererCapability.newBuilder()
                .setProviderId(providerId)
                .setProviderVersion("1.0.0")
                .setSourceProfile("search-metadata")
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .setLossDescription("Omits non-textual search metadata fields and timestamps")
                .setMaxInputBytes(1024 * 1024)
                .setMaxOutputBytes(2 * 1024 * 1024)
                .build();
        return new RendererProvider() {
            @Override
            public RendererCapability capability() {
                return capability;
            }

            @Override
            public Descriptor optionsDescriptor() {
                return Empty.getDescriptor();
            }

            @Override
            public void check(Message input, Message options) throws RenderFailure {
                if (!SearchMetadata.getDescriptor().getFullName()
                        .equals(input.getDescriptorForType().getFullName())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED, "Requires SearchMetadata");
                }
            }

            @Override
            public void render(
                    Message input, Message options, OutputStream output, Cancellation cancellation)
                    throws java.io.IOException {
                cancellation.check();
                String title = (String) input.getField(
                        input.getDescriptorForType().findFieldByName("title"));
                output.write(("# " + title + "\n").getBytes(StandardCharsets.UTF_8));
            }
        };
    }

    private static RendererProvider documentOnlyProvider(String providerId) {
        RendererCapability capability = RendererCapability.newBuilder()
                .setProviderId(providerId)
                .setProviderVersion("1.0.0")
                .setSourceProfile("document-full")
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .setLossDescription("Projects document title only")
                .setMaxInputBytes(1024 * 1024)
                .setMaxOutputBytes(1024 * 1024)
                .build();
        return new RendererProvider() {
            @Override
            public RendererCapability capability() {
                return capability;
            }

            @Override
            public Descriptor optionsDescriptor() {
                return Empty.getDescriptor();
            }

            @Override
            public void check(Message input, Message options) throws RenderFailure {
                if (!Document.getDescriptor().getFullName()
                        .equals(input.getDescriptorForType().getFullName())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED, "Requires Document");
                }
            }

            @Override
            public void render(
                    Message input, Message options, OutputStream output, Cancellation cancellation)
                    throws java.io.IOException {
                output.write("doc".getBytes(StandardCharsets.UTF_8));
            }
        };
    }

    private static RendererProvider blockingMarkdownProvider(
            String providerId, CountDownLatch started, CountDownLatch release) {
        RendererCapability capability = RendererCapability.newBuilder()
                .setProviderId(providerId)
                .setProviderVersion("1.0.0")
                .setSourceProfile("search-metadata")
                .setMediaType("text/markdown")
                .setExtension("md")
                .setOptionsType(Empty.getDescriptor().getFullName())
                .setOptionsFingerprint(DescriptorFingerprints.fingerprintOf(Empty.getDescriptor()))
                .setLossDescription("Omits non-textual search metadata fields")
                .setMaxInputBytes(1024 * 1024)
                .setMaxOutputBytes(1024 * 1024)
                .build();
        return new RendererProvider() {
            @Override
            public RendererCapability capability() {
                return capability;
            }

            @Override
            public Descriptor optionsDescriptor() {
                return Empty.getDescriptor();
            }

            @Override
            public void check(Message input, Message options) throws RenderFailure {
                if (!SearchMetadata.getDescriptor().getFullName()
                        .equals(input.getDescriptorForType().getFullName())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED, "Requires SearchMetadata");
                }
            }

            @Override
            public void render(
                    Message input, Message options, OutputStream output, Cancellation cancellation)
                    throws java.io.IOException, RenderFailure {
                started.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new RenderFailure(RenderFailure.Kind.LIMIT, "Render latch timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RenderFailure(
                            RenderFailure.Kind.CANCELLED, "Interrupted", interrupted);
                }
                cancellation.check();
                output.write("# live\n".getBytes(StandardCharsets.UTF_8));
            }
        };
    }
}
