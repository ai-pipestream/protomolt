package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.renderer.RendererRegistry;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.sink.v1.ArchiveRenditionSource;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.DocumentPartSource;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.ProcessingStateSource;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenanceEntry;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.SaveAsCapability;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.sink.v1.SourcePartDigest;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceKeyType;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceMapKey;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetManifestResponse;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorRequest;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorResponse;
import ai.protomolt.proto.repo.v2.OpaqueOccurrence;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSchemaBinding;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.ResolvedOccurrence;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveRejection;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Explicit repository sink coordinating capability discovery, optional bounded conversion via
 * {@link RendererRegistry}, streamed rendition publication through {@link RenditionUploads} and
 * {@link DocumentArchive}, provenance inspection, and durable job lifecycle management.
 *
 * <p>Conversion and streamed upload execute strictly outside any catalog database transaction.
 * Source reads decode exclusively from closed retained {@code FileDescriptorSet} bytes without
 * consulting any live schema registry.
 */
public final class RepositorySink implements AutoCloseable {
    /** Default protobuf media type stamped on direct protobuf publications with no explicit MIME. */
    public static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";

    /**
     * Durable ownership lease, background renewal, and cross-instance cancellation wait parameters
     * for {@link RepositorySink} publication jobs.
     *
     * @param leaseDuration validity window of an acquired or renewed job lease
     * @param heartbeatInterval interval between background lease renewals while a job executes
     * @param cancelSettleWait bounded wait for an active owner to settle after cancellation is
     *     requested
     */
    public record LeaseConfig(
            Duration leaseDuration,
            Duration heartbeatInterval,
            Duration cancelSettleWait) {
        /** Production default lease configuration. */
        public static final LeaseConfig DEFAULT = new LeaseConfig(
                Duration.ofSeconds(15),
                Duration.ofMillis(250),
                Duration.ofSeconds(2));

        /** Validates lease timing parameters. */
        public LeaseConfig {
            Objects.requireNonNull(leaseDuration, "leaseDuration");
            Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
            Objects.requireNonNull(cancelSettleWait, "cancelSettleWait");
            if (leaseDuration.isZero() || leaseDuration.isNegative()) {
                throw new IllegalArgumentException("leaseDuration must be > 0");
            }
            if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
                throw new IllegalArgumentException("heartbeatInterval must be > 0");
            }
            if (cancelSettleWait.isNegative()) {
                throw new IllegalArgumentException("cancelSettleWait must be >= 0");
            }
        }
    }

    private static final int MAX_DESCRIPTOR_BYTES = 16 * 1024 * 1024;
    private static final int MAX_DESCRIPTOR_FILES = 512;
    private static final int MAX_DESCRIPTOR_DEPENDENCIES = 4096;
    private static final int MAX_DESCRIPTOR_IMPORT_DEPTH = 64;
    private static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(
                    MAX_DESCRIPTOR_BYTES,
                    MAX_DESCRIPTOR_FILES,
                    MAX_DESCRIPTOR_DEPENDENCIES,
                    MAX_DESCRIPTOR_IMPORT_DEPTH);

    private static final int MAX_SOURCE_PAYLOAD_BYTES = 64 * 1024 * 1024;
    private static final long MAX_WIRE_VALUES = 2_000_000L;
    private static final int MAX_WIRE_DEPTH = 64;
    private static final int MAX_OCCURRENCE_STEPS = 128;
    private static final int STREAM_READ_BUFFER_BYTES = 8192;
    private static final long UPLOAD_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_FAILURE_MESSAGE_CHARS = 2000;
    private static final int MAX_INCOMPATIBILITY_REASON_CHARS = 1000;
    private static final String ANY_FULL_NAME = "google.protobuf.Any";
    private static final String ENTRY_UUID_KIND = "repository-sink.entry-uuid";

    private final DocumentArchive archive;
    private final RenditionUploads uploads;
    private final RenditionReads reads;
    private final Supplier<RepositorySinkRenderers> renderers;
    private final RepositorySinkAuthorizer authorizer;
    private final Clock clock;
    private final RepositorySinkLeaseCoordinator leases;
    private final ExecutorService asyncExecutor;

    /**
     * Constructs a {@link RepositorySink} with an in-memory job store and pass-through authorizer.
     *
     * @param archive the archive over v2 documents
     * @param uploads flow-controlled rendition upload translator
     * @param reads rendition read planner and verifier
     * @param renderers enabled bounded renderer capabilities
     * @param clock server clock stamping job and provenance timestamps
     */
    public RepositorySink(
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            RepositorySinkRenderers renderers,
            Clock clock) {
        this(
                archive,
                uploads,
                reads,
                renderers,
                new InMemoryRepositorySinkJobStore(),
                RepositorySinkAuthorizer.PASS_THROUGH,
                clock);
    }

    /**
     * Constructs a {@link RepositorySink} with explicit durable job storage and authorization
     * hooks.
     *
     * @param archive the archive over v2 documents
     * @param uploads flow-controlled rendition upload translator
     * @param reads rendition read planner and verifier
     * @param renderers enabled bounded renderer capabilities
     * @param jobStore durable job persistence store
     * @param authorizer caller account and pre-commit writer authorization hooks
     * @param clock server clock stamping job and provenance timestamps
     */
    public RepositorySink(
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            RepositorySinkRenderers renderers,
            RepositorySinkJobStore jobStore,
            RepositorySinkAuthorizer authorizer,
            Clock clock) {
        this(
                archive,
                uploads,
                reads,
                () -> renderers,
                jobStore,
                authorizer,
                clock);
        Objects.requireNonNull(renderers, "renderers");
    }

    /**
     * Constructs a {@link RepositorySink} resolving enabled renderer capabilities dynamically on
     * each operation, with explicit durable job storage and authorization hooks.
     *
     * @param archive the archive over v2 documents
     * @param uploads flow-controlled rendition upload translator
     * @param reads rendition read planner and verifier
     * @param renderers supplier of the host's enabled bounded renderer capabilities
     * @param jobStore durable job persistence store
     * @param authorizer caller account and pre-commit writer authorization hooks
     * @param clock server clock stamping job and provenance timestamps
     */
    public RepositorySink(
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            Supplier<RepositorySinkRenderers> renderers,
            RepositorySinkJobStore jobStore,
            RepositorySinkAuthorizer authorizer,
            Clock clock) {
        this(
                archive,
                uploads,
                reads,
                renderers,
                jobStore,
                authorizer,
                () -> LeaseConfig.DEFAULT,
                clock);
    }

    /**
     * Constructs a {@link RepositorySink} resolving enabled renderer capabilities and lease timing
     * dynamically on each operation, with explicit durable job storage and authorization hooks.
     *
     * @param archive the archive over v2 documents
     * @param uploads flow-controlled rendition upload translator
     * @param reads rendition read planner and verifier
     * @param renderers supplier of the host's enabled bounded renderer capabilities
     * @param jobStore durable job persistence store
     * @param authorizer caller account and pre-commit writer authorization hooks
     * @param leaseConfig supplier of durable lease duration, heartbeat, and settle parameters
     * @param clock server clock stamping job and provenance timestamps
     */
    public RepositorySink(
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            Supplier<RepositorySinkRenderers> renderers,
            RepositorySinkJobStore jobStore,
            RepositorySinkAuthorizer authorizer,
            Supplier<LeaseConfig> leaseConfig,
            Clock clock) {
        this(
                UUID.randomUUID().toString(),
                archive,
                uploads,
                reads,
                renderers,
                jobStore,
                authorizer,
                leaseConfig,
                clock);
    }

    RepositorySink(
            String instanceId,
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            Supplier<RepositorySinkRenderers> renderers,
            RepositorySinkJobStore jobStore,
            RepositorySinkAuthorizer authorizer,
            Supplier<LeaseConfig> leaseConfig,
            Clock clock) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.renderers = Objects.requireNonNull(renderers, "renderers");
        Objects.requireNonNull(jobStore, "jobStore");
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        Objects.requireNonNull(leaseConfig, "leaseConfig");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leases = new RepositorySinkLeaseCoordinator(instanceId, jobStore, leaseConfig, clock);
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    String instanceId() {
        return leases.instanceId();
    }

    /**
     * Inspects an authorized source against all enabled renderer capabilities without mutating
     * repository state or authorizing writes.
     *
     * @param request capability discovery request
     * @return capability compatibility verdicts in deterministic provider order
     */
    public ListSaveAsCapabilitiesResponse listSaveAsCapabilities(
            ListSaveAsCapabilitiesRequest request) {
        DocumentArchive.requireValid(request);
        String accountId = RepositorySinkKeys.sourceAccountId(request.getSource());
        authorizer.checkAccountAccess(accountId);
        RepositorySinkAuthorizer.CapturedCaller caller = authorizer.captureCaller();
        ResolvedSource source =
                caller.callForRead(accountId, () -> resolveSource(request.getSource(), null));

        ListSaveAsCapabilitiesResponse.Builder response = ListSaveAsCapabilitiesResponse.newBuilder()
                .setResolvedSourceRevision(source.frozenSourceRevision())
                .setSourceSchemaType(source.schemaType())
                .setSourceSchemaFingerprint(source.schemaFingerprint())
                .setSourceSizeBytes(source.rawPayload().size());

        RepositorySinkRenderers activeRenderers = Objects.requireNonNull(renderers.get(), "renderers");
        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        for (RendererCapability capability : activeRenderers.registry().capabilities()) {
            Optional<Descriptor> optionsDescOpt =
                    activeRenderers.optionsDescriptor(capability.getProviderId());
            ByteString optionsClosure = optionsDescOpt
                    .map(desc -> DescriptorFingerprints.closure(desc).toByteString())
                    .orElse(ByteString.EMPTY);
            SaveAsCapability.Builder entry = SaveAsCapability.newBuilder()
                    .setCapability(capability)
                    .setOptionsDescriptors(optionsClosure);
            if (optionsDescOpt.isEmpty()) {
                entry.setStatus(
                                SaveAsCompatibilityStatus
                                        .SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE)
                        .setIncompatibilityReason("Renderer options descriptor is not registered");
                response.addCapabilities(entry.build());
                continue;
            }
            Message defaultOptions = DynamicMessage.getDefaultInstance(optionsDescOpt.get());
            try {
                activeRenderers.registry().check(
                        capability.getProviderId(),
                        source.message(),
                        defaultOptions,
                        (cap, in, opts) -> {},
                        cancellation);
                entry.setStatus(SaveAsCompatibilityStatus.SAVE_AS_COMPATIBILITY_STATUS_COMPATIBLE);
            } catch (RenderFailure failure) {
                if (failure.kind() == RenderFailure.Kind.CANCELLED) {
                    throw Status.CANCELLED
                            .withDescription(sanitizeMessage(failure.getMessage(), "Cancelled"))
                            .withCause(failure)
                            .asRuntimeException();
                }
                entry.setStatus(
                                SaveAsCompatibilityStatus
                                        .SAVE_AS_COMPATIBILITY_STATUS_INCOMPATIBLE)
                        .setIncompatibilityReason(
                                truncate(
                                        sanitizeMessage(
                                                failure.getMessage(), failure.kind().name()),
                                        MAX_INCOMPATIBILITY_REASON_CHARS));
            }
            response.addCapabilities(entry.build());
        }
        ListSaveAsCapabilitiesResponse built = response.build();
        DocumentArchive.requireValidView(built);
        return built;
    }

    /**
     * Admits, authorizes, optionally converts, and streams a selected protobuf state or repository
     * revision to the destination archive entry through {@link RenditionUploads}.
     *
     * @param request publication request
     * @return settled (when {@code async == false}) or queued/running (when {@code async == true})
     *     publication job
     */
    public PublishRepositorySinkResponse publish(PublishRepositorySinkRequest request) {
        DocumentArchive.requireValid(request);
        SinkDestination dest = request.getDestination();
        ArchiveClassifications.requireUserMetadataKeys(dest.getMetadataMap());
        validateDestinationRenditionRules(request);
        String accountId = RepositorySinkKeys.resolveAndValidateAccount(request.getSource(), dest);
        authorizer.checkAccountAccess(accountId);
        RepositorySinkAuthorizer.CapturedCaller caller = authorizer.captureCaller();

        String intentSha256 = RepositorySinkKeys.intentSha256(request);
        boolean explicitOperationId = !request.getOperationId().isEmpty();
        String operationId = RepositorySinkKeys.resolveOperationId(request, accountId, intentSha256);
        RepositorySinkLeaseCoordinator.JobKey jobKey =
                new RepositorySinkLeaseCoordinator.JobKey(accountId, operationId);

        Optional<RepositorySinkJobStore.StoredJob> existingOpt =
                leases.find(accountId, operationId);
        if (existingOpt.isPresent()) {
            return handleExistingPublish(
                    existingOpt.get(),
                    request,
                    accountId,
                    operationId,
                    intentSha256,
                    explicitOperationId,
                    jobKey,
                    caller);
        }

        ArchiveBinding destBinding =
                caller.callForRead(accountId, () -> archive.binding(dest.getAddress()));
        authorizer.checkWriteAccess(
                dest.getAddress(), destBinding.datasourceId(), dest.getWrittenBy());

        // Resolve source once at admission so a 'current' selector freezes a deterministic revision.
        ResolvedSource admittedSource =
                caller.callForRead(accountId, () -> resolveSource(request.getSource(), null));

        Timestamp admittedAt = now();
        RepositorySinkJob initialJob = RepositorySinkJob.newBuilder()
                .setAccountId(accountId)
                .setOperationId(operationId)
                .setIntentSha256(intentSha256)
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED)
                .setFrozenSourceRevision(admittedSource.frozenSourceRevision())
                .setDestinationAddress(dest.getAddress())
                .setDestinationRendition(effectiveDestinationRendition(request))
                .setExpectedDestinationVersion(dest.getExpectedDestinationVersion())
                .setCreateOnly(dest.getCreateOnly())
                .setCreatedAt(admittedAt)
                .setUpdatedAt(admittedAt)
                .build();
        DocumentArchive.requireValidView(initialJob);

        PublishRepositorySinkRequest pinnedRequest =
                pinRequestSourceRevision(request, operationId, admittedSource.frozenSourceRevision());
        RepositorySinkJobStore.StoredJob candidate = new RepositorySinkJobStore.StoredJob(
                initialJob, pinnedRequest, null, leases.newInitialLease(), false, "");
        RepositorySinkJobStore.StoredJob persisted = leases.insertOrGet(candidate);
        if (persisted != candidate) {
            return handleExistingPublish(
                    persisted,
                    request,
                    accountId,
                    operationId,
                    intentSha256,
                    explicitOperationId,
                    jobKey,
                    caller);
        }

        return dispatchExecution(
                persisted, admittedSource, destBinding, jobKey, caller, request.getAsync());
    }

    /**
     * Retrieves the durable state of a repository sink publication job, reconciling any uncertain
     * commit outcome or expired owner lease against v2 {@code GetOperationOutcome} while leaving
     * active remote leases undisturbed.
     *
     * @param request job lookup request
     * @return current job state and receipt
     */
    public GetRepositorySinkJobResponse getJob(GetRepositorySinkJobRequest request) {
        DocumentArchive.requireValid(request);
        String accountId = request.getAccountId();
        String operationId = request.getOperationId().trim();
        authorizer.checkAccountAccess(accountId);
        RepositorySinkAuthorizer.CapturedCaller caller = authorizer.captureCaller();

        RepositorySinkJobStore.StoredJob stored = leases.find(accountId, operationId)
                .orElseThrow(() -> ArchiveErrors.notFound(
                        "Repository sink job " + operationId + " does not exist"));
        RepositorySinkLeaseCoordinator.JobKey key =
                new RepositorySinkLeaseCoordinator.JobKey(accountId, operationId);
        RepositorySinkJobStore.StoredJob settled = reconcileIfNeeded(stored, key, caller, true);
        GetRepositorySinkJobResponse response =
                GetRepositorySinkJobResponse.newBuilder().setJob(settled.job()).build();
        DocumentArchive.requireValidView(response);
        return response;
    }

    /**
     * Signals cooperative cancellation of a queued or running repository sink publication job
     * across both local and peer repository host instances.
     *
     * @param request job cancellation request
     * @return resulting job state
     */
    public CancelRepositorySinkJobResponse cancelJob(CancelRepositorySinkJobRequest request) {
        DocumentArchive.requireValid(request);
        String accountId = request.getAccountId();
        String operationId = request.getOperationId().trim();
        authorizer.checkAccountAccess(accountId);
        RepositorySinkAuthorizer.CapturedCaller caller = authorizer.captureCaller();

        RepositorySinkJobStore.StoredJob stored = leases.find(accountId, operationId)
                .orElseThrow(() -> ArchiveErrors.notFound(
                        "Repository sink job " + operationId + " does not exist"));
        if (isTerminalState(stored.job().getState())) {
            return validCancelResponse(stored.job());
        }

        RepositorySinkLeaseCoordinator.JobKey key =
                new RepositorySinkLeaseCoordinator.JobKey(accountId, operationId);
        String reason = request.getReason().isBlank()
                ? "Cancelled by CancelRepositorySinkJob"
                : request.getReason().trim();

        RepositorySinkLeaseCoordinator.InFlightJob active = leases.getInFlight(key);
        if (active != null) {
            leases.requestCancel(accountId, operationId, reason);
            active.cancel(reason);
            long settleWaitMillis = leases.config().cancelSettleWait().toMillis();
            try {
                RepositorySinkJob finished =
                        active.completion.get(settleWaitMillis, TimeUnit.MILLISECONDS);
                return validCancelResponse(finished);
            } catch (TimeoutException | ExecutionException unsettled) {
                RepositorySinkJob latest = leases.find(accountId, operationId)
                        .map(RepositorySinkJobStore.StoredJob::job)
                        .orElse(stored.job());
                return validCancelResponse(latest);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED
                        .withDescription("Interrupted while awaiting sink job cancellation")
                        .withCause(interrupted)
                        .asRuntimeException();
            }
        }

        if (stored.job().getState() != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED
                && stored.lease().isActiveAt(clock.instant())) {
            RepositorySinkJobStore.StoredJob requested =
                    leases.requestCancel(accountId, operationId, reason).orElse(stored);
            RepositorySinkJobStore.StoredJob progressed =
                    leases.awaitRemoteOwnerProgress(requested, leases.config().cancelSettleWait());
            if (isTerminalState(progressed.job().getState())) {
                return validCancelResponse(progressed.job());
            }
            if (progressed.job().getState()
                            != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED
                    && progressed.lease().isActiveAt(clock.instant())) {
                return validCancelResponse(progressed.job());
            }
            stored = progressed;
        }

        RepositorySinkJobStore.StoredJob reconciled =
                reconcileIfNeeded(stored, key, caller, false);
        if (isTerminalState(reconciled.job().getState())) {
            return validCancelResponse(reconciled.job());
        }
        if (reconciled.lease().ownerId().equals(leases.instanceId())) {
            return validCancelResponse(markCancelled(reconciled, reason));
        }
        RepositorySinkJobStore.StoredJob latest =
                leases.requestCancel(accountId, operationId, reason).orElse(reconciled);
        return validCancelResponse(latest.job());
    }

    /**
     * Lists persisted {@link RenditionProvenance} records for a current or historical archive entry
     * version, enriched with live source revision currency indicators.
     *
     * @param request provenance listing request
     * @return rendition provenance entries in manifest order
     */
    public ListRenditionProvenanceResponse listRenditionProvenance(
            ListRenditionProvenanceRequest request) {
        DocumentArchive.requireValid(request);
        String accountId = request.getAddress().getAccountId();
        authorizer.checkAccountAccess(accountId);
        RepositorySinkAuthorizer.CapturedCaller caller = authorizer.captureCaller();

        return caller.callForRead(accountId, () -> {
            DocumentArchive.ServedEntry entry =
                    archive.readEntryDocument(request.getAddress(), request.getVersion());
            long currentEntryVersion = entry.current().getManifest().getRevision();
            VersionManifest manifest = archive.views().manifest(entry.served());
            Map<String, Blob> blobsById = new HashMap<>();
            for (Blob blob :
                    entry.served().getDocument().getBlobBag().getBlobs().getBlobList()) {
                blobsById.put(blob.getBlobId(), blob);
            }
            PartManifestEntry parsedPartEntry = findPartManifestEntry(
                    entry.served().getManifest(), ArchiveCoordinates.PARSED_PART).orElse(null);

            ListRenditionProvenanceResponse.Builder response =
                    ListRenditionProvenanceResponse.newBuilder()
                            .setAddress(request.getAddress())
                            .setVersion(manifest.getVersion())
                            .setCurrentEntryVersion(currentEntryVersion);

            for (RenditionManifestEntry row : manifest.getRenditionsList()) {
                if (row.getState() != RenditionState.RENDITION_STATE_PRESENT) {
                    continue;
                }
                RenditionDescriptor rend = row.getRendition();
                Optional<RenditionProvenance> provenanceOpt;
                if (rend.getName().equals(ArchiveCoordinates.PARSED)) {
                    long writtenRev =
                            parsedPartEntry != null ? parsedPartEntry.getWrittenInRevision() : 0L;
                    Timestamp writtenAt =
                            parsedPartEntry != null && parsedPartEntry.hasWrittenAt()
                                    ? parsedPartEntry.getWrittenAt()
                                    : null;
                    provenanceOpt = RepositorySinkProvenanceCodec.readParsedProvenance(
                            entry.served().getDocument().getSearchMetadata(),
                            writtenRev,
                            writtenAt,
                            row.getSizeBytes());
                } else {
                    String blobId = ArchiveCoordinates.blobId(rend.getName(), rend.getSubKey());
                    Blob blob = blobsById.get(blobId);
                    provenanceOpt = blob != null
                            ? RepositorySinkProvenanceCodec.readBlobProvenance(blob)
                            : Optional.empty();
                }
                if (provenanceOpt.isEmpty()) {
                    continue;
                }
                RenditionProvenance provenance = provenanceOpt.get();
                DocumentArchive.requireValidView(provenance);
                SourceCurrency currency = inspectSourceCurrency(provenance);
                response.addEntries(RenditionProvenanceEntry.newBuilder()
                        .setRendition(rend)
                        .setProvenance(provenance)
                        .setCurrentSourceRevision(currency.currentSourceRevision())
                        .setSourceStale(currency.sourceStale())
                        .build());
            }

            ListRenditionProvenanceResponse built = response.build();
            DocumentArchive.requireValidView(built);
            return built;
        });
    }

    @Override
    public void close() {
        leases.close();
        asyncExecutor.shutdownNow();
    }

    // --------------------------------------------------------- existing job handling

    private PublishRepositorySinkResponse handleExistingPublish(
            RepositorySinkJobStore.StoredJob existing,
            PublishRepositorySinkRequest incomingRequest,
            String accountId,
            String operationId,
            String intentSha256,
            boolean explicitOperationId,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller) {
        if (!existing.job().getIntentSha256().equals(intentSha256)) {
            throw ArchiveErrors.alreadyExists(
                    "operation_id",
                    operationId
                            + " is already bound to a different publication intent (expected SHA-256 "
                            + existing.job().getIntentSha256()
                            + ", received "
                            + intentSha256
                            + ")");
        }

        RepositorySinkJobStore.StoredJob cursor = existing;
        while (true) {
            RepositorySinkLeaseCoordinator.InFlightJob active = leases.getInFlight(jobKey);
            if (active != null) {
                if (incomingRequest.getAsync()) {
                    return validPublishResponse(cursor.job());
                }
                RepositorySinkJob awaited = awaitInFlight(active);
                return finishSyncPublish(awaited);
            }

            RepositorySinkJobStore.StoredJob reconciled =
                    reconcileIfNeeded(cursor, jobKey, caller, false);
            RepositorySinkJob reconciledJob = reconciled.job();

            switch (reconciledJob.getState()) {
                case REPOSITORY_SINK_JOB_STATE_SUCCEEDED -> {
                    if (!explicitOperationId
                            && incomingRequest.getDestination().getExpectedDestinationVersion() == 0
                            && !incomingRequest.getDestination().getCreateOnly()) {
                        requireDestinationStillAtVersion(
                                caller,
                                accountId,
                                incomingRequest.getDestination().getAddress(),
                                operationId,
                                reconciledJob.getDestinationVersion());
                    }
                    RepositorySinkJob replayed = markReplayedReceipt(reconciledJob);
                    return validPublishResponse(replayed);
                }
                case REPOSITORY_SINK_JOB_STATE_CANCELLED, REPOSITORY_SINK_JOB_STATE_UNRESOLVED -> {
                    if (incomingRequest.getAsync()) {
                        return validPublishResponse(reconciledJob);
                    }
                    throw toStatusException(reconciledJob.getFailure());
                }
                case REPOSITORY_SINK_JOB_STATE_FAILED -> {
                    if (!reconciledJob.getFailure().getRetryable()) {
                        if (incomingRequest.getAsync()) {
                            return validPublishResponse(reconciledJob);
                        }
                        throw toStatusException(reconciledJob.getFailure());
                    }
                    RepositorySinkJob requeuedJob = reconciledJob.toBuilder()
                            .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED)
                            .setBytesRendered(0L)
                            .setBytesCommitted(0L)
                            .clearFailure()
                            .setUpdatedAt(now())
                            .build();
                    DocumentArchive.requireValidView(requeuedJob);
                    Optional<RepositorySinkJobStore.StoredJob> claimedRetry =
                            leases.tryClaimRetryableFailure(
                                    accountId,
                                    operationId,
                                    reconciled.lease().leaseEpoch(),
                                    requeuedJob);
                    if (claimedRetry.isPresent()) {
                        return retryFromFrozenRevision(
                                claimedRetry.get(), incomingRequest, accountId, jobKey, caller);
                    }
                    cursor = leases.find(accountId, operationId).orElse(reconciled);
                }
                case REPOSITORY_SINK_JOB_STATE_QUEUED, REPOSITORY_SINK_JOB_STATE_RUNNING -> {
                    if (reconciled.lease().ownerId().equals(leases.instanceId())) {
                        return retryFromFrozenRevision(
                                reconciled, incomingRequest, accountId, jobKey, caller);
                    }
                    if (incomingRequest.getAsync()) {
                        return validPublishResponse(reconciledJob);
                    }
                    cursor = leases.awaitRemoteOwnerProgress(
                            reconciled, leases.config().leaseDuration());
                }
                case REPOSITORY_SINK_JOB_STATE_UNSPECIFIED, UNRECOGNIZED ->
                        throw ArchiveErrors.dataLoss(
                                "Stored sink job " + operationId + " has invalid state", null);
            }
        }
    }

    private PublishRepositorySinkResponse retryFromFrozenRevision(
            RepositorySinkJobStore.StoredJob stored,
            PublishRepositorySinkRequest incomingRequest,
            String accountId,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller) {
        SinkDestination dest = stored.request().getDestination();
        ArchiveBinding destBinding =
                caller.callForRead(accountId, () -> archive.binding(dest.getAddress()));
        authorizer.checkWriteAccess(
                dest.getAddress(), destBinding.datasourceId(), dest.getWrittenBy());
        ResolvedSource source = caller.callForRead(
                accountId,
                () -> resolveSource(
                        stored.request().getSource(),
                        stored.job().getFrozenSourceRevision()));
        return dispatchExecution(
                stored, source, destBinding, jobKey, caller, incomingRequest.getAsync());
    }

    private PublishRepositorySinkResponse dispatchExecution(
            RepositorySinkJobStore.StoredJob stored,
            ResolvedSource source,
            ArchiveBinding destBinding,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller,
            boolean async) {
        RepositorySinkLeaseCoordinator.InFlightRegistration registration =
                leases.registerInFlight(jobKey);
        if (!registration.newlyRegistered()) {
            if (async) {
                RepositorySinkJob latest = leases.find(jobKey.accountId(), jobKey.operationId())
                        .map(RepositorySinkJobStore.StoredJob::job)
                        .orElse(stored.job());
                return validPublishResponse(latest);
            }
            return finishSyncPublish(awaitInFlight(registration.tracker()));
        }

        RepositorySinkLeaseCoordinator.InFlightJob created = registration.tracker();
        if (async) {
            asyncExecutor.execute(
                    () -> runGuarded(stored, source, destBinding, jobKey, caller, created));
            return validPublishResponse(stored.job());
        }

        RepositorySinkJob settled =
                runGuarded(stored, source, destBinding, jobKey, caller, created);
        return finishSyncPublish(settled);
    }

    private PublishRepositorySinkResponse finishSyncPublish(RepositorySinkJob settled) {
        if (settled.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED) {
            return validPublishResponse(settled);
        }
        throw toStatusException(settled.getFailure());
    }

    private static RepositorySinkJob awaitInFlight(
            RepositorySinkLeaseCoordinator.InFlightJob active) {
        try {
            return active.completion.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("Interrupted while awaiting sink job completion")
                    .withCause(interrupted)
                    .asRuntimeException();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof StatusRuntimeException sre) {
                throw sre;
            }
            throw Status.INTERNAL.withCause(failed.getCause()).asRuntimeException();
        }
    }

    // --------------------------------------------------------- job execution

    private RepositorySinkJob runGuarded(
            RepositorySinkJobStore.StoredJob stored,
            ResolvedSource source,
            ArchiveBinding destBinding,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        tracker.runnerThread = Thread.currentThread();
        long leaseEpoch = stored.lease().leaseEpoch();
        ScheduledFuture<?> heartbeat = leases.startHeartbeat(jobKey, leaseEpoch, tracker);
        try {
            RepositorySinkJob settled =
                    executeJob(stored, source, destBinding, jobKey, caller, tracker, leaseEpoch);
            tracker.completion.complete(settled);
            return settled;
        } catch (RuntimeException | Error unexpected) {
            Thread.interrupted();
            if (tracker.leaseLost.get()) {
                RepositorySinkJob latest = latestJobOr(jobKey, stored.job());
                tracker.completion.complete(latest);
                if (unexpected instanceof Error err) {
                    throw err;
                }
                return latest;
            }
            if (tracker.cancelled.get() && !tracker.enteringCommit.get()) {
                RepositorySinkJob cancelledJob = markCancelled(stored, tracker.reasonOrDefault());
                tracker.completion.complete(cancelledJob);
                if (unexpected instanceof Error err) {
                    throw err;
                }
                return cancelledJob;
            }
            if (tracker.shuttingDown.get() && !tracker.enteringCommit.get()) {
                RepositorySinkJob shutdownJob = persistShutdownInterruption(stored, 0L);
                tracker.completion.complete(shutdownJob);
                if (unexpected instanceof Error err) {
                    throw err;
                }
                return shutdownJob;
            }
            Status status = Status.fromThrowable(unexpected);
            RepositorySinkFailure failure = failureOf(
                    status.getCode(),
                    "EXECUTION_ERROR",
                    sanitizeMessage(status.getDescription(), unexpected.getMessage()),
                    isTransientStatus(status.getCode()),
                    status.getCode() == Status.Code.ABORTED);
            RepositorySinkJob failedJob = persistFailure(
                    stored,
                    RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                    failure,
                    0L,
                    null);
            tracker.completion.complete(failedJob);
            if (unexpected instanceof Error err) {
                throw err;
            }
            return failedJob;
        } finally {
            leases.unregisterInFlight(jobKey, tracker, heartbeat);
            Thread.interrupted();
        }
    }

    private RepositorySinkJob executeJob(
            RepositorySinkJobStore.StoredJob initialStored,
            ResolvedSource source,
            ArchiveBinding destBinding,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller,
            RepositorySinkLeaseCoordinator.InFlightJob tracker,
            long leaseEpoch) {
        if (tracker.cancelled.get() || initialStored.cancelRequested()) {
            String reason = !initialStored.cancelReason().isBlank()
                    ? initialStored.cancelReason()
                    : tracker.reasonOrDefault();
            return markCancelled(initialStored, reason);
        }
        if (tracker.shuttingDown.get()) {
            return persistShutdownInterruption(initialStored, 0L);
        }
        if (tracker.leaseLost.get()) {
            return latestJobOr(jobKey, initialStored.job());
        }

        RepositorySinkJob runningJob = initialStored.job().toBuilder()
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING)
                .clearFailure()
                .setUpdatedAt(now())
                .build();
        DocumentArchive.requireValidView(runningJob);
        RepositorySinkJobStore.StoredJob stored = initialStored.withTransition(
                runningJob, initialStored.pendingProvenance(), leases.nextExpiration());
        if (!leases.transitionOwned(stored, leaseEpoch)) {
            tracker.markLeaseLost();
            return latestJobOr(jobKey, runningJob);
        }

        PublishRepositorySinkRequest request = stored.request();
        SinkDestination dest = request.getDestination();
        RenditionDescriptor effectiveRendition = effectiveDestinationRendition(request);
        boolean parsedDestination =
                effectiveRendition.getName().equals(ArchiveCoordinates.PARSED);

        RendererProvider.Cancellation cancellation = () ->
                tracker.shouldAbortWork()
                        || Context.current().isCancelled()
                        || Thread.currentThread().isInterrupted();

        ByteString outputBytes;
        ByteString retainedDescriptorsForOutput;
        RendererCapability usedCapability = null;
        String optionsSha256 = "";

        if (request.hasConversion()) {
            RepositorySinkRenderers activeRenderers =
                    Objects.requireNonNull(renderers.get(), "renderers");
            ConversionSpec conv = request.getConversion();
            Optional<RendererCapability> capOpt = findCapability(activeRenderers, conv.getProviderId());
            if (capOpt.isEmpty()) {
                return persistFailure(
                        stored,
                        RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                        failureOf(
                                Status.Code.FAILED_PRECONDITION,
                                "RENDERER_NOT_ENABLED",
                                "Renderer capability '" + conv.getProviderId() + "' is not enabled",
                                false,
                                false),
                        0L,
                        null);
            }
            usedCapability = capOpt.get();
            Optional<String> mismatch = verifyCapabilityMatchesSpec(usedCapability, conv);
            if (mismatch.isPresent()) {
                return persistFailure(
                        stored,
                        RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                        failureOf(
                                Status.Code.FAILED_PRECONDITION,
                                "CAPABILITY_MISMATCH",
                                mismatch.get(),
                                false,
                                false),
                        0L,
                        null);
            }
            Optional<Descriptor> optionsDescOpt =
                    activeRenderers.optionsDescriptor(conv.getProviderId());
            if (optionsDescOpt.isEmpty()) {
                return persistFailure(
                        stored,
                        RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                        failureOf(
                                Status.Code.FAILED_PRECONDITION,
                                "OPTIONS_DESCRIPTOR_MISSING",
                                "Options descriptor is not registered for " + conv.getProviderId(),
                                false,
                                false),
                        0L,
                        null);
            }
            Message optionsMsg;
            try {
                optionsMsg = decodeMessage(
                        conv.getOptions(),
                        optionsDescOpt.get(),
                        cancellation);
            } catch (StatusRuntimeException invalidOptions) {
                return persistStatusFailure(stored, invalidOptions, "INVALID_RENDER_OPTIONS", 0L, null);
            }
            optionsSha256 = RenditionReads.sha256Hex(
                    ByteString.copyFrom(MessageFingerprints.deterministicBytes(optionsMsg)));
            try {
                outputBytes = activeRenderers.registry().render(
                        conv.getProviderId(),
                        source.message(),
                        optionsMsg,
                        (cap, in, opts) -> {},
                        cancellation);
            } catch (RenderFailure renderFailure) {
                return persistRenderFailure(stored, renderFailure, jobKey, tracker);
            } catch (IOException ioFailure) {
                return persistFailure(
                        stored,
                        RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                        failureOf(
                                Status.Code.INTERNAL,
                                "RENDER_IO_ERROR",
                                sanitizeMessage(ioFailure.getMessage(), "Renderer I/O failure"),
                                false,
                                false),
                        0L,
                        null);
            }
            retainedDescriptorsForOutput = ByteString.EMPTY;
        } else {
            try {
                cancellation.check();
            } catch (RenderFailure cancelled) {
                return persistRenderFailure(stored, cancelled, jobKey, tracker);
            }
            outputBytes = source.rawPayload();
            retainedDescriptorsForOutput = source.descriptorSetBytes();
        }

        long bytesRendered = outputBytes.size();
        String outputSha256 = RenditionReads.sha256Hex(outputBytes);
        Timestamp provisionalCommittedAt = now();
        RenditionProvenance provisionalProvenance = buildProvenance(
                jobKey.operationId(),
                source,
                request,
                effectiveRendition,
                usedCapability,
                optionsSha256,
                bytesRendered,
                bytesRendered,
                outputSha256,
                1L,
                false,
                provisionalCommittedAt);

        if (tracker.leaseLost.get()) {
            return latestJobOr(jobKey, stored.job());
        }
        if (tracker.cancelled.get()) {
            return markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
        }
        if (tracker.shuttingDown.get()) {
            return persistShutdownInterruption(stored, bytesRendered);
        }
        if (Thread.currentThread().isInterrupted()) {
            return markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
        }

        RepositorySinkJob preCommitJob = stored.job().toBuilder()
                .setBytesRendered(bytesRendered)
                .setBytesCommitted(0L)
                .setUpdatedAt(provisionalCommittedAt)
                .build();
        DocumentArchive.requireValidView(preCommitJob);
        RepositorySinkJobStore.StoredJob preCommitCandidate = stored.withTransition(
                preCommitJob, provisionalProvenance, leases.nextExpiration());
        RepositorySinkJobStore.PreCommitDecision preCommitFence =
                leases.prepareCommit(preCommitCandidate, leaseEpoch);
        switch (preCommitFence.status()) {
            case CANCEL_REQUESTED -> {
                String reason = preCommitFence.stored().cancelReason().isBlank()
                        ? tracker.reasonOrDefault()
                        : preCommitFence.stored().cancelReason();
                return markCancelledWithRendered(preCommitFence.stored(), reason, bytesRendered);
            }
            case LEASE_LOST -> {
                tracker.markLeaseLost();
                return preCommitFence.stored().job();
            }
            case READY -> stored = preCommitFence.stored();
        }

        tracker.enteringCommit.set(true);
        if (tracker.cancelled.get()) {
            return markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
        }
        if (tracker.leaseLost.get()) {
            return latestJobOr(jobKey, stored.job());
        }

        String commitOperationId = RepositorySinkKeys.downstreamUploadOperationId(
                jobKey.accountId(), jobKey.operationId());

        UploadRenditionResponse uploadReceipt;
        try {
            ByteString payloadToUpload = outputBytes;
            ByteString descriptorsToAttach = retainedDescriptorsForOutput;
            uploadReceipt = caller.callForCommit(
                    dest.getAddress(),
                    destBinding.datasourceId(),
                    dest.getWrittenBy(),
                    () -> streamUpload(
                            jobKey,
                            leaseEpoch,
                            tracker,
                            commitOperationId,
                            storedJobIntentSha(preCommitJob),
                            dest,
                            effectiveRendition,
                            parsedDestination,
                            payloadToUpload,
                            outputSha256,
                            provisionalProvenance,
                            descriptorsToAttach));
        } catch (StatusRuntimeException uploadFailure) {
            return handleUploadFailure(
                    stored, jobKey, commitOperationId, bytesRendered, caller, tracker, uploadFailure);
        }

        Timestamp committedAt = now();
        RenditionProvenance finalProvenance = provisionalProvenance.toBuilder()
                .setDestinationVersion(uploadReceipt.getVersion())
                .setBytesCommitted(uploadReceipt.getSizeBytes())
                .setReplayed(uploadReceipt.getReplayed())
                .setCommittedAt(committedAt)
                .build();
        DocumentArchive.requireValidView(finalProvenance);

        RepositorySinkJob succeeded = preCommitJob.toBuilder()
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED)
                .setBytesRendered(bytesRendered)
                .setBytesCommitted(uploadReceipt.getSizeBytes())
                .setDestinationVersion(uploadReceipt.getVersion())
                .setEntryUuid(entryUuid(dest.getAddress()))
                .setRootChecksum(uploadReceipt.getRootChecksum())
                .setReplayed(uploadReceipt.getReplayed())
                .setProvenance(finalProvenance)
                .clearFailure()
                .setUpdatedAt(committedAt)
                .build();
        DocumentArchive.requireValidView(succeeded);
        RepositorySinkJobStore.StoredJob succeededStored =
                stored.withTransition(succeeded, finalProvenance, clock.instant());
        if (!leases.transitionOwned(succeededStored, leaseEpoch)) {
            return latestJobOr(jobKey, succeeded);
        }
        return succeeded;
    }

    private static String storedJobIntentSha(RepositorySinkJob job) {
        return job.getIntentSha256();
    }

    private UploadRenditionResponse streamUpload(
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            long leaseEpoch,
            RepositorySinkLeaseCoordinator.InFlightJob tracker,
            String commitOperationId,
            String intentSha256,
            SinkDestination dest,
            RenditionDescriptor effectiveRendition,
            boolean parsedDestination,
            ByteString payload,
            String outputSha256,
            RenditionProvenance provisionalProvenance,
            ByteString retainedDescriptors) {
        UploadRenditionHeader.Builder header = UploadRenditionHeader.newBuilder()
                .setOperationId(commitOperationId)
                .setAddress(dest.getAddress())
                .setRendition(effectiveRendition)
                .setExpectedVersion(dest.getExpectedDestinationVersion())
                .setSizeBytes(payload.size())
                .setExpectedSha256(outputSha256);
        if (dest.hasWrittenBy()) {
            header.setWrittenBy(dest.getWrittenBy());
        }
        if (!dest.getFilename().isEmpty()) {
            header.setFilename(dest.getFilename());
        }

        Map<String, Value> blobMetadataFields = parsedDestination
                ? Map.of()
                : RepositorySinkProvenanceCodec.blobMetadataFields(
                        provisionalProvenance, retainedDescriptors);
        Map<String, String> systemSearchMetadata = parsedDestination
                ? RepositorySinkProvenanceCodec.parsedSearchMetadata(
                        provisionalProvenance, retainedDescriptors)
                : Map.of();

        DocumentArchive.SinkUploadCustomization customization =
                new DocumentArchive.SinkUploadCustomization(
                        dest.getCreateOnly(),
                        intentSha256,
                        dest.getTitle(),
                        dest.getContentType(),
                        dest.getSourceUri(),
                        dest.getMetadataMap(),
                        dest.hasSecurity() ? dest.getSecurity() : null,
                        blobMetadataFields,
                        systemSearchMetadata);

        CompletableFuture<UploadRenditionResponse> responseFuture = new CompletableFuture<>();
        RenditionUploads.FlowControlledUpload call =
                uploads.uploadFlowControlled(customization, new StreamObserver<>() {
                    private UploadRenditionResponse value;

                    @Override
                    public void onNext(UploadRenditionResponse response) {
                        this.value = response;
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseFuture.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        if (value == null) {
                            responseFuture.completeExceptionally(
                                    Status.INTERNAL
                                            .withDescription(
                                                    "UploadRendition completed without a receipt")
                                            .asRuntimeException());
                        } else {
                            responseFuture.complete(value);
                        }
                    }
                });

        try {
            call.onNext(UploadRenditionRequest.newBuilder().setHeader(header.build()).build());
            for (int offset = 0;
                    offset < payload.size() && !responseFuture.isDone();
                    offset += RenditionReads.MAX_FRAME_BYTES) {
                abortStreamIfInvalidated(call, tracker);
                awaitUploadReady(call, responseFuture);
                if (responseFuture.isDone()) {
                    break;
                }
                int end = Math.min(payload.size(), offset + RenditionReads.MAX_FRAME_BYTES);
                call.onNext(UploadRenditionRequest.newBuilder()
                        .setChunk(payload.substring(offset, end))
                        .build());
            }
            if (!responseFuture.isDone()) {
                verifyDurableLeaseBeforeFinalCommit(call, jobKey, leaseEpoch, tracker);
                call.onCompleted();
            }
            return responseFuture.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription("Interrupted while streaming rendition upload")
                    .withCause(interrupted)
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof StatusRuntimeException sre) {
                throw sre;
            }
            throw Status.INTERNAL.withCause(failed.getCause()).asRuntimeException();
        }
    }

    private void verifyDurableLeaseBeforeFinalCommit(
            RenditionUploads.FlowControlledUpload call,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            long leaseEpoch,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        abortStreamIfInvalidated(call, tracker);
        Optional<RepositorySinkJobStore.LeaseRenewal> renewal =
                leases.renewLease(jobKey.accountId(), jobKey.operationId(), leaseEpoch);
        if (renewal.isEmpty()) {
            tracker.markLeaseLost();
            StatusRuntimeException fenced = Status.ABORTED
                    .withDescription("Repository sink job lease lost prior to repository commit")
                    .asRuntimeException();
            call.onError(fenced);
            throw fenced;
        }
        if (renewal.get().cancelRequested()) {
            tracker.cancel(renewal.get().cancelReason());
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription(tracker.reasonOrDefault())
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        }
    }

    private static void abortStreamIfInvalidated(
            RenditionUploads.FlowControlledUpload call,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        if (tracker.leaseLost.get()) {
            StatusRuntimeException fenced = Status.ABORTED
                    .withDescription("Repository sink job lease lost during upload")
                    .asRuntimeException();
            call.onError(fenced);
            throw fenced;
        }
        if (tracker.cancelled.get()) {
            StatusRuntimeException cancelled = Status.CANCELLED
                    .withDescription(tracker.reasonOrDefault())
                    .asRuntimeException();
            call.onError(cancelled);
            throw cancelled;
        }
        if (tracker.shuttingDown.get()) {
            StatusRuntimeException shutdown = Status.UNAVAILABLE
                    .withDescription("RepositorySink shutting down before commit")
                    .asRuntimeException();
            call.onError(shutdown);
            throw shutdown;
        }
    }

    private static void awaitUploadReady(
            RenditionUploads.FlowControlledUpload call,
            CompletableFuture<UploadRenditionResponse> responseFuture)
            throws InterruptedException {
        while (!call.isReady() && !responseFuture.isDone()) {
            if (!call.awaitReady(UPLOAD_READY_WAIT_NANOS) && !responseFuture.isDone()) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("Timed out waiting for downstream UploadDocument flow control")
                        .asRuntimeException();
            }
        }
    }

    // --------------------------------------------------------- reconciliation & failures

    private RepositorySinkJob handleUploadFailure(
            RepositorySinkJobStore.StoredJob stored,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            String commitOperationId,
            long bytesRendered,
            RepositorySinkAuthorizer.CapturedCaller caller,
            RepositorySinkLeaseCoordinator.InFlightJob tracker,
            StatusRuntimeException uploadFailure) {
        if (tracker.leaseLost.get()) {
            return latestJobOr(jobKey, stored.job());
        }
        Status.Code code = uploadFailure.getStatus().getCode();
        if (isUncertainCommitFailure(code)) {
            return reconcileAfterUncertainCommit(
                    stored, jobKey, commitOperationId, bytesRendered, caller, uploadFailure);
        }
        String reasonCode = switch (code) {
            case ABORTED -> "DESTINATION_CONFLICT";
            case PERMISSION_DENIED, UNAUTHENTICATED -> "COMMIT_ACCESS_DENIED";
            case INVALID_ARGUMENT -> "INVALID_DESTINATION_SAVE";
            case ALREADY_EXISTS -> "COMMIT_OPERATION_CONFLICT";
            case RESOURCE_EXHAUSTED -> "COMMIT_LIMIT_EXCEEDED";
            default -> "COMMIT_FAILED";
        };
        return persistStatusFailure(
                stored, uploadFailure, reasonCode, bytesRendered, stored.pendingProvenance());
    }

    private RepositorySinkJob reconcileAfterUncertainCommit(
            RepositorySinkJobStore.StoredJob stored,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            String commitOperationId,
            long bytesRendered,
            RepositorySinkAuthorizer.CapturedCaller caller,
            StatusRuntimeException originalFailure) {
        GetOperationOutcomeResponse outcome;
        try {
            outcome = caller.callForRead(
                    jobKey.accountId(),
                    () -> archive.documents().getOperationOutcome(
                            GetOperationOutcomeRequest.newBuilder()
                                    .setAccountId(jobKey.accountId())
                                    .setOperationId(commitOperationId)
                                    .build()));
        } catch (StatusRuntimeException lookupFailure) {
            RepositorySinkFailure unresolved = failureOf(
                    lookupFailure.getStatus().getCode(),
                    "COMMIT_OUTCOME_UNRESOLVED",
                    sanitizeMessage(
                            originalFailure.getStatus().getDescription(),
                            "Commit reply was lost and outcome lookup is currently unavailable"),
                    true,
                    false);
            return persistFailure(
                    stored,
                    RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED,
                    unresolved,
                    bytesRendered,
                    stored.pendingProvenance());
        }

        if (outcome.hasCommitted()) {
            return settleFromRecordedOutcome(stored, outcome.getCommitted());
        }

        Status.Code code = originalFailure.getStatus().getCode();
        RepositorySinkFailure notCommitted = failureOf(
                code,
                "COMMIT_NOT_RECORDED",
                sanitizeMessage(
                        originalFailure.getStatus().getDescription(),
                        "Upload failed before repository commit"),
                isTransientStatus(code),
                code == Status.Code.ABORTED);
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                notCommitted,
                bytesRendered,
                stored.pendingProvenance());
    }

    private RepositorySinkJobStore.StoredJob reconcileIfNeeded(
            RepositorySinkJobStore.StoredJob stored,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller,
            boolean settleUncommittedAsFailed) {
        RepositorySinkJobState state = stored.job().getState();
        if (isTerminalState(state)) {
            return stored;
        }
        if ((state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED
                        || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING)
                && (leases.isInFlight(jobKey) || stored.lease().isActiveAt(clock.instant()))) {
            return stored;
        }
        if (state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED
                && stored.lease().isActiveAt(clock.instant())) {
            return stored;
        }

        String accountId = stored.job().getAccountId();
        String operationId = stored.job().getOperationId();
        Optional<RepositorySinkJobStore.StoredJob> claimedOpt =
                leases.tryClaimExpiredOrUnresolved(
                        accountId, operationId, stored.lease().leaseEpoch());
        if (claimedOpt.isEmpty()) {
            return leases.find(accountId, operationId).orElse(stored);
        }
        return reconcileClaimedOrphan(claimedOpt.get(), caller, settleUncommittedAsFailed);
    }

    private RepositorySinkJobStore.StoredJob reconcileClaimedOrphan(
            RepositorySinkJobStore.StoredJob claimed,
            RepositorySinkAuthorizer.CapturedCaller caller,
            boolean settleUncommittedAsFailed) {
        String accountId = claimed.job().getAccountId();
        String operationId = claimed.job().getOperationId();
        String commitOpId = RepositorySinkKeys.downstreamUploadOperationId(accountId, operationId);
        GetOperationOutcomeResponse outcome;
        try {
            outcome = caller.callForRead(
                    accountId,
                    () -> archive.documents().getOperationOutcome(
                            GetOperationOutcomeRequest.newBuilder()
                                    .setAccountId(accountId)
                                    .setOperationId(commitOpId)
                                    .build()));
        } catch (StatusRuntimeException lookupFailure) {
            if (isUncertainCommitFailure(lookupFailure.getStatus().getCode())) {
                RepositorySinkFailure unresolved = failureOf(
                        lookupFailure.getStatus().getCode(),
                        "COMMIT_OUTCOME_UNRESOLVED",
                        sanitizeMessage(
                                lookupFailure.getStatus().getDescription(),
                                "Repository outcome lookup is unavailable"),
                        true,
                        false);
                RepositorySinkJob unresolvedJob = persistFailure(
                        claimed,
                        RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED,
                        unresolved,
                        claimed.job().getBytesRendered(),
                        claimed.pendingProvenance());
                return leases.find(accountId, operationId)
                        .orElseGet(() -> claimed.withTransition(
                                unresolvedJob, claimed.pendingProvenance(), clock.instant()));
            }
            throw lookupFailure;
        }

        if (outcome.hasCommitted()) {
            RepositorySinkJob settled = settleFromRecordedOutcome(claimed, outcome.getCommitted());
            return leases.find(accountId, operationId)
                    .orElseGet(() -> claimed.withTransition(
                            settled, settled.getProvenance(), clock.instant()));
        }
        if (claimed.cancelRequested()) {
            String reason = claimed.cancelReason().isBlank()
                    ? "Cancelled by CancelRepositorySinkJob"
                    : claimed.cancelReason();
            RepositorySinkJob cancelled = markCancelled(claimed, reason);
            return leases.find(accountId, operationId)
                    .orElseGet(() -> claimed.withTransition(cancelled, null, clock.instant()));
        }
        if (!settleUncommittedAsFailed) {
            return claimed;
        }
        RepositorySinkFailure abandoned = failureOf(
                Status.Code.UNAVAILABLE,
                "COMMIT_NOT_RECORDED",
                "Sink publication did not commit before interruption and may be retried with the same operation_id",
                true,
                false);
        RepositorySinkJob failed = persistFailure(
                claimed,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                abandoned,
                claimed.job().getBytesRendered(),
                claimed.pendingProvenance());
        return leases.find(accountId, operationId)
                .orElseGet(() -> claimed.withTransition(
                        failed, claimed.pendingProvenance(), clock.instant()));
    }

    private RepositorySinkJob settleFromRecordedOutcome(
            RepositorySinkJobStore.StoredJob stored, RecordedOutcome recorded) {
        if (recorded.hasRejection()) {
            SaveRejection rejection = recorded.getRejection();
            RepositorySinkFailure rejected = failureOf(
                    Status.Code.INVALID_ARGUMENT,
                    "SAVE_REJECTED_" + rejection.getReason().name(),
                    sanitizeMessage(rejection.getMessage(), "Repository save was rejected"),
                    false,
                    false);
            return persistFailure(
                    stored,
                    RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                    rejected,
                    stored.job().getBytesRendered(),
                    stored.pendingProvenance());
        }

        Accession accession =
                recorded.hasAccession() ? recorded.getAccession() : recorded.getUnchanged();
        long committedVersion = accession.getRevision();
        Timestamp committedAt = accession.getCommittedAt();
        RenditionProvenance baseProvenance = stored.pendingProvenance();
        if (baseProvenance == null) {
            baseProvenance = readProvenanceFromCommittedEntry(
                            stored.job().getDestinationAddress(),
                            stored.job().getDestinationRendition(),
                            committedVersion)
                    .orElseThrow(() -> ArchiveErrors.dataLoss(
                            "Committed sink outcome for "
                                    + stored.job().getOperationId()
                                    + " has no recoverable rendition provenance",
                            null));
        }
        long committedBytes = baseProvenance.getBytesRendered();
        RenditionProvenance settledProvenance = baseProvenance.toBuilder()
                .setDestinationVersion(committedVersion)
                .setBytesCommitted(committedBytes)
                .setReplayed(true)
                .setCommittedAt(committedAt)
                .build();
        DocumentArchive.requireValidView(settledProvenance);

        RepositorySinkJob settledJob = stored.job().toBuilder()
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED)
                .setBytesRendered(committedBytes)
                .setBytesCommitted(committedBytes)
                .setDestinationVersion(committedVersion)
                .setEntryUuid(entryUuid(stored.job().getDestinationAddress()))
                .setRootChecksum(accession.getValidation().getCandidateSha256())
                .setReplayed(true)
                .setProvenance(settledProvenance)
                .clearFailure()
                .setUpdatedAt(now())
                .build();
        DocumentArchive.requireValidView(settledJob);
        RepositorySinkJobStore.StoredJob updated =
                stored.withTransition(settledJob, settledProvenance, clock.instant());
        if (!leases.transitionOwned(updated, stored.lease().leaseEpoch())) {
            return leases.find(stored.job().getAccountId(), stored.job().getOperationId())
                    .map(RepositorySinkJobStore.StoredJob::job)
                    .orElse(settledJob);
        }
        return settledJob;
    }

    private Optional<RenditionProvenance> readProvenanceFromCommittedEntry(
            EntryAddress address, RenditionDescriptor rendition, long version) {
        DocumentArchive.ServedEntry served = archive.readEntryDocument(address, version);
        if (rendition.getName().equals(ArchiveCoordinates.PARSED)) {
            PartManifestEntry part = findPartManifestEntry(
                            served.served().getManifest(), ArchiveCoordinates.PARSED_PART)
                    .orElse(null);
            if (part == null) {
                return Optional.empty();
            }
            return RepositorySinkProvenanceCodec.readParsedProvenance(
                    served.served().getDocument().getSearchMetadata(),
                    part.getWrittenInRevision(),
                    part.hasWrittenAt() ? part.getWrittenAt() : null,
                    part.getSizeBytes());
        }
        String blobId = ArchiveCoordinates.blobId(rendition.getName(), rendition.getSubKey());
        for (Blob blob : served.served().getDocument().getBlobBag().getBlobs().getBlobList()) {
            if (blob.getBlobId().equals(blobId)) {
                return RepositorySinkProvenanceCodec.readBlobProvenance(blob);
            }
        }
        return Optional.empty();
    }

    private RepositorySinkJob persistRenderFailure(
            RepositorySinkJobStore.StoredJob stored,
            RenderFailure failure,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkLeaseCoordinator.InFlightJob tracker) {
        if (tracker.leaseLost.get()) {
            return latestJobOr(jobKey, stored.job());
        }
        if (tracker.shuttingDown.get() && !tracker.cancelled.get()) {
            return persistShutdownInterruption(stored, 0L);
        }
        if (failure.kind() == RenderFailure.Kind.CANCELLED || tracker.cancelled.get()) {
            String message = tracker.cancelled.get()
                    ? tracker.reasonOrDefault()
                    : sanitizeMessage(failure.getMessage(), "Rendering cancelled");
            return markCancelled(stored, message);
        }
        Status.Code code = switch (failure.kind()) {
            case LIMIT -> Status.Code.RESOURCE_EXHAUSTED;
            case DENIED -> Status.Code.PERMISSION_DENIED;
            case INVALID -> Status.Code.INVALID_ARGUMENT;
            case UNSUPPORTED -> Status.Code.FAILED_PRECONDITION;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case FAILED -> Status.Code.INTERNAL;
            case CANCELLED -> Status.Code.CANCELLED;
        };
        String reasonCode = "RENDER_" + failure.kind().name();
        boolean retryable = failure.kind() == RenderFailure.Kind.UNAVAILABLE;
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                failureOf(
                        code,
                        reasonCode,
                        sanitizeMessage(failure.getMessage(), reasonCode),
                        retryable,
                        false),
                0L,
                null);
    }

    private RepositorySinkJob persistStatusFailure(
            RepositorySinkJobStore.StoredJob stored,
            StatusRuntimeException exception,
            String reasonCode,
            long bytesRendered,
            RenditionProvenance pendingProvenance) {
        Status.Code code = exception.getStatus().getCode();
        if (code == Status.Code.CANCELLED) {
            return markCancelledWithRendered(
                    stored,
                    sanitizeMessage(exception.getStatus().getDescription(), "Cancelled"),
                    bytesRendered);
        }
        RepositorySinkFailure failure = failureOf(
                code,
                reasonCode,
                sanitizeMessage(exception.getStatus().getDescription(), code.name()),
                isTransientStatus(code),
                code == Status.Code.ABORTED);
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                failure,
                bytesRendered,
                pendingProvenance);
    }

    private RepositorySinkJob persistShutdownInterruption(
            RepositorySinkJobStore.StoredJob stored, long bytesRendered) {
        RepositorySinkFailure failure = failureOf(
                Status.Code.UNAVAILABLE,
                "HOST_SHUTDOWN",
                "RepositorySink host shut down before commit; retry with the same operation_id",
                true,
                false);
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                failure,
                bytesRendered,
                null);
    }

    private RepositorySinkJob markCancelled(
            RepositorySinkJobStore.StoredJob stored, String reason) {
        return markCancelledWithRendered(stored, reason, stored.job().getBytesRendered());
    }

    private RepositorySinkJob markCancelledWithRendered(
            RepositorySinkJobStore.StoredJob stored, String reason, long bytesRendered) {
        RepositorySinkFailure failure = failureOf(
                Status.Code.CANCELLED,
                "JOB_CANCELLED",
                sanitizeMessage(reason, "Cancelled"),
                false,
                false);
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED,
                failure,
                bytesRendered,
                null);
    }

    private RepositorySinkJob persistFailure(
            RepositorySinkJobStore.StoredJob stored,
            RepositorySinkJobState state,
            RepositorySinkFailure failure,
            long bytesRendered,
            RenditionProvenance pendingProvenance) {
        RepositorySinkJob updated = stored.job().toBuilder()
                .setState(state)
                .setBytesRendered(bytesRendered)
                .setBytesCommitted(0L)
                .setFailure(failure)
                .setUpdatedAt(now())
                .build();
        DocumentArchive.requireValidView(updated);
        RepositorySinkJobStore.StoredJob next =
                stored.withTransition(updated, pendingProvenance, clock.instant());
        if (!leases.transitionOwned(next, stored.lease().leaseEpoch())) {
            return leases.find(stored.job().getAccountId(), stored.job().getOperationId())
                    .map(RepositorySinkJobStore.StoredJob::job)
                    .orElse(updated);
        }
        return updated;
    }

    private RepositorySinkJob latestJobOr(
            RepositorySinkLeaseCoordinator.JobKey key, RepositorySinkJob fallback) {
        return leases.find(key.accountId(), key.operationId())
                .map(RepositorySinkJobStore.StoredJob::job)
                .orElse(fallback);
    }

    private RepositorySinkJob markReplayedReceipt(RepositorySinkJob succeeded) {
        RenditionProvenance replayedProvenance =
                succeeded.getProvenance().toBuilder().setReplayed(true).build();
        RepositorySinkJob replayed = succeeded.toBuilder()
                .setReplayed(true)
                .setProvenance(replayedProvenance)
                .build();
        DocumentArchive.requireValidView(replayed);
        return replayed;
    }

    private void requireDestinationStillAtVersion(
            RepositorySinkAuthorizer.CapturedCaller caller,
            String accountId,
            EntryAddress destination,
            String operationId,
            long recordedVersion) {
        long currentVersion = caller.callForRead(accountId, () -> {
            Optional<ai.protomolt.proto.repo.v2.GetDocumentResponse> current =
                    archive.readCurrent(ArchiveCoordinates.address(destination));
            return current.map(doc -> doc.getManifest().getRevision()).orElse(0L);
        });
        if (currentVersion != recordedVersion) {
            throw ArchiveErrors.failedPrecondition(
                    "Derived sink operation_id "
                            + operationId
                            + " recorded version "
                            + recordedVersion
                            + ", which is no longer current (now version "
                            + currentVersion
                            + "); supply a new operation_id or explicit expected_destination_version");
        }
    }

    // --------------------------------------------------------- source resolution

    private ResolvedSource resolveSource(SinkSource source, Long pinnedRevisionOverride) {
        return switch (source.getKindCase()) {
            case PROCESSING_STATE -> resolveProcessingState(source.getProcessingState());
            case REPOSITORY_REVISION -> resolveRepositoryRevision(
                    source.getRepositoryRevision(), pinnedRevisionOverride);
            case KIND_NOT_SET -> throw ArchiveErrors.invalid("source", "must select one source arm");
        };
    }

    private ResolvedSource resolveProcessingState(ProcessingStateSource ps) {
        Descriptor rootDescriptor = linkDescriptor(
                ps.getDescriptors(),
                ps.getMessageType(),
                "source.processing_state.descriptors",
                "source.processing_state.message_type");
        FileDescriptorSet closureSet = DescriptorFingerprints.closure(rootDescriptor);
        String closureFingerprint = DescriptorFingerprints.fingerprint(closureSet);
        String suppliedSetFingerprint = fingerprintOfRawSet(
                ps.getDescriptors(), "source.processing_state.descriptors");
        if (!ps.getSchemaFingerprint().equals(closureFingerprint)
                && !ps.getSchemaFingerprint().equals(suppliedSetFingerprint)) {
            throw ArchiveErrors.invalid(
                    "source.processing_state.schema_fingerprint",
                    "supplied fingerprint '"
                            + ps.getSchemaFingerprint()
                            + "' does not match descriptor closure fingerprint '"
                            + closureFingerprint
                            + "'");
        }

        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message parsed = decodeMessage(ps.getPayload(), rootDescriptor, cancellation);
        ByteString closureBytes = closureSet.toByteString();
        String descriptorSha256 = RenditionReads.sha256Hex(closureBytes);

        List<SourcePartDigest> digests = new ArrayList<>();
        String primaryIdentity = ps.getStepName().isEmpty()
                ? "processing_state:" + rootDescriptor.getFullName()
                : "step:" + ps.getStepName();
        digests.add(SourcePartDigest.newBuilder()
                .setIdentity(primaryIdentity)
                .setSha256(RenditionReads.sha256Hex(ps.getPayload()))
                .setSizeBytes(ps.getPayload().size())
                .build());
        digests.addAll(ps.getInputDigestsList());

        return new ResolvedSource(
                0L,
                rootDescriptor,
                parsed,
                ps.getPayload(),
                closureBytes,
                descriptorSha256,
                rootDescriptor.getFullName(),
                closureFingerprint,
                List.copyOf(digests),
                null,
                null,
                null,
                null,
                "",
                ps.getRunId(),
                ps.getStepName(),
                ps.getInputId());
    }

    private ResolvedSource resolveRepositoryRevision(
            RepositoryRevisionSource repoSource, Long pinnedRevisionOverride) {
        return switch (repoSource.getTargetCase()) {
            case ARCHIVE_RENDITION -> resolveArchiveRendition(
                    repoSource.getArchiveRendition(), pinnedRevisionOverride);
            case DOCUMENT_PART -> resolveDocumentPart(
                    repoSource.getDocumentPart(), pinnedRevisionOverride);
            case TARGET_NOT_SET -> throw ArchiveErrors.invalid(
                    "source.repository_revision",
                    "must select archive_rendition or document_part");
        };
    }

    private ResolvedSource resolveArchiveRendition(
            ArchiveRenditionSource ars, Long pinnedRevisionOverride) {
        EntryAddress entryAddress = ars.getAddress();
        long requestedVersion = resolveRequestedVersion(ars.getRevision(), pinnedRevisionOverride);
        DocumentArchive.ServedEntry served =
                archive.readEntryDocument(entryAddress, requestedVersion);
        long resolvedVersion = served.served().getManifest().getRevision();
        RenditionDescriptor want = ars.getRendition();

        ReadRenditionRequest readReq = ReadRenditionRequest.newBuilder()
                .setAddress(entryAddress)
                .setVersion(resolvedVersion)
                .setRendition(want.getName())
                .setSubKey(want.getSubKey())
                .build();
        RenditionReads.OpenedRead opened = reads.open(readReq);
        ByteString payloadBytes;
        String payloadSha256;
        RenditionDescriptor storedRendition = opened.header().getRendition();
        try (InputStream body = opened.body()) {
            if (!want.getMediaType().isEmpty()
                    && !storedRendition.getMediaType().isEmpty()
                    && !want.getMediaType().equals(storedRendition.getMediaType())) {
                throw ArchiveErrors.failedPrecondition(
                        "Source rendition media_type is '"
                                + storedRendition.getMediaType()
                                + "', not requested '"
                                + want.getMediaType()
                                + "'");
            }
            payloadBytes = readBoundedBytes(body, MAX_SOURCE_PAYLOAD_BYTES, "source rendition");
            payloadSha256 = opened.header().getSha256();
        } catch (IOException ioFailure) {
            throw Status.INTERNAL
                    .withDescription("Failed to stream source rendition bytes")
                    .withCause(ioFailure)
                    .asRuntimeException();
        }

        RetainedSourceSchema retainedSchema =
                loadArchiveRenditionSchema(served, resolvedVersion, want);
        Descriptor rootDescriptor = linkDescriptor(
                retainedSchema.descriptorSetBytes(),
                retainedSchema.messageType(),
                "retained_descriptors",
                "retained_message_type");
        FileDescriptorSet closureSet = DescriptorFingerprints.closure(rootDescriptor);
        String schemaFingerprint = DescriptorFingerprints.fingerprint(closureSet);
        if (!ars.getExpectedSchemaFingerprint().isEmpty()
                && !ars.getExpectedSchemaFingerprint().equals(schemaFingerprint)) {
            throw ArchiveErrors.failedPrecondition(
                    "Source rendition schema fingerprint '"
                            + schemaFingerprint
                            + "' does not match expected_schema_fingerprint '"
                            + ars.getExpectedSchemaFingerprint()
                            + "'");
        }

        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message message = decodeMessage(payloadBytes, rootDescriptor, cancellation);
        ByteString closureBytes = closureSet.toByteString();

        SourcePartDigest digest = SourcePartDigest.newBuilder()
                .setIdentity(
                        "rendition:" + ArchiveCoordinates.blobId(want.getName(), want.getSubKey()))
                .setSha256(payloadSha256)
                .setSizeBytes(payloadBytes.size())
                .build();

        return new ResolvedSource(
                resolvedVersion,
                rootDescriptor,
                message,
                payloadBytes,
                closureBytes,
                retainedSchema.descriptorSha256(),
                rootDescriptor.getFullName(),
                schemaFingerprint,
                List.of(digest),
                entryAddress,
                storedRendition,
                null,
                null,
                "",
                "",
                "",
                "");
    }

    private RetainedSourceSchema loadArchiveRenditionSchema(
            DocumentArchive.ServedEntry served,
            long resolvedVersion,
            RenditionDescriptor want) {
        if (want.getName().equals(ArchiveCoordinates.PARSED)) {
            SearchMetadata search = served.served().getDocument().getSearchMetadata();
            String encodedDesc = search.getMetadataOrDefault(
                    RepositorySinkProvenanceCodec.PARSED_RETAINED_DESCRIPTORS_METADATA_KEY, "");
            String msgType = search.getMetadataOrDefault(
                    RepositorySinkProvenanceCodec.PARSED_RETAINED_MESSAGE_TYPE_METADATA_KEY, "");
            if (!encodedDesc.isEmpty() && !msgType.isEmpty()) {
                ByteString bytes = RepositorySinkProvenanceCodec.decodeDescriptors(
                        encodedDesc, MAX_DESCRIPTOR_BYTES);
                return new RetainedSourceSchema(
                        msgType, bytes, RenditionReads.sha256Hex(bytes));
            }
            PartSchemaBinding partBinding = findPartSchemaBinding(
                            served.served().getValidation(), ArchiveCoordinates.PARSED_PART)
                    .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                            "Source entry parsed rendition at version "
                                    + resolvedVersion
                                    + " has no retained schema binding"));
            return fetchRetainedSchemaFromDocument(
                    served.address(), resolvedVersion, partBinding.getFragmentSchema());
        }

        String blobId = ArchiveCoordinates.blobId(want.getName(), want.getSubKey());
        for (Blob blob : served.served().getDocument().getBlobBag().getBlobs().getBlobList()) {
            if (blob.getBlobId().equals(blobId)) {
                RepositorySinkProvenanceCodec.RetainedBlobSchema retained =
                        RepositorySinkProvenanceCodec.readBlobRetainedSchema(
                                        blob, MAX_DESCRIPTOR_BYTES)
                                .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                                        "Rendition '"
                                                + blobId
                                                + "' at version "
                                                + resolvedVersion
                                                + " does not carry retained authoritative protobuf descriptors;"
                                                + " derived non-protobuf renditions cannot be re-projected as authoritative sources"));
                return new RetainedSourceSchema(
                        retained.messageType(),
                        retained.descriptorSetBytes(),
                        RenditionReads.sha256Hex(retained.descriptorSetBytes()));
            }
        }
        throw ArchiveErrors.notFound(
                "Source entry version " + resolvedVersion + " has no rendition " + blobId);
    }

    private ResolvedSource resolveDocumentPart(
            DocumentPartSource dps, Long pinnedRevisionOverride) {
        NodeAddress docAddress = dps.getAddress();
        requireCompleteNodeAddress(docAddress, "source.repository_revision.document_part.address");
        RevisionSelector selector =
                resolveRevisionSelector(dps.getRevision(), pinnedRevisionOverride);

        StreamedPartRead partRead = readDocumentPartBytes(docAddress, selector, dps.getSelector());
        long resolvedRevision = partRead.manifest().getRevision();
        RevisionSelector pinnedSelector = DocumentArchive.revision(resolvedRevision);

        GetManifestResponse manifestResp = archive.documents().getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(docAddress)
                        .setRevision(pinnedSelector)
                        .build());
        RevisionValidation validation = manifestResp.getValidation();
        PartSchemaBinding partBinding = findPartSchemaBinding(validation, dps.getSelector())
                .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                        "Source document revision "
                                + resolvedRevision
                                + " has no retained fragment schema for "
                                + formatPartSelector(dps.getSelector())));

        RetainedSourceSchema fragmentSchema = fetchRetainedSchemaFromDocument(
                docAddress, resolvedRevision, partBinding.getFragmentSchema());
        Descriptor fragmentDescriptor = linkDescriptor(
                fragmentSchema.descriptorSetBytes(),
                fragmentSchema.messageType(),
                "retained_fragment_schema",
                "retained_fragment_type");
        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message fragmentMessage =
                decodeMessage(partRead.bytes(), fragmentDescriptor, cancellation);

        SourcePartDigest partDigest = SourcePartDigest.newBuilder()
                .setIdentity("part:" + formatPartSelector(dps.getSelector()))
                .setSha256(partRead.entry().getSha256())
                .setSizeBytes(partRead.bytes().size())
                .build();

        Descriptor selectedDescriptor;
        Message selectedMessage;
        ByteString selectedPayload;
        ByteString selectedDescriptorSet;
        String retainedDescriptorSha256;
        List<SourcePartDigest> digests;

        if (dps.getOccurrenceFieldPath().isEmpty()) {
            selectedDescriptor = fragmentDescriptor;
            selectedMessage = fragmentMessage;
            selectedPayload = partRead.bytes();
            selectedDescriptorSet = DescriptorFingerprints.closure(fragmentDescriptor).toByteString();
            retainedDescriptorSha256 = fragmentSchema.descriptorSha256();
            digests = List.of(partDigest);
        } else {
            ResolvedOccurrenceValue occ = resolveOccurrenceInFragment(
                    docAddress,
                    resolvedRevision,
                    dps.getSelector(),
                    dps.getOccurrenceFieldPath(),
                    fragmentDescriptor,
                    fragmentMessage,
                    fragmentSchema.descriptorSha256(),
                    validation,
                    cancellation);
            selectedDescriptor = occ.descriptor();
            selectedMessage = occ.message();
            selectedPayload = occ.payloadBytes();
            selectedDescriptorSet = DescriptorFingerprints.closure(selectedDescriptor).toByteString();
            retainedDescriptorSha256 = occ.retainedDescriptorSha256();
            digests = List.of(
                    partDigest,
                    SourcePartDigest.newBuilder()
                            .setIdentity("occurrence:" + dps.getOccurrenceFieldPath())
                            .setSha256(RenditionReads.sha256Hex(selectedPayload))
                            .setSizeBytes(selectedPayload.size())
                            .build());
        }

        String schemaFingerprint = DescriptorFingerprints.fingerprintOf(selectedDescriptor);
        if (!dps.getExpectedSchemaFingerprint().isEmpty()
                && !dps.getExpectedSchemaFingerprint().equals(schemaFingerprint)) {
            throw ArchiveErrors.failedPrecondition(
                    "Source document schema fingerprint '"
                            + schemaFingerprint
                            + "' does not match expected_schema_fingerprint '"
                            + dps.getExpectedSchemaFingerprint()
                            + "'");
        }

        return new ResolvedSource(
                resolvedRevision,
                selectedDescriptor,
                selectedMessage,
                selectedPayload,
                selectedDescriptorSet,
                retainedDescriptorSha256,
                selectedDescriptor.getFullName(),
                schemaFingerprint,
                digests,
                null,
                null,
                docAddress,
                dps.getSelector(),
                dps.getOccurrenceFieldPath(),
                "",
                "",
                "");
    }

    private ResolvedOccurrenceValue resolveOccurrenceInFragment(
            NodeAddress docAddress,
            long resolvedRevision,
            PartSelector partSelector,
            String fieldPath,
            Descriptor rootDescriptor,
            Message rootMessage,
            String rootDescriptorSha256,
            RevisionValidation validation,
            RendererProvider.Cancellation cancellation) {
        List<PathSegment> segments = parseOccurrenceFieldPath(fieldPath);
        Descriptor curDesc = rootDescriptor;
        Message curMsg = rootMessage;
        String curDescriptorSha256 = rootDescriptorSha256;
        List<RepositorySchemaOccurrenceStep> navSteps = new ArrayList<>();

        for (int i = 0; i < segments.size(); i++) {
            if (ANY_FULL_NAME.equals(curDesc.getFullName())) {
                UnpackedAny unpacked = unpackRetainedAny(
                        docAddress,
                        resolvedRevision,
                        partSelector,
                        fieldPath,
                        curMsg,
                        navSteps,
                        validation,
                        cancellation);
                curDesc = unpacked.descriptor();
                curMsg = unpacked.message();
                curDescriptorSha256 = unpacked.descriptorSha256();
            }

            PathSegment seg = segments.get(i);
            FieldDescriptor fd = curDesc.findFieldByName(seg.fieldName());
            if (fd == null) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "unknown field '" + seg.fieldName() + "' on " + curDesc.getFullName());
            }
            if (fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "field '" + seg.fieldName() + "' is not a protobuf message field");
            }
            navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                    .setFieldNumber(fd.getNumber())
                    .build());

            if (fd.isMapField()) {
                if (seg.subscript() == null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "map field '" + seg.fieldName() + "' requires a [key] subscript");
                }
                FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
                FieldDescriptor valFd = fd.getMessageType().findFieldByNumber(2);
                if (valFd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "map field '" + seg.fieldName() + "' values are not protobuf messages");
                }
                ParsedMapKey parsedKey = parseMapKey(seg.subscript(), keyFd, fieldPath);
                navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                        .setMapKey(parsedKey.stepKey())
                        .build());
                Message matchedEntry = null;
                @SuppressWarnings("unchecked")
                List<Message> entries = (List<Message>) curMsg.getField(fd);
                for (Message entry : entries) {
                    if (Objects.equals(entry.getField(keyFd), parsedKey.javaKey())) {
                        matchedEntry = entry;
                    }
                }
                if (matchedEntry == null) {
                    throw ArchiveErrors.notFound(
                            "Occurrence map key '"
                                    + seg.subscript()
                                    + "' is absent at '"
                                    + fieldPath
                                    + "' on revision "
                                    + resolvedRevision);
                }
                curMsg = (Message) matchedEntry.getField(valFd);
                curDesc = valFd.getMessageType();
            } else if (fd.isRepeated()) {
                if (seg.subscript() == null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "repeated field '" + seg.fieldName() + "' requires an [index] subscript");
                }
                int idx = parseNonNegativeIndex(seg.subscript(), fieldPath);
                int count = curMsg.getRepeatedFieldCount(fd);
                if (idx >= count) {
                    throw ArchiveErrors.notFound(
                            "Occurrence index "
                                    + idx
                                    + " out of bounds (size "
                                    + count
                                    + ") at '"
                                    + fieldPath
                                    + "' on revision "
                                    + resolvedRevision);
                }
                navSteps.add(RepositorySchemaOccurrenceStep.newBuilder()
                        .setRepeatedIndex(idx)
                        .build());
                curMsg = (Message) curMsg.getRepeatedField(fd, idx);
                curDesc = fd.getMessageType();
            } else {
                if (seg.subscript() != null) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "singular field '" + seg.fieldName() + "' does not take a subscript");
                }
                if (!curMsg.hasField(fd)) {
                    throw ArchiveErrors.notFound(
                            "Occurrence field '"
                                    + fieldPath
                                    + "' is not set on revision "
                                    + resolvedRevision);
                }
                curMsg = (Message) curMsg.getField(fd);
                curDesc = fd.getMessageType();
            }
        }

        if (ANY_FULL_NAME.equals(curDesc.getFullName())) {
            UnpackedAny unpacked = unpackRetainedAny(
                    docAddress,
                    resolvedRevision,
                    partSelector,
                    fieldPath,
                    curMsg,
                    navSteps,
                    validation,
                    cancellation);
            return new ResolvedOccurrenceValue(
                    unpacked.descriptor(),
                    unpacked.message(),
                    unpacked.valueBytes(),
                    unpacked.descriptorSha256());
        }

        ByteString deterministicPayload =
                ByteString.copyFrom(MessageFingerprints.deterministicBytes(curMsg));
        return new ResolvedOccurrenceValue(
                curDesc, curMsg, deterministicPayload, curDescriptorSha256);
    }

    private UnpackedAny unpackRetainedAny(
            NodeAddress docAddress,
            long resolvedRevision,
            PartSelector partSelector,
            String fieldPath,
            Message anyMessage,
            List<RepositorySchemaOccurrenceStep> navSteps,
            RevisionValidation validation,
            RendererProvider.Cancellation cancellation) {
        FieldDescriptor typeUrlField = anyMessage.getDescriptorForType().findFieldByNumber(1);
        FieldDescriptor valueField = anyMessage.getDescriptorForType().findFieldByNumber(2);
        String typeUrl = (String) anyMessage.getField(typeUrlField);
        ByteString valueBytes = (ByteString) anyMessage.getField(valueField);

        for (OpaqueOccurrence opaque : validation.getOpaqueOccurrencesList()) {
            if (opaque.getOccurrence().getPart().equals(partSelector)
                    && stripAnyBoundaries(opaque.getOccurrence().getStepsList()).equals(navSteps)) {
                throw ArchiveErrors.failedPrecondition(
                        "Occurrence '"
                                + fieldPath
                                + "' on revision "
                                + resolvedRevision
                                + " was admitted as opaque ("
                                + opaque.getTypeUrl()
                                + ") without a retained schema and cannot be decoded");
            }
        }

        ResolvedOccurrence matched = null;
        for (ResolvedOccurrence candidate : validation.getResolvedOccurrencesList()) {
            if (!candidate.getOccurrence().getPart().equals(partSelector)) {
                continue;
            }
            if (stripAnyBoundaries(candidate.getOccurrence().getStepsList()).equals(navSteps)
                    && candidate.getSchema().getTypeUrl().equals(typeUrl)) {
                matched = candidate;
                break;
            }
        }
        if (matched == null) {
            throw ArchiveErrors.failedPrecondition(
                    "Occurrence '"
                            + fieldPath
                            + "' ("
                            + typeUrl
                            + ") on revision "
                            + resolvedRevision
                            + " has no retained schema binding in RevisionValidation");
        }

        RetainedSourceSchema occSchema = fetchRetainedSchemaFromDocument(
                docAddress, resolvedRevision, matched.getSchema());
        Descriptor occDesc = linkDescriptor(
                occSchema.descriptorSetBytes(),
                occSchema.messageType(),
                "retained_occurrence_schema",
                "retained_occurrence_type");
        Message decoded = decodeMessage(valueBytes, occDesc, cancellation);
        return new UnpackedAny(occDesc, decoded, valueBytes, occSchema.descriptorSha256());
    }

    private static List<RepositorySchemaOccurrenceStep> stripAnyBoundaries(
            List<RepositorySchemaOccurrenceStep> steps) {
        List<RepositorySchemaOccurrenceStep> filtered = new ArrayList<>(steps.size());
        for (RepositorySchemaOccurrenceStep step : steps) {
            if (!step.hasAnyBoundary()) {
                filtered.add(step);
            }
        }
        return filtered;
    }

    private RetainedSourceSchema fetchRetainedSchemaFromDocument(
            NodeAddress docAddress,
            long revision,
            RepositorySchemaAssetReference reference) {
        GetSchemaDescriptorResponse resp = archive.documents().getSchemaDescriptor(
                GetSchemaDescriptorRequest.newBuilder()
                        .setAddress(docAddress)
                        .setRevision(DocumentArchive.revision(revision))
                        .setDescriptorSha256(reference.getDescriptorSha256())
                        .build());
        String messageType = typeNameFromUrl(reference.getTypeUrl());
        return new RetainedSourceSchema(
                messageType, resp.getDescriptorSet(), resp.getDescriptorSha256());
    }

    private StreamedPartRead readDocumentPartBytes(
            NodeAddress docAddress, RevisionSelector revision, PartSelector partSelector) {
        List<ByteString> chunks = new ArrayList<>();
        DocumentManifest[] manifestHolder = new DocumentManifest[1];
        boolean[] completed = {false};
        try (Context.CancellableContext callContext = Context.current().withCancellation()) {
            callContext.run(() -> {
                Iterator<ReadDocumentResponse> frames = archive.documents().readDocument(
                        ReadDocumentRequest.newBuilder()
                                .setAddress(docAddress)
                                .setRevision(revision)
                                .setSelection(
                                        PartSelection.newBuilder().addSelectors(partSelector))
                                .build());
                long total = 0L;
                while (frames.hasNext()) {
                    ReadDocumentResponse frame = frames.next();
                    if (frame.hasManifest()) {
                        manifestHolder[0] = frame.getManifest();
                    } else if (frame.hasPartData()) {
                        ByteString data = frame.getPartData().getData();
                        if (data.size() > MAX_SOURCE_PAYLOAD_BYTES - total) {
                            throw Status.RESOURCE_EXHAUSTED
                                    .withDescription(
                                            "Source document part exceeds maximum sink input budget of "
                                                    + MAX_SOURCE_PAYLOAD_BYTES
                                                    + " bytes")
                                    .asRuntimeException();
                        }
                        total += data.size();
                        chunks.add(data);
                    } else if (frame.hasComplete()) {
                        completed[0] = true;
                    }
                }
            });
        }
        if (manifestHolder[0] == null || !completed[0]) {
            throw ArchiveErrors.dataLoss("ReadDocument ended before ReadComplete", null);
        }
        PartManifestEntry entry = findPartManifestEntry(manifestHolder[0], partSelector)
                .orElseThrow(() -> ArchiveErrors.notFound(
                        "Source document revision "
                                + manifestHolder[0].getRevision()
                                + " has no part "
                                + formatPartSelector(partSelector)));
        if (entry.getState() != PartState.PART_STATE_PRESENT) {
            throw ArchiveErrors.failedPrecondition(
                    "Source document part "
                            + formatPartSelector(partSelector)
                            + " at revision "
                            + manifestHolder[0].getRevision()
                            + " is "
                            + entry.getState());
        }
        ByteString assembled = ByteString.copyFrom(chunks);
        if (assembled.size() != entry.getSizeBytes()
                || !RenditionReads.sha256Hex(assembled).equals(entry.getSha256())) {
            throw ArchiveErrors.dataLoss(
                    "Source document part bytes do not match manifest digest", null);
        }
        return new StreamedPartRead(manifestHolder[0], entry, assembled);
    }

    // --------------------------------------------------------- path & descriptor helpers

    private static List<PathSegment> parseOccurrenceFieldPath(String path) {
        String trimmed = path.trim();
        if (trimmed.isEmpty()) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "must not be blank");
        }
        List<PathSegment> segments = new ArrayList<>();
        int i = 0;
        int n = trimmed.length();
        while (i < n) {
            if (segments.size() >= MAX_OCCURRENCE_STEPS) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "exceeds maximum step count of " + MAX_OCCURRENCE_STEPS);
            }
            int nameStart = i;
            while (i < n) {
                char c = trimmed.charAt(i);
                if (c == '.' || c == '[') {
                    break;
                }
                if (!isIdentifierChar(c)) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "invalid character '" + c + "' in field path '" + path + "'");
                }
                i++;
            }
            if (i == nameStart) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "empty field segment in '" + path + "'");
            }
            String fieldName = trimmed.substring(nameStart, i);
            String subscript = null;
            if (i < n && trimmed.charAt(i) == '[') {
                i++;
                int subStart = i;
                boolean inQuotes = false;
                while (i < n) {
                    char c = trimmed.charAt(i);
                    if (c == '\\' && inQuotes && i + 1 < n) {
                        i += 2;
                        continue;
                    }
                    if (c == '"') {
                        inQuotes = !inQuotes;
                    } else if (c == ']' && !inQuotes) {
                        break;
                    }
                    i++;
                }
                if (i >= n || trimmed.charAt(i) != ']') {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "unterminated '[' in '" + path + "'");
                }
                subscript = trimmed.substring(subStart, i);
                i++;
            }
            segments.add(new PathSegment(fieldName, subscript));
            if (i < n) {
                if (trimmed.charAt(i) != '.') {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "expected '.' after segment in '" + path + "'");
                }
                i++;
                if (i == n) {
                    throw ArchiveErrors.invalid(
                            "source.repository_revision.document_part.occurrence_field_path",
                            "trailing '.' in '" + path + "'");
                }
            }
        }
        return List.copyOf(segments);
    }

    private static boolean isIdentifierChar(char c) {
        return (c >= 'a' && c >= 'z')
                || (c >= 'A' && c >= 'Z')
                || (c >= '0' && c >= '9')
                || c == '_';
    }

    private static int parseNonNegativeIndex(String raw, String path) {
        if (raw.isEmpty()) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "empty repeated index in '" + path + "'");
        }
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) {
                throw ArchiveErrors.invalid(
                        "source.repository_revision.document_part.occurrence_field_path",
                        "negative repeated index in '" + path + "'");
            }
            return value;
        } catch (NumberFormatException invalid) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "invalid repeated index '" + raw + "' in '" + path + "'");
        }
    }

    private static ParsedMapKey parseMapKey(String raw, FieldDescriptor keyFd, String path) {
        RepositoryOccurrenceMapKey.Builder step = RepositoryOccurrenceMapKey.newBuilder();
        try {
            return switch (keyFd.getType()) {
                case STRING -> {
                    String unquoted = unquoteMapKey(raw);
                    yield new ParsedMapKey(
                            unquoted,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING)
                                    .setStringValue(unquoted)
                                    .build());
                }
                case BOOL -> {
                    if (!"true".equals(raw) && !"false".equals(raw)) {
                        throw new IllegalArgumentException("invalid bool key");
                    }
                    boolean val = Boolean.parseBoolean(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL)
                                    .setBoolValue(val)
                                    .build());
                }
                case INT32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT32)
                                    .setSignedValue(val)
                                    .build());
                }
                case SINT32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT32)
                                    .setSignedValue(val)
                                    .build());
                }
                case SFIXED32 -> {
                    int val = Integer.parseInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED32)
                                    .setSignedValue(val)
                                    .build());
                }
                case UINT32 -> {
                    int val = Integer.parseUnsignedInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT32)
                                    .setUnsignedValue(Integer.toUnsignedLong(val))
                                    .build());
                }
                case FIXED32 -> {
                    int val = Integer.parseUnsignedInt(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED32)
                                    .setUnsignedValue(Integer.toUnsignedLong(val))
                                    .build());
                }
                case INT64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT64)
                                    .setSignedValue(val)
                                    .build());
                }
                case SINT64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64)
                                    .setSignedValue(val)
                                    .build());
                }
                case SFIXED64 -> {
                    long val = Long.parseLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED64)
                                    .setSignedValue(val)
                                    .build());
                }
                case UINT64 -> {
                    long val = Long.parseUnsignedLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT64)
                                    .setUnsignedValue(val)
                                    .build());
                }
                case FIXED64 -> {
                    long val = Long.parseUnsignedLong(raw);
                    yield new ParsedMapKey(
                            val,
                            step.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED64)
                                    .setUnsignedValue(val)
                                    .build());
                }
                default -> throw new IllegalArgumentException("unsupported map key type");
            };
        } catch (IllegalArgumentException badKey) {
            throw ArchiveErrors.invalid(
                    "source.repository_revision.document_part.occurrence_field_path",
                    "invalid map key '" + raw + "' in '" + path + "'");
        }
    }

    private static String unquoteMapKey(String raw) {
        if (raw.length() >= 2 && raw.charAt(0) == '"' && raw.charAt(raw.length() - 1) == '"') {
            StringBuilder out = new StringBuilder(raw.length() - 2);
            for (int i = 1; i < raw.length() - 1; i++) {
                char c = raw.charAt(i);
                if (c == '\\' && i + 1 < raw.length() - 1) {
                    i++;
                    out.append(raw.charAt(i));
                } else {
                    out.append(c);
                }
            }
            return out.toString();
        }
        return raw;
    }

    private static Descriptor linkDescriptor(
            ByteString descriptorBytes,
            String messageType,
            String descriptorsField,
            String typeField) {
        List<FileDescriptor> linked;
        try {
            linked = ClosedDescriptorSet.load(descriptorBytes, DESCRIPTOR_LIMITS);
        } catch (ClosedDescriptorSet.LimitExceededException limit) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription(descriptorsField + ": " + limit.getMessage())
                    .asRuntimeException();
        } catch (IllegalArgumentException invalid) {
            throw ArchiveErrors.invalid(descriptorsField, invalid.getMessage());
        }
        for (FileDescriptor file : linked) {
            Descriptor found = findDescriptorInFile(file, messageType);
            if (found != null) {
                return found;
            }
        }
        throw ArchiveErrors.invalid(
                typeField, "message type '" + messageType + "' is not present in descriptor set");
    }

    private static Descriptor findDescriptorInFile(FileDescriptor file, String fullName) {
        ArrayDeque<Descriptor> queue = new ArrayDeque<>(file.getMessageTypes());
        while (!queue.isEmpty()) {
            Descriptor current = queue.removeFirst();
            if (current.getFullName().equals(fullName)) {
                return current;
            }
            queue.addAll(current.getNestedTypes());
        }
        return null;
    }

    private static String fingerprintOfRawSet(ByteString descriptorBytes, String field) {
        try {
            return DescriptorFingerprints.fingerprint(FileDescriptorSet.parseFrom(descriptorBytes));
        } catch (InvalidProtocolBufferException invalid) {
            throw ArchiveErrors.invalid(field, "invalid FileDescriptorSet wire bytes");
        }
    }

    private static Message decodeMessage(
            ByteString wireBytes,
            Descriptor descriptor,
            RendererProvider.Cancellation cancellation) {
        MessageWireBudget budget = new MessageWireBudget(MAX_WIRE_VALUES, MAX_WIRE_DEPTH, () -> {
            if (cancellation.cancelled() || Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Wire scan cancelled");
            }
        });
        try {
            budget.check(wireBytes, descriptor);
            return DynamicMessage.parseFrom(descriptor, wireBytes);
        } catch (CancellationException cancelled) {
            throw Status.CANCELLED
                    .withDescription("Cancelled while decoding protobuf message")
                    .withCause(cancelled)
                    .asRuntimeException();
        } catch (MessageWireBudget.LimitExceededException limit) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription(limit.getMessage())
                    .asRuntimeException();
        } catch (InvalidProtocolBufferException malformed) {
            throw ArchiveErrors.invalid(
                    descriptor.getFullName(),
                    "wire bytes do not decode cleanly: " + malformed.getMessage());
        }
    }

    private static ByteString readBoundedBytes(InputStream in, int maxBytes, String label)
            throws IOException {
        ByteString.Output out = ByteString.newOutput();
        byte[] buffer = new byte[STREAM_READ_BUFFER_BYTES];
        int read;
        int total = 0;
        while ((read = in.read(buffer)) >= 0) {
            if (read > maxBytes - total) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription(
                                label
                                        + " exceeds maximum sink input budget of "
                                        + maxBytes
                                        + " bytes")
                        .asRuntimeException();
            }
            out.write(buffer, 0, read);
            total += read;
        }
        return out.toByteString();
    }

    // --------------------------------------------------------- provenance & currency helpers

    private RenditionProvenance buildProvenance(
            String operationId,
            ResolvedSource source,
            PublishRepositorySinkRequest request,
            RenditionDescriptor effectiveRendition,
            RendererCapability usedCapability,
            String optionsSha256,
            long bytesRendered,
            long bytesCommitted,
            String outputSha256,
            long destinationVersion,
            boolean replayed,
            Timestamp committedAt) {
        RenditionProvenance.Builder builder = RenditionProvenance.newBuilder()
                .setOperationId(operationId)
                .setSourceRevision(source.frozenSourceRevision())
                .addAllSourceDigests(source.sourceDigests())
                .setSchemaType(source.schemaType())
                .setSchemaFingerprint(source.schemaFingerprint())
                .setRetainedDescriptorSha256(source.descriptorSha256())
                .setOutputMediaType(effectiveRendition.getMediaType())
                .setBytesRendered(bytesRendered)
                .setBytesCommitted(bytesCommitted)
                .setOutputSha256(outputSha256)
                .setDestinationAddress(request.getDestination().getAddress())
                .setDestinationRendition(effectiveRendition)
                .setDestinationVersion(destinationVersion)
                .setReplayed(replayed)
                .setCommittedAt(committedAt);

        if (source.sourceEntry() != null) {
            builder.setSourceEntry(source.sourceEntry());
        }
        if (source.sourceRendition() != null) {
            builder.setSourceRendition(source.sourceRendition());
        }
        if (source.sourceDocument() != null) {
            builder.setSourceDocument(source.sourceDocument());
        }
        if (source.sourcePart() != null) {
            builder.setSourcePart(source.sourcePart());
        }
        if (!source.sourceOccurrenceFieldPath().isEmpty()) {
            builder.setSourceOccurrenceFieldPath(source.sourceOccurrenceFieldPath());
        }
        if (!source.processingRunId().isEmpty()) {
            builder.setProcessingRunId(source.processingRunId());
        }
        if (!source.processingStepName().isEmpty()) {
            builder.setProcessingStepName(source.processingStepName());
        }
        if (!source.processingInputId().isEmpty()) {
            builder.setProcessingInputId(source.processingInputId());
        }

        if (usedCapability != null) {
            ConversionSpec conv = request.getConversion();
            builder.setAuthoritativeProtobuf(false)
                    .setProviderId(usedCapability.getProviderId())
                    .setProviderVersion(usedCapability.getProviderVersion())
                    .setSourceProfile(usedCapability.getSourceProfile())
                    .setOptionsType(usedCapability.getOptionsType())
                    .setOptionsFingerprint(usedCapability.getOptionsFingerprint())
                    .setOptionsSha256(optionsSha256)
                    .setMappingId(conv.getMappingId())
                    .setPolicyId(conv.getPolicyId())
                    .setLossDescription(usedCapability.getLossDescription());
        } else {
            builder.setAuthoritativeProtobuf(true);
        }

        RenditionProvenance built = builder.build();
        DocumentArchive.requireValidView(built);
        return built;
    }

    private SourceCurrency inspectSourceCurrency(RenditionProvenance provenance) {
        if (provenance.getSourceRevision() == 0L) {
            return new SourceCurrency(0L, false);
        }
        NodeAddress sourceAddress;
        if (provenance.hasSourceEntry()) {
            sourceAddress = ArchiveCoordinates.address(provenance.getSourceEntry());
        } else if (provenance.hasSourceDocument()) {
            sourceAddress = provenance.getSourceDocument();
        } else {
            return new SourceCurrency(0L, false);
        }
        try {
            GetManifestResponse live = archive.documents().getManifest(
                    GetManifestRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setRevision(DocumentArchive.current())
                            .build());
            long currentRev = live.getManifest().getRevision();
            return new SourceCurrency(currentRev, currentRev != provenance.getSourceRevision());
        } catch (StatusRuntimeException failure) {
            Status.Code code = failure.getStatus().getCode();
            if (code == Status.Code.NOT_FOUND) {
                return new SourceCurrency(0L, true);
            }
            if (code == Status.Code.PERMISSION_DENIED) {
                return new SourceCurrency(0L, false);
            }
            throw failure;
        }
    }

    // --------------------------------------------------------- request & capability validation

    private static void validateDestinationRenditionRules(PublishRepositorySinkRequest request) {
        SinkDestination dest = request.getDestination();
        RenditionDescriptor rend = dest.getRendition();
        if (!dest.getFilename().isEmpty() && !rend.getName().equals(ArchiveCoordinates.ORIGINAL)) {
            throw ArchiveErrors.invalid(
                    "destination.filename", "describes only the original rendition");
        }
        if (rend.getName().equals(ArchiveCoordinates.PARSED)) {
            if (!rend.getSubKey().isEmpty()) {
                throw ArchiveErrors.invalid(
                        "destination.rendition.sub_key",
                        "the parsed rendition is the document's PARSED part and takes no sub-key");
            }
            if (request.hasConversion()
                    && !PROTOBUF_MEDIA_TYPE.equals(request.getConversion().getMediaType())) {
                throw ArchiveErrors.invalid(
                        "destination.rendition.name",
                        "non-protobuf converted renditions cannot overwrite the 'parsed' protobuf part");
            }
        }
        if (request.hasConversion()
                && !rend.getMediaType().isEmpty()
                && !rend.getMediaType().equals(request.getConversion().getMediaType())) {
            throw ArchiveErrors.invalid(
                    "destination.rendition.media_type",
                    "must match conversion.media_type '"
                            + request.getConversion().getMediaType()
                            + "' when specified");
        }
    }

    private static RenditionDescriptor effectiveDestinationRendition(
            PublishRepositorySinkRequest request) {
        RenditionDescriptor rend = request.getDestination().getRendition();
        if (!rend.getMediaType().isEmpty()) {
            return rend;
        }
        String inferredMediaType = request.hasConversion()
                ? request.getConversion().getMediaType()
                : PROTOBUF_MEDIA_TYPE;
        return rend.toBuilder().setMediaType(inferredMediaType).build();
    }

    private static Optional<RendererCapability> findCapability(
            RepositorySinkRenderers activeRenderers, String providerId) {
        for (RendererCapability capability : activeRenderers.registry().capabilities()) {
            if (capability.getProviderId().equals(providerId)) {
                return Optional.of(capability);
            }
        }
        return Optional.empty();
    }

    private static Optional<String> verifyCapabilityMatchesSpec(
            RendererCapability capability, ConversionSpec spec) {
        if (!capability.getProviderVersion().equals(spec.getProviderVersion())) {
            return Optional.of("conversion.provider_version '" + spec.getProviderVersion()
                    + "' does not match enabled '" + capability.getProviderVersion() + "'");
        }
        if (!capability.getSourceProfile().equals(spec.getSourceProfile())) {
            return Optional.of("conversion.source_profile '" + spec.getSourceProfile()
                    + "' does not match enabled '" + capability.getSourceProfile() + "'");
        }
        if (!capability.getMediaType().equals(spec.getMediaType())) {
            return Optional.of("conversion.media_type '" + spec.getMediaType()
                    + "' does not match enabled '" + capability.getMediaType() + "'");
        }
        if (!capability.getExtension().equals(spec.getExtension())) {
            return Optional.of("conversion.extension '" + spec.getExtension()
                    + "' does not match enabled '" + capability.getExtension() + "'");
        }
        if (!capability.getOptionsType().equals(spec.getOptionsType())) {
            return Optional.of("conversion.options_type '" + spec.getOptionsType()
                    + "' does not match enabled '" + capability.getOptionsType() + "'");
        }
        if (!capability.getOptionsFingerprint().equals(spec.getOptionsFingerprint())) {
            return Optional.of("conversion.options_fingerprint '" + spec.getOptionsFingerprint()
                    + "' does not match enabled '" + capability.getOptionsFingerprint() + "'");
        }
        return Optional.empty();
    }

    private static PublishRepositorySinkRequest pinRequestSourceRevision(
            PublishRepositorySinkRequest request, String operationId, long frozenSourceRevision) {
        PublishRepositorySinkRequest.Builder builder =
                request.toBuilder().setOperationId(operationId);
        if (frozenSourceRevision > 0L
                && request.getSource().getKindCase()
                        == SinkSource.KindCase.REPOSITORY_REVISION) {
            RevisionSelector pinned = DocumentArchive.revision(frozenSourceRevision);
            RepositoryRevisionSource repoSource = request.getSource().getRepositoryRevision();
            RepositoryRevisionSource.Builder pinnedRepo = repoSource.toBuilder();
            if (repoSource.hasArchiveRendition()) {
                pinnedRepo.setArchiveRendition(
                        repoSource.getArchiveRendition().toBuilder().setRevision(pinned));
            } else if (repoSource.hasDocumentPart()) {
                pinnedRepo.setDocumentPart(
                        repoSource.getDocumentPart().toBuilder().setRevision(pinned));
            }
            builder.setSource(SinkSource.newBuilder().setRepositoryRevision(pinnedRepo));
        }
        return builder.build();
    }

    // --------------------------------------------------------- misc utilities

    private static long resolveRequestedVersion(RevisionSelector selector, Long pinnedOverride) {
        if (pinnedOverride != null && pinnedOverride > 0L) {
            return pinnedOverride;
        }
        if (selector != null && selector.hasRevision()) {
            return selector.getRevision();
        }
        return 0L;
    }

    private static RevisionSelector resolveRevisionSelector(
            RevisionSelector selector, Long pinnedOverride) {
        if (pinnedOverride != null && pinnedOverride > 0L) {
            return DocumentArchive.revision(pinnedOverride);
        }
        if (selector != null && selector.hasRevision()) {
            return DocumentArchive.revision(selector.getRevision());
        }
        return DocumentArchive.current();
    }

    private static void requireCompleteNodeAddress(NodeAddress address, String field) {
        if (address.getAccountId().isBlank()
                || address.getGraphId().isBlank()
                || address.getGraphAddressId().isBlank()
                || address.getDocId().isBlank()) {
            throw ArchiveErrors.invalid(
                    field,
                    "requires non-blank account_id, graph_id, graph_address_id, and doc_id");
        }
    }

    private static Optional<PartManifestEntry> findPartManifestEntry(
            DocumentManifest manifest, PartSelector selector) {
        for (PartManifestEntry entry : manifest.getPartsList()) {
            if (entry.getSelector().equals(selector)) {
                return Optional.of(entry);
            }
        }
        return Optional.empty();
    }

    private static Optional<PartSchemaBinding> findPartSchemaBinding(
            RevisionValidation validation, PartSelector selector) {
        for (PartSchemaBinding binding : validation.getPartSchemasList()) {
            if (binding.getPart().equals(selector)) {
                return Optional.of(binding);
            }
        }
        return Optional.empty();
    }

    private static String formatPartSelector(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case PART -> selector.getPart().name();
            case SEGMENTATION_ID -> "chunks:" + selector.getSegmentationId();
            case EMBEDDING_SET -> "embeddings:"
                    + selector.getEmbeddingSet().getSegmentationId()
                    + "/"
                    + selector.getEmbeddingSet().getEmbeddingSetId();
            case SELECTOR_NOT_SET -> "unspecified";
        };
    }

    private static String typeNameFromUrl(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }

    private static String entryUuid(EntryAddress address) {
        return RepositorySinkKeys.deriveUuidV5(
                        ENTRY_UUID_KIND,
                        address.getAccountId(),
                        address.getArchive() + '/' + address.getEntryId())
                .toString();
    }

    private static boolean isTerminalState(RepositorySinkJobState state) {
        return state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED;
    }

    private static boolean isUncertainCommitFailure(Status.Code code) {
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.UNKNOWN
                || code == Status.Code.INTERNAL;
    }

    private static boolean isTransientStatus(Status.Code code) {
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
    }

    private static RepositorySinkFailure failureOf(
            Status.Code code,
            String reasonCode,
            String message,
            boolean retryable,
            boolean conflict) {
        int numericCode = code == Status.Code.OK ? Status.Code.INTERNAL.value() : code.value();
        return RepositorySinkFailure.newBuilder()
                .setGrpcCode(numericCode)
                .setReasonCode(reasonCode)
                .setMessage(truncate(sanitizeMessage(message, reasonCode), MAX_FAILURE_MESSAGE_CHARS))
                .setRetryable(retryable)
                .setConflict(conflict)
                .build();
    }

    private static StatusRuntimeException toStatusException(RepositorySinkFailure failure) {
        Status status = Status.fromCodeValue(failure.getGrpcCode())
                .withDescription(failure.getMessage());
        return status.asRuntimeException();
    }

    private static String sanitizeMessage(String message, String fallback) {
        if (message == null || message.isBlank()) {
            return fallback == null || fallback.isBlank() ? "Sink failure" : fallback.trim();
        }
        return message.trim();
    }

    private static String truncate(String value, int maxChars) {
        return value.length() <= maxChars ? value : value.substring(0, maxChars);
    }

    private static PublishRepositorySinkResponse validPublishResponse(RepositorySinkJob job) {
        PublishRepositorySinkResponse response =
                PublishRepositorySinkResponse.newBuilder().setJob(job).build();
        DocumentArchive.requireValidView(response);
        return response;
    }

    private static CancelRepositorySinkJobResponse validCancelResponse(RepositorySinkJob job) {
        CancelRepositorySinkJobResponse response =
                CancelRepositorySinkJobResponse.newBuilder().setJob(job).build();
        DocumentArchive.requireValidView(response);
        return response;
    }

    private Timestamp now() {
        Instant instant = clock.instant();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private record ResolvedSource(
            long frozenSourceRevision,
            Descriptor rootDescriptor,
            Message message,
            ByteString rawPayload,
            ByteString descriptorSetBytes,
            String descriptorSha256,
            String schemaType,
            String schemaFingerprint,
            List<SourcePartDigest> sourceDigests,
            EntryAddress sourceEntry,
            RenditionDescriptor sourceRendition,
            NodeAddress sourceDocument,
            PartSelector sourcePart,
            String sourceOccurrenceFieldPath,
            String processingRunId,
            String processingStepName,
            String processingInputId) {}

    private record RetainedSourceSchema(
            String messageType, ByteString descriptorSetBytes, String descriptorSha256) {}

    private record StreamedPartRead(
            DocumentManifest manifest, PartManifestEntry entry, ByteString bytes) {}

    private record PathSegment(String fieldName, String subscript) {}

    private record ParsedMapKey(Object javaKey, RepositoryOccurrenceMapKey stepKey) {}

    private record UnpackedAny(
            Descriptor descriptor,
            Message message,
            ByteString valueBytes,
            String descriptorSha256) {}

    private record ResolvedOccurrenceValue(
            Descriptor descriptor,
            Message message,
            ByteString payloadBytes,
            String retainedDescriptorSha256) {}

    private record SourceCurrency(long currentSourceRevision, boolean sourceStale) {}
}
