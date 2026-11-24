package ai.protomolt.proto.repo.archive;

import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.isTerminalState;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.sanitizeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.truncate;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.validCancelResponse;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.effectiveDestinationRendition;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.findPartManifestEntry;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.pinRequestSourceRevision;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.timestamp;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.validateDestinationRenditionRules;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.renderer.RendererRegistry;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.RepositorySinkSources.ResolvedSource;
import ai.protomolt.proto.repo.archive.RepositorySinkSources.SourceCurrency;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobRequest;
import ai.protomolt.proto.repo.sink.v1.GetRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceRequest;
import ai.protomolt.proto.repo.sink.v1.ListRenditionProvenanceResponse;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesRequest;
import ai.protomolt.proto.repo.sink.v1.ListSaveAsCapabilitiesResponse;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenanceEntry;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.SaveAsCapability;
import ai.protomolt.proto.repo.sink.v1.SaveAsCompatibilityStatus;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private static final int MAX_INCOMPATIBILITY_REASON_CHARS = 1000;

    private final DocumentArchive archive;
    private final RenditionUploads uploads;
    private final RenditionReads reads;
    private final Supplier<RepositorySinkRenderers> renderers;
    private final RepositorySinkAuthorizer authorizer;
    private final Clock clock;
    private final RepositorySinkLeaseCoordinator leases;
    private final ExecutorService asyncExecutor;
    private final RepositorySinkSources sources;
    private final RepositorySinkSettlement settlement;
    private final RepositorySinkExecution execution;

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
        this.sources = new RepositorySinkSources(archive, reads, new RepositorySinkOccurrences(archive));
        this.settlement = new RepositorySinkSettlement(archive, clock, leases);
        this.execution = new RepositorySinkExecution(
                archive,
                authorizer,
                renderers,
                clock,
                leases,
                asyncExecutor,
                sources,
                settlement,
                new RepositorySinkUpload(uploads, leases));
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
                caller.callForRead(accountId, () -> sources.resolveSource(request.getSource(), null));

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
            return execution.handleExistingPublish(
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
                caller.callForRead(accountId, () -> sources.resolveSource(request.getSource(), null));

        Timestamp admittedAt = timestamp(clock);
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
            return execution.handleExistingPublish(
                    persisted,
                    request,
                    accountId,
                    operationId,
                    intentSha256,
                    explicitOperationId,
                    jobKey,
                    caller);
        }

        return execution.dispatchExecution(
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
        RepositorySinkJobStore.StoredJob settled = settlement.reconcileIfNeeded(stored, key, caller, true);
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
        // Cancelling stops a write, so it takes the same authority on the stored destination
        // as the publication it stops.
        SinkDestination destination = stored.request().getDestination();
        ArchiveBinding destinationBinding =
                caller.callForRead(accountId, () -> archive.binding(destination.getAddress()));
        authorizer.checkWriteAccess(
                destination.getAddress(),
                destinationBinding.datasourceId(),
                destination.getWrittenBy());
        RepositorySinkLeaseCoordinator.JobKey key =
                new RepositorySinkLeaseCoordinator.JobKey(accountId, operationId);
        if (isTerminalState(stored.job().getState())) {
            return validCancelResponse(settlement.reconcileIfNeeded(stored, key, caller, false).job());
        }

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
                settlement.reconcileIfNeeded(stored, key, caller, false);
        if (isTerminalState(reconciled.job().getState())) {
            return validCancelResponse(reconciled.job());
        }
        if (reconciled.lease().ownerId().equals(leases.instanceId())) {
            return validCancelResponse(settlement.markCancelled(reconciled, reason));
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
                SourceCurrency currency = sources.inspectSourceCurrency(provenance);
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

}
