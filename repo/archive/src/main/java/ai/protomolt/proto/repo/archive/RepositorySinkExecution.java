package ai.protomolt.proto.repo.archive;

import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.decodeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.failureOf;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.isTransientStatus;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.sanitizeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.toStatusException;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.validPublishResponse;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.effectiveDestinationRendition;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.entryUuid;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.findCapability;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.storedJobIntentSha;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.timestamp;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.verifyCapabilityMatchesSpec;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.RepositorySinkSources.ResolvedSource;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

/**
 * Runs {@link RepositorySink} publications: replays or retries an existing operation, dispatches
 * a job synchronously or on the async executor, and executes it (render, upload, commit) under the
 * durable lease, handing every terminal outcome to {@link RepositorySinkSettlement}.
 */
final class RepositorySinkExecution {
    private final DocumentArchive archive;
    private final RepositorySinkAuthorizer authorizer;
    private final Supplier<RepositorySinkRenderers> renderers;
    private final Clock clock;
    private final RepositorySinkLeaseCoordinator leases;
    private final ExecutorService asyncExecutor;
    private final RepositorySinkSources sources;
    private final RepositorySinkSettlement settlement;
    private final RepositorySinkUpload upload;

    RepositorySinkExecution(
            DocumentArchive archive,
            RepositorySinkAuthorizer authorizer,
            Supplier<RepositorySinkRenderers> renderers,
            Clock clock,
            RepositorySinkLeaseCoordinator leases,
            ExecutorService asyncExecutor,
            RepositorySinkSources sources,
            RepositorySinkSettlement settlement,
            RepositorySinkUpload upload) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        this.renderers = Objects.requireNonNull(renderers, "renderers");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor, "asyncExecutor");
        this.sources = Objects.requireNonNull(sources, "sources");
        this.settlement = Objects.requireNonNull(settlement, "settlement");
        this.upload = Objects.requireNonNull(upload, "upload");
    }

    PublishRepositorySinkResponse handleExistingPublish(
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

            // Preserve the retry admission gate before changing a failed publication's
            // receipt, even when reconciliation discovers a late committed outcome.
            if (cursor.job().getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                    && cursor.job().getFailure().getRetryable()) {
                requireRetryWriteAccess(cursor, accountId, caller);
            }
            RepositorySinkJobStore.StoredJob reconciled =
                    settlement.reconcileIfNeeded(cursor, jobKey, caller, false);
            RepositorySinkJob reconciledJob = reconciled.job();

            switch (reconciledJob.getState()) {
                case REPOSITORY_SINK_JOB_STATE_SUCCEEDED -> {
                    if (!explicitOperationId
                            && incomingRequest.getDestination().getExpectedDestinationVersion() == 0
                            && !incomingRequest.getDestination().getCreateOnly()) {
                        settlement.requireDestinationStillAtVersion(
                                caller,
                                accountId,
                                incomingRequest.getDestination().getAddress(),
                                operationId,
                                reconciledJob.getDestinationVersion());
                    }
                    RepositorySinkJob replayed = settlement.markReplayedReceipt(reconciledJob);
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
                    // Authority and the frozen source are settled before the failure is
                    // cleared, so a caller who may not retry leaves the job as it was.
                    RetryInputs retry = resolveRetryInputs(reconciled, accountId, caller);
                    RepositorySinkJob requeuedJob = reconciledJob.toBuilder()
                            .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED)
                            .setBytesRendered(0L)
                            .setBytesCommitted(0L)
                            .clearFailure()
                            .setUpdatedAt(timestamp(clock))
                            .build();
                    DocumentArchive.requireValidView(requeuedJob);
                    Optional<RepositorySinkJobStore.StoredJob> claimedRetry =
                            leases.tryClaimRetryableFailure(
                                    accountId,
                                    operationId,
                                    reconciled.lease().leaseEpoch(),
                                    requeuedJob);
                    if (claimedRetry.isPresent()) {
                        return dispatchExecution(
                                claimedRetry.get(),
                                retry.source(),
                                retry.destinationBinding(),
                                jobKey,
                                caller,
                                incomingRequest.getAsync());
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

    /** What a retry runs with: the destination it may write and the source it froze. */
    private record RetryInputs(ArchiveBinding destinationBinding, ResolvedSource source) {}

    private ArchiveBinding requireRetryWriteAccess(
            RepositorySinkJobStore.StoredJob stored,
            String accountId,
            RepositorySinkAuthorizer.CapturedCaller caller) {
        SinkDestination dest = stored.request().getDestination();
        ArchiveBinding binding = caller.callForRead(accountId, () -> archive.binding(dest.getAddress()));
        authorizer.checkWriteAccess(dest.getAddress(), binding.datasourceId(), dest.getWrittenBy());
        return binding;
    }

    /**
     * Checks that the caller may write the stored destination and resolves the stored source at
     * its frozen revision. Changes nothing, so it runs before a retry claims the job.
     */
    private RetryInputs resolveRetryInputs(
            RepositorySinkJobStore.StoredJob stored,
            String accountId,
            RepositorySinkAuthorizer.CapturedCaller caller) {
        ArchiveBinding destBinding = requireRetryWriteAccess(stored, accountId, caller);
        ResolvedSource source = caller.callForRead(
                accountId,
                () -> sources.resolveSource(
                        stored.request().getSource(),
                        stored.job().getFrozenSourceRevision()));
        return new RetryInputs(destBinding, source);
    }

    PublishRepositorySinkResponse retryFromFrozenRevision(
            RepositorySinkJobStore.StoredJob stored,
            PublishRepositorySinkRequest incomingRequest,
            String accountId,
            RepositorySinkLeaseCoordinator.JobKey jobKey,
            RepositorySinkAuthorizer.CapturedCaller caller) {
        RetryInputs retry = resolveRetryInputs(stored, accountId, caller);
        return dispatchExecution(
                stored,
                retry.source(),
                retry.destinationBinding(),
                jobKey,
                caller,
                incomingRequest.getAsync());
    }

    PublishRepositorySinkResponse dispatchExecution(
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

    PublishRepositorySinkResponse finishSyncPublish(RepositorySinkJob settled) {
        if (settled.getState() == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED) {
            return validPublishResponse(settled);
        }
        throw toStatusException(settled.getFailure());
    }

    static RepositorySinkJob awaitInFlight(
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

    RepositorySinkJob runGuarded(
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
                RepositorySinkJob latest = settlement.latestJobOr(jobKey, stored.job());
                tracker.completion.complete(latest);
                if (unexpected instanceof Error err) {
                    throw err;
                }
                return latest;
            }
            if (tracker.cancelled.get() && !tracker.enteringCommit.get()) {
                RepositorySinkJob cancelledJob = settlement.markCancelled(stored, tracker.reasonOrDefault());
                tracker.completion.complete(cancelledJob);
                if (unexpected instanceof Error err) {
                    throw err;
                }
                return cancelledJob;
            }
            if (tracker.shuttingDown.get() && !tracker.enteringCommit.get()) {
                RepositorySinkJob shutdownJob = settlement.persistShutdownInterruption(stored, 0L);
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
            RepositorySinkJob failedJob = settlement.persistFailure(
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

    RepositorySinkJob executeJob(
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
            return settlement.markCancelled(initialStored, reason);
        }
        if (tracker.shuttingDown.get()) {
            return settlement.persistShutdownInterruption(initialStored, 0L);
        }
        if (tracker.leaseLost.get()) {
            return settlement.latestJobOr(jobKey, initialStored.job());
        }

        RepositorySinkJob runningJob = initialStored.job().toBuilder()
                .setState(RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING)
                .clearFailure()
                .setUpdatedAt(timestamp(clock))
                .build();
        DocumentArchive.requireValidView(runningJob);
        RepositorySinkJobStore.StoredJob stored = initialStored.withTransition(
                runningJob, initialStored.pendingProvenance(), leases.nextExpiration());
        if (!leases.transitionOwned(stored, leaseEpoch)) {
            tracker.markLeaseLost();
            return settlement.latestJobOr(jobKey, runningJob);
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
                return settlement.persistFailure(
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
                return settlement.persistFailure(
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
                return settlement.persistFailure(
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
                return settlement.persistStatusFailure(stored, invalidOptions, "INVALID_RENDER_OPTIONS", 0L, null);
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
                return settlement.persistRenderFailure(stored, renderFailure, jobKey, tracker);
            } catch (IOException ioFailure) {
                return settlement.persistFailure(
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
                return settlement.persistRenderFailure(stored, cancelled, jobKey, tracker);
            }
            outputBytes = source.rawPayload();
            retainedDescriptorsForOutput = source.descriptorSetBytes();
        }

        long bytesRendered = outputBytes.size();
        String outputSha256 = RenditionReads.sha256Hex(outputBytes);
        Timestamp provisionalCommittedAt = timestamp(clock);
        RenditionProvenance provisionalProvenance = sources.buildProvenance(
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
            return settlement.latestJobOr(jobKey, stored.job());
        }
        if (tracker.cancelled.get()) {
            return settlement.markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
        }
        if (tracker.shuttingDown.get()) {
            return settlement.persistShutdownInterruption(stored, bytesRendered);
        }
        if (Thread.currentThread().isInterrupted()) {
            return settlement.markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
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
                return settlement.markCancelledWithRendered(preCommitFence.stored(), reason, bytesRendered);
            }
            case LEASE_LOST -> {
                tracker.markLeaseLost();
                return preCommitFence.stored().job();
            }
            case READY -> stored = preCommitFence.stored();
        }

        tracker.enteringCommit.set(true);
        if (tracker.cancelled.get()) {
            return settlement.markCancelledWithRendered(stored, tracker.reasonOrDefault(), bytesRendered);
        }
        if (tracker.leaseLost.get()) {
            return settlement.latestJobOr(jobKey, stored.job());
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
                    () -> upload.streamUpload(
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
            return settlement.handleUploadFailure(
                    stored, jobKey, commitOperationId, bytesRendered, caller, tracker, uploadFailure);
        }

        Timestamp committedAt = timestamp(clock);
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
            return settlement.latestJobOr(jobKey, succeeded);
        }
        return succeeded;
    }
}
