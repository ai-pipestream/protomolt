package ai.protomolt.proto.repo.archive;

import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.failureOf;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.isTerminalState;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.isTransientStatus;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.isUncertainCommitFailure;
import static ai.protomolt.proto.repo.archive.RepositorySinkFailures.sanitizeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.entryUuid;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.findPartManifestEntry;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.timestamp;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeResponse;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.SaveRejection;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

/**
 * Terminal state transitions for {@link RepositorySink} jobs: failure, cancellation and shutdown
 * records written under the current lease epoch, and reconciliation of an uncertain or orphaned
 * commit from the catalog's recorded outcome.
 */
final class RepositorySinkSettlement {
    private final DocumentArchive archive;
    private final Clock clock;
    private final RepositorySinkLeaseCoordinator leases;

    RepositorySinkSettlement(
            DocumentArchive archive,
            Clock clock,
            RepositorySinkLeaseCoordinator leases) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.leases = Objects.requireNonNull(leases, "leases");
    }

    RepositorySinkJob handleUploadFailure(
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

    RepositorySinkJob reconcileAfterUncertainCommit(
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

        RepositorySinkFailure notCommitted = RepositorySinkFailures.commitNotRecorded(
                originalFailure.getStatus().getCode(),
                originalFailure.getStatus().getDescription());
        return persistFailure(
                stored,
                RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED,
                notCommitted,
                bytesRendered,
                stored.pendingProvenance());
    }

    RepositorySinkJobStore.StoredJob reconcileIfNeeded(
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

    RepositorySinkJobStore.StoredJob reconcileClaimedOrphan(
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

    RepositorySinkJob settleFromRecordedOutcome(
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
                .setUpdatedAt(timestamp(clock))
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

    Optional<RenditionProvenance> readProvenanceFromCommittedEntry(
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

    RepositorySinkJob persistRenderFailure(
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

    RepositorySinkJob persistStatusFailure(
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

    RepositorySinkJob persistShutdownInterruption(
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

    RepositorySinkJob markCancelled(
            RepositorySinkJobStore.StoredJob stored, String reason) {
        return markCancelledWithRendered(stored, reason, stored.job().getBytesRendered());
    }

    RepositorySinkJob markCancelledWithRendered(
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

    RepositorySinkJob persistFailure(
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
                .setUpdatedAt(timestamp(clock))
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

    RepositorySinkJob latestJobOr(
            RepositorySinkLeaseCoordinator.JobKey key, RepositorySinkJob fallback) {
        return leases.find(key.accountId(), key.operationId())
                .map(RepositorySinkJobStore.StoredJob::job)
                .orElse(fallback);
    }

    RepositorySinkJob markReplayedReceipt(RepositorySinkJob succeeded) {
        RenditionProvenance replayedProvenance =
                succeeded.getProvenance().toBuilder().setReplayed(true).build();
        RepositorySinkJob replayed = succeeded.toBuilder()
                .setReplayed(true)
                .setProvenance(replayedProvenance)
                .build();
        DocumentArchive.requireValidView(replayed);
        return replayed;
    }

    void requireDestinationStillAtVersion(
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
}
