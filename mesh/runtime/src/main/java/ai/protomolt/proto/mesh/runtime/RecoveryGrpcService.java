package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.CancelScheduledRetryRequest;
import ai.protomolt.proto.mesh.runtime.v1.ChangeDeadLetterStatusRequest;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterEntry;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterRecord;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.GetDeadLetterRequest;
import ai.protomolt.proto.mesh.runtime.v1.ListDeadLettersRequest;
import ai.protomolt.proto.mesh.runtime.v1.ListDeadLettersResponse;
import ai.protomolt.proto.mesh.runtime.v1.ReconcileRecoveryRequest;
import ai.protomolt.proto.mesh.runtime.v1.ReconcileRecoveryResponse;
import ai.protomolt.proto.mesh.runtime.v1.ReconciliationClassification;
import ai.protomolt.proto.mesh.runtime.v1.ReconciliationFinding;
import ai.protomolt.proto.mesh.runtime.v1.RecoveryServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.ReplayDeadLetterRequest;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/** Recovery-center RPCs over channel evidence and the existing flow replay engine. */
public final class RecoveryGrpcService
        extends RecoveryServiceGrpc.RecoveryServiceImplBase {

    private final DurableProcessorChannel channel;
    private final DurableFlowCoordinator flows;
    private final RecoveryReconciler reconciler;
    private final DeadLetterPayloadControl payloadControl;
    private final Clock clock;

    public RecoveryGrpcService(
            DurableProcessorChannel channel,
            DurableFlowCoordinator flows,
            RecoveryReconciler reconciler,
            Clock clock) {
        this(channel, flows, reconciler, DeadLetterPayloadControl.none(), clock);
    }

    public RecoveryGrpcService(
            DurableProcessorChannel channel,
            DurableFlowCoordinator flows,
            RecoveryReconciler reconciler,
            DeadLetterPayloadControl payloadControl,
            Clock clock) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.flows = Objects.requireNonNull(flows, "flows");
        this.reconciler = Objects.requireNonNull(reconciler, "reconciler");
        this.payloadControl = Objects.requireNonNull(payloadControl, "payloadControl");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void listDeadLetters(ListDeadLettersRequest request,
            StreamObserver<ListDeadLettersResponse> response) {
        unary(response, () -> {
            if (request.getNamespace().isBlank()) {
                throw new IllegalArgumentException("dead-letter-namespace-required");
            }
            int limit = request.getLimit() == 0 ? 100 : request.getLimit();
            if (limit < 1 || limit > 1_000) {
                throw new IllegalArgumentException("dead-letter-limit-exceeded");
            }
            DurableProcessorChannel.DeadLetterPage page = channel.deadLetters(
                    request.getNamespace(), request.getAfterSequence(), limit);
            ListDeadLettersResponse.Builder result = ListDeadLettersResponse.newBuilder();
            for (DurableProcessorChannel.DeadLetterPage.Entry entry : page.entries()) {
                result.addEntries(DeadLetterEntry.newBuilder()
                        .setSequence(entry.sequence()).setRecord(entry.record()));
            }
            return result.setNextSequence(page.nextSequence()).build();
        });
    }

    @Override
    public void getDeadLetter(GetDeadLetterRequest request,
            StreamObserver<DeadLetterRecord> response) {
        unary(response, () -> require(request.getDeadLetterId()));
    }

    @Override
    public void replayDeadLetter(ReplayDeadLetterRequest request,
            StreamObserver<DeadLetterRecord> response) {
        unary(response, () -> {
            DeadLetterRecord record = require(request.getDeadLetterId());
            RemoteValidation.uuid(request.getReplayRunId(), "replay_run_id");
            if (record.getSourceHistorySequence() == 0
                    || record.getWorkflowName().isBlank()
                    || record.getWorkflowVersion().isBlank()
                    || record.getPlanFingerprint().isBlank()) {
                throw new IllegalStateException(
                        "dead-letter-replay-identity-missing: source frontier is incomplete");
            }
            validateReplayIdentity(record);
            if (!record.getReplayRunId().isBlank()
                    && !record.getReplayRunId().equals(request.getReplayRunId())) {
                throw new LifecycleConflictException(
                        "dead-letter-replay-conflict: record already names replay run "
                                + record.getReplayRunId());
            }
            if (record.getReplayRunId().equals(request.getReplayRunId())) {
                return replayWithStatus(record, request.getReplayRunId());
            }
            DeadLetterRecord requested = channel.changeDeadLetterStatus(
                    record.getDeadLetterId(),
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_REQUESTED,
                    request.getReplayRunId(), "replay requested", record.getRetentionHold());
            return replayWithStatus(requested, request.getReplayRunId());
        });
    }

    private DeadLetterRecord replayWithStatus(DeadLetterRecord record, String replayRunId) {
        try {
            return finishReplay(record, replayRunId);
        } catch (RuntimeException failure) {
            channel.changeDeadLetterStatus(record.getDeadLetterId(),
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_FAILED,
                    replayRunId, bounded(failure.getMessage()), record.getRetentionHold());
            throw failure;
        }
    }

    private DeadLetterRecord finishReplay(DeadLetterRecord record, String replayRunId) {
        DurableFlowRun replay = flows.replay(record.getRunId(), replayRunId,
                List.of(record.getSourceHistorySequence()));
        DeadLetterReplayStatus status = switch (replay.getState()) {
            case DURABLE_RUN_STATE_COMPLETED ->
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_COMPLETED;
            case DURABLE_RUN_STATE_FAILED, DURABLE_RUN_STATE_CANCELLED ->
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_FAILED;
            default -> DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_STARTED;
        };
        if (record.getReplayStatus() == status
                && record.getReplayRunId().equals(replayRunId)) {
            return record;
        }
        return channel.changeDeadLetterStatus(record.getDeadLetterId(), status,
                replayRunId, "replay delegated", record.getRetentionHold());
    }

    private void validateReplayIdentity(DeadLetterRecord record) {
        DurableFlowRun source = flows.get(record.getRunId());
        if (!source.getWorkflowName().equals(record.getWorkflowName())
                || !source.getWorkflowVersion().equals(record.getWorkflowVersion())
                || !source.getPlanFingerprint().equals(record.getPlanFingerprint())
                || source.getDeploymentRevision() != record.getDeploymentRevision()) {
            throw new IllegalStateException(
                    "dead-letter-replay-identity-mismatch: source run identity differs");
        }
        HistoryEvent frontier = source.getHistory().getEventsList().stream()
                .filter(event -> event.getSequence() == record.getSourceHistorySequence())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "dead-letter-replay-frontier-missing: source history sequence is absent"));
        if (!frontier.hasMessage()
                || !frontier.getMessage().equals(record.getInput())
                || !frontier.getNodeId().equals(record.getNodeId())
                || !frontier.getProcessorId().equals(record.getProcessorId())
                || !frontier.getEdgeId().equals(record.getEdgeId())) {
            throw new IllegalStateException(
                    "dead-letter-replay-frontier-mismatch: source evidence differs");
        }
    }

    @Override
    public void cancelScheduledRetry(CancelScheduledRetryRequest request,
            StreamObserver<DeadLetterRecord> response) {
        unary(response, () -> channel.cancelRetry(
                request.getDeliveryId(), request.getReason(), clock.instant()));
    }

    @Override
    public void acknowledgeDeadLetter(ChangeDeadLetterStatusRequest request,
            StreamObserver<DeadLetterRecord> response) {
        unary(response, () -> {
            DeadLetterRecord record = require(request.getDeadLetterId());
            payloadControl.retain(record, request.getRetain());
            return channel.changeDeadLetterStatus(record.getDeadLetterId(),
                DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_ACKNOWLEDGED,
                "", request.getReason(), request.getRetain());
        });
    }

    @Override
    public void retainDeadLetter(ChangeDeadLetterStatusRequest request,
            StreamObserver<DeadLetterRecord> response) {
        unary(response, () -> {
            DeadLetterRecord record = require(request.getDeadLetterId());
            payloadControl.retain(record, request.getRetain());
            return channel.changeDeadLetterStatus(record.getDeadLetterId(),
                    record.getReplayStatus(), record.getReplayRunId(), request.getReason(),
                    request.getRetain());
        });
    }

    @Override
    public void reconcileRecovery(ReconcileRecoveryRequest request,
            StreamObserver<ReconcileRecoveryResponse> response) {
        unary(response, () -> {
            if (request.getRepair()) {
                if (!request.hasNotBefore()
                        || !RemoteValidation.instant(request.getNotBefore())
                        .isBefore(clock.instant())) {
                    throw new IllegalArgumentException(
                            "reconciliation-repair-age-guard-required");
                }
            }
            int limit = request.getLimit() == 0 ? 1_000 : request.getLimit();
            if (limit > 10_000) {
                throw new IllegalArgumentException("reconciliation-limit-exceeded");
            }
            return reconciler.reconcile(request, limit);
        });
    }

    private DeadLetterRecord require(String deadLetterId) {
        RemoteValidation.uuid(deadLetterId, "dead_letter_id");
        return channel.deadLetter(deadLetterId)
                .orElseThrow(() -> new LifecycleNotFoundException(
                        "unknown dead_letter_id " + deadLetterId));
    }

    private static <T> void unary(StreamObserver<T> response,
            java.util.function.Supplier<T> operation) {
        try {
            response.onNext(operation.get());
            response.onCompleted();
        } catch (RuntimeException failure) {
            Status status = failure instanceof LifecycleNotFoundException
                    ? Status.NOT_FOUND
                    : failure instanceof LifecycleConflictException
                    ? Status.ABORTED
                    : failure instanceof IllegalArgumentException
                    ? Status.INVALID_ARGUMENT : Status.FAILED_PRECONDITION;
            response.onError(status.withDescription(bounded(failure.getMessage()))
                    .withCause(failure).asRuntimeException());
        }
    }

    private static String bounded(String message) {
        String value = message == null ? "recovery operation failed" : message;
        return value.substring(0, Math.min(8192, value.length()));
    }

    @FunctionalInterface
    public interface RecoveryReconciler {
        ReconcileRecoveryResponse reconcile(ReconcileRecoveryRequest request, int limit);

        static RecoveryReconciler reportOnly() {
            return (request, limit) -> ReconcileRecoveryResponse.newBuilder()
                    .addFindings(ReconciliationFinding.newBuilder()
                            .setStableId(request.getNamespace())
                            .setClassification(ReconciliationClassification
                                    .RECONCILIATION_CLASSIFICATION_UNKNOWN)
                            .setEvidence("payload or broker evidence source is not configured")
                            .setRepaired(false))
                    .build();
        }
    }

    @FunctionalInterface
    public interface DeadLetterPayloadControl {
        void retain(DeadLetterRecord record, boolean retained);

        static DeadLetterPayloadControl none() {
            return (record, retained) -> {
                if (record.getInput().hasClaimCheck()) {
                    throw new IllegalStateException(
                            "dead-letter-payload-store-unavailable");
                }
            };
        }
    }
}
