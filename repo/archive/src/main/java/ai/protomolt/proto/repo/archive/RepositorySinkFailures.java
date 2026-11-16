package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.CancelRepositorySinkJobResponse;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkResponse;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkFailure;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Failure classification and response validation shared by the {@link RepositorySink} steps.
 */
final class RepositorySinkFailures {
    static final int MAX_FAILURE_MESSAGE_CHARS = 2000;

    private RepositorySinkFailures() {}

    static boolean isTerminalState(RepositorySinkJobState state) {
        return state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED;
    }

    static boolean isUncertainCommitFailure(Status.Code code) {
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.UNKNOWN
                || code == Status.Code.INTERNAL;
    }

    static boolean isTransientStatus(Status.Code code) {
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
    }

    static RepositorySinkFailure failureOf(
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

    /**
     * The failure recorded when an upload failed with an uncertain status and the repository
     * has no outcome for it yet. No outcome yet does not prove none will arrive, so the
     * publication stays retryable whatever the status: the retry replays a commit that landed
     * late instead of leaving it reported as failed.
     */
    static RepositorySinkFailure commitNotRecorded(Status.Code code, String description) {
        return failureOf(
                code,
                "COMMIT_NOT_RECORDED",
                sanitizeMessage(description, "Upload failed before repository commit"),
                true,
                false);
    }

    static StatusRuntimeException toStatusException(RepositorySinkFailure failure) {
        Status status = Status.fromCodeValue(failure.getGrpcCode())
                .withDescription(failure.getMessage());
        return status.asRuntimeException();
    }

    static String sanitizeMessage(String message, String fallback) {
        if (message == null || message.isBlank()) {
            return fallback == null || fallback.isBlank() ? "Sink failure" : fallback.trim();
        }
        return message.trim();
    }

    static String truncate(String value, int maxChars) {
        return value.length() <= maxChars ? value : value.substring(0, maxChars);
    }

    static PublishRepositorySinkResponse validPublishResponse(RepositorySinkJob job) {
        PublishRepositorySinkResponse response =
                PublishRepositorySinkResponse.newBuilder().setJob(job).build();
        DocumentArchive.requireValidView(response);
        return response;
    }

    static CancelRepositorySinkJobResponse validCancelResponse(RepositorySinkJob job) {
        CancelRepositorySinkJobResponse response =
                CancelRepositorySinkJobResponse.newBuilder().setJob(job).build();
        DocumentArchive.requireValidView(response);
        return response;
    }
}
