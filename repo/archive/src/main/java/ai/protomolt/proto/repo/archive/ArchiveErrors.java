package ai.protomolt.proto.repo.archive;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/** The archive's own refusals, each naming the field or rule it is about. */
final class ArchiveErrors {
    private ArchiveErrors() {
    }

    static StatusRuntimeException invalid(String field, String message) {
        return Status.INVALID_ARGUMENT.withDescription(field + ": " + message).asRuntimeException();
    }

    static StatusRuntimeException notFound(String message) {
        return Status.NOT_FOUND.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException failedPrecondition(String message) {
        return Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException alreadyExists(String field, String message) {
        return Status.ALREADY_EXISTS.withDescription(field + ": " + message).asRuntimeException();
    }

    static StatusRuntimeException dataLoss(String message, Throwable cause) {
        return Status.DATA_LOSS.withDescription(message).withCause(cause).asRuntimeException();
    }

    static StatusRuntimeException unimplemented(String message) {
        return Status.UNIMPLEMENTED.withDescription(message).asRuntimeException();
    }
}
