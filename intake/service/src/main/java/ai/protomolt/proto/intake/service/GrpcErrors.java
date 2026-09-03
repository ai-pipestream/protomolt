package ai.protomolt.proto.intake.service;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between blocking handler bodies and the gRPC {@link StreamObserver}
 * contract, plus intake's single place for exception→status mapping.
 *
 * <p>Mapping policy: explicit status exceptions pass through unchanged — the
 * handlers raise INVALID_ARGUMENT / PERMISSION_DENIED / RESOURCE_EXHAUSTED at
 * the decision point, and a repo-service failure arrives as the repo call's
 * own {@link StatusRuntimeException} and is forwarded as-is (intake adds no
 * translation layer over repo's wire contract). {@link IllegalArgumentException}
 * → INVALID_ARGUMENT. Anything else → INTERNAL with only the exception
 * message — never a stack trace — in the status description.
 */
final class GrpcErrors {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcErrors.class);

    private GrpcErrors() {
    }

    /**
     * Runs {@code work} on the current (virtual) server thread, sends its
     * result, and converts any failure to a gRPC status error.
     *
     * @param <T> the response message type
     * @param observer the unary response observer
     * @param work the blocking handler body
     */
    static <T> void run(StreamObserver<T> observer, Supplier<T> work) {
        try {
            observer.onNext(work.get());
            observer.onCompleted();
        } catch (Throwable t) {
            observer.onError(map(t));
        }
    }

    /** Maps a handler failure to the gRPC status error per the policy above. */
    static Throwable map(Throwable t) {
        if (t instanceof StatusRuntimeException || t instanceof StatusException) {
            return t;
        }
        Status status = switch (t) {
            case IllegalArgumentException _ -> Status.INVALID_ARGUMENT;
            default -> {
                LOG.error("Unhandled handler failure", t);
                yield Status.INTERNAL;
            }
        };
        String message = t.getMessage();
        if (message != null && !message.isBlank()) {
            status = status.withDescription(message);
        }
        return status.asRuntimeException();
    }

    static StatusRuntimeException invalidArgument(String message) {
        return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException permissionDenied(String message) {
        return Status.PERMISSION_DENIED.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException resourceExhausted(String message) {
        return Status.RESOURCE_EXHAUSTED.withDescription(message).asRuntimeException();
    }
}
