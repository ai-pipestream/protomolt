package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.blob.PartStorage;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Bridge between the blocking handler bodies and the gRPC {@link StreamObserver}
 * unary contract, plus the service's single place for exception→status mapping.
 *
 * <p>Mapping policy (the wire contract of both services):
 * <ul>
 *   <li>explicit {@link StatusRuntimeException}/{@link StatusException} pass
 *   through unchanged — handler code raises INVALID_ARGUMENT / NOT_FOUND /
 *   FAILED_PRECONDITION / UNAVAILABLE at the decision point;</li>
 *   <li>{@link IllegalArgumentException} → INVALID_ARGUMENT (validation that
 *   slipped through, e.g. the storage-identity guards in
 *   {@code DocumentIds});</li>
 *   <li>{@link BlobStore.BlobNotFoundException} → NOT_FOUND;</li>
 *   <li>{@link PartStorage.PartObjectMissingException} → FAILED_PRECONDITION
 *   (the manifest claims a part is PRESENT but its object is gone);</li>
 *   <li>anything else → INTERNAL with only the exception message — never a
 *   stack trace — in the status description.</li>
 * </ul>
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
            case BlobStore.BlobNotFoundException _ -> Status.NOT_FOUND;
            case PartStorage.PartObjectMissingException _ -> Status.FAILED_PRECONDITION;
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

    static StatusRuntimeException notFound(String message) {
        return Status.NOT_FOUND.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException failedPrecondition(String message) {
        return Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException aborted(String message) {
        return Status.ABORTED.withDescription(message).asRuntimeException();
    }

    static StatusRuntimeException alreadyExists(String message) {
        return Status.ALREADY_EXISTS.withDescription(message).asRuntimeException();
    }
}
