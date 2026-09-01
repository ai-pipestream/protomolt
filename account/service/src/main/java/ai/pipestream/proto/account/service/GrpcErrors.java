package ai.pipestream.proto.account.service;

import ai.pipestream.proto.account.service.store.AccountStoreException;
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
 * <p>Mapping policy (the wire contract):
 * <ul>
 *   <li>explicit {@link StatusRuntimeException}/{@link StatusException} pass
 *   through unchanged — handler code raises INVALID_ARGUMENT / NOT_FOUND /
 *   ALREADY_EXISTS / UNAVAILABLE at the decision point;</li>
 *   <li>{@link IllegalArgumentException} → INVALID_ARGUMENT (validation that
 *   slipped through);</li>
 *   <li>{@link AccountStoreException} → NOT_FOUND / ALREADY_EXISTS per its
 *   kind, INTERNAL when unclassified;</li>
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
            case AccountStoreException store -> switch (store.kind()) {
                case NOT_FOUND -> Status.NOT_FOUND;
                case CONFLICT -> Status.ALREADY_EXISTS;
                case NONE -> {
                    LOG.error("Unhandled store failure", t);
                    yield Status.INTERNAL;
                }
            };
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
}
