package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.blob.PartStorage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.function.Supplier;

/**
 * Exception to status mapping for the v1 document services. It adds the v1
 * part-storage failure to {@link GrpcErrors}:
 * {@link PartStorage.PartObjectMissingException} maps to FAILED_PRECONDITION
 * (the manifest claims a part is PRESENT but its object is gone). Every other
 * failure maps exactly as {@link GrpcErrors#map} does.
 */
final class DocumentGrpcErrors {

    private DocumentGrpcErrors() {
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

    /** Maps a v1 document handler failure to its gRPC status error. */
    static Throwable map(Throwable t) {
        if (t instanceof PartStorage.PartObjectMissingException) {
            return GrpcErrors.describe(Status.FAILED_PRECONDITION, t);
        }
        return GrpcErrors.map(t);
    }
}
