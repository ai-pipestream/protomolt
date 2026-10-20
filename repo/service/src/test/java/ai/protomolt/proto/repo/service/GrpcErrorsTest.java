package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.spi.RepositoryException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the exception→status mapping policy of {@link GrpcErrors} —
 * the wire contract of both gRPC services — and for the {@code run} bridge
 * between blocking handler bodies and the unary {@link StreamObserver}.
 */
class GrpcErrorsTest {

    // ------------------------------------------------------------------ map

    @Test
    void explicitStatusErrorsPassThroughUnchanged() {
        StatusRuntimeException runtime =
                Status.ALREADY_EXISTS.withDescription("row exists").asRuntimeException();
        assertThat(GrpcErrors.map(runtime)).isSameAs(runtime);

        StatusException checked = Status.UNAVAILABLE.withDescription("retry later").asException();
        assertThat(GrpcErrors.map(checked)).isSameAs(checked);
    }

    @Test
    void illegalArgumentMapsToInvalidArgumentKeepingTheMessage() {
        Throwable mapped = GrpcErrors.map(new IllegalArgumentException("node_id must be a UUID"));
        assertThat(mapped).isInstanceOf(StatusRuntimeException.class);
        Status status = Status.fromThrowable(mapped);
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).isEqualTo("node_id must be a UUID");
    }

    @Test
    void blobNotFoundMapsToNotFound() {
        Throwable mapped = GrpcErrors.map(
                new BlobStore.BlobNotFoundException("no object at bucket/k", null));
        Status status = Status.fromThrowable(mapped);
        assertThat(status.getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(status.getDescription()).isEqualTo("no object at bucket/k");
    }

    @ParameterizedTest
    @MethodSource("repositoryCodes")
    void repositoryExceptionMapsItsCodeKeepingOnlyTheMessage(RepositoryException.Code code, Status.Code expected) {
        Throwable mapped = GrpcErrors.map(new RepositoryException(code, "repository said " + code,
                new IllegalStateException("provider detail")));
        assertThat(mapped).isInstanceOf(StatusRuntimeException.class);
        Status status = Status.fromThrowable(mapped);
        assertThat(status.getCode()).isEqualTo(expected);
        assertThat(status.getDescription()).isEqualTo("repository said " + code);
        assertThat(mapped.getCause()).isNull();
    }

    @Test
    void everyRepositoryCodeHasAnExpectedStatus() {
        assertThat(repositoryCodes()).hasSize(RepositoryException.Code.values().length);
    }

    static Stream<Arguments> repositoryCodes() {
        return Stream.of(RepositoryException.Code.values()).map(code -> Arguments.of(code, switch (code) {
            case INVALID_ARGUMENT -> Status.Code.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.Code.NOT_FOUND;
            case FAILED_PRECONDITION -> Status.Code.FAILED_PRECONDITION;
            case PERMISSION_DENIED -> Status.Code.PERMISSION_DENIED;
            case CONFLICT -> Status.Code.ABORTED;
            case UNSUPPORTED -> Status.Code.UNIMPLEMENTED;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case INTERNAL -> Status.Code.INTERNAL;
            case CANCELLED -> Status.Code.CANCELLED;
            case UNKNOWN -> Status.Code.UNKNOWN;
            case DEADLINE_EXCEEDED -> Status.Code.DEADLINE_EXCEEDED;
            case ALREADY_EXISTS -> Status.Code.ALREADY_EXISTS;
            case RESOURCE_EXHAUSTED -> Status.Code.RESOURCE_EXHAUSTED;
            case OUT_OF_RANGE -> Status.Code.OUT_OF_RANGE;
            case DATA_LOSS -> Status.Code.DATA_LOSS;
            case UNAUTHENTICATED -> Status.Code.UNAUTHENTICATED;
        }));
    }

    @Test
    void repositoryExceptionWithoutAMessageLeavesNoDescription() {
        Status status = Status.fromThrowable(GrpcErrors.map(
                new RepositoryException(RepositoryException.Code.CONFLICT, null)));
        assertThat(status.getCode()).isEqualTo(Status.Code.ABORTED);
        assertThat(status.getDescription()).isNull();
    }

    @Test
    void anythingElseMapsToInternalWithOnlyTheMessage() {
        Throwable mapped = GrpcErrors.map(new RuntimeException("connection reset by peer"));
        Status status = Status.fromThrowable(mapped);
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        // Only the message crosses the wire — never a stack trace.
        assertThat(status.getDescription()).isEqualTo("connection reset by peer");
        assertThat(mapped.getCause()).isNull();
    }

    @Test
    void nullOrBlankMessagesLeaveNoDescription() {
        Status nullMessage = Status.fromThrowable(GrpcErrors.map(new RuntimeException()));
        assertThat(nullMessage.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(nullMessage.getDescription()).isNull();

        Status blankMessage = Status.fromThrowable(GrpcErrors.map(new RuntimeException("   ")));
        assertThat(blankMessage.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(blankMessage.getDescription()).isNull();
    }

    @Test
    void factoriesRaiseTheDocumentedCodes() {
        assertThat(Status.fromThrowable(GrpcErrors.invalidArgument("a")).getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(Status.fromThrowable(GrpcErrors.notFound("b")).getCode())
                .isEqualTo(Status.Code.NOT_FOUND);
        assertThat(Status.fromThrowable(GrpcErrors.failedPrecondition("c")).getCode())
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(Status.fromThrowable(GrpcErrors.notFound("b")).getDescription()).isEqualTo("b");
    }

    // ------------------------------------------------------------------ run

    @Test
    void runSendsTheResultAndCompletes() {
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> "payload");
        assertThat(observer.values).containsExactly("payload");
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
    }

    @Test
    void runMapsAHandlerFailureOntoTheWire() {
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> {
            throw new IllegalArgumentException("drive is required");
        });
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        Status status = Status.fromThrowable(observer.error);
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).isEqualTo("drive is required");
    }

    @Test
    void runPassesExplicitStatusErrorsThrough() {
        RecordingObserver<String> observer = new RecordingObserver<>();
        StatusRuntimeException raised = GrpcErrors.notFound("no such row");
        GrpcErrors.run(observer, () -> {
            throw raised;
        });
        assertThat(observer.error).isSameAs(raised);
        assertThat(observer.completed).isFalse();
    }

    /** A unary observer that records everything the bridge emits. */
    private static final class RecordingObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
