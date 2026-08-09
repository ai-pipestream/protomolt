package ai.pipestream.proto.account.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GrpcErrors#run}: the blocking-handler → {@link StreamObserver}
 * bridge. {@code GrpcErrorsTest} pins the mapping policy; this pins the
 * bridge itself — exactly one terminal callback, the value on the success
 * path, the mapped status on the failure path. No containers.
 */
class GrpcErrorsRunTest {

    /** A unary StreamObserver that records what the bridge did to it. */
    private static final class RecordingObserver<T> implements StreamObserver<T> {
        final List<T> values = new ArrayList<>();
        Throwable error;
        boolean completed;

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

    @Test
    void successSendsTheValueThenCompletes() {
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> "the-result");

        assertThat(observer.values).containsExactly("the-result");
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
    }

    @Test
    void explicitStatusErrorReachesOnErrorUnchanged() {
        StatusRuntimeException raised =
                Status.NOT_FOUND.withDescription("gone").asRuntimeException();
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> {
            throw raised;
        });

        assertThat(observer.error).isSameAs(raised);
        // Exactly one terminal callback: no value, no completion.
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
    }

    @Test
    void unexpectedFailureIsMappedToInternal() {
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> {
            throw new RuntimeException("database on fire");
        });

        assertThat(observer.error).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
            assertThat(e.getStatus().getDescription()).isEqualTo("database on fire");
        });
        assertThat(observer.values).isEmpty();
        assertThat(observer.completed).isFalse();
    }

    @Test
    void nullResultStillCompletesTheUnaryCall() {
        // Proto-free suppliers can return null; the bridge must still
        // terminate the call rather than hang the client.
        RecordingObserver<String> observer = new RecordingObserver<>();
        GrpcErrors.run(observer, () -> null);

        assertThat(observer.values).containsExactly((String) null);
        assertThat(observer.completed).isTrue();
        assertThat(observer.error).isNull();
    }
}
