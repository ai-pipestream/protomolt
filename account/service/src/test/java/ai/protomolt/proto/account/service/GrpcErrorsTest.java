package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.store.AccountStoreException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The exception → gRPC status mapping policy in {@link GrpcErrors}: the wire
 * contract every handler error crosses. The ITs cover the happy mappings
 * end-to-end; this pins the branches no RPC can reach deterministically
 * (unclassified store failures, validation that slipped through, unknown
 * failures) — including the no-stack-trace rule.
 */
class GrpcErrorsTest {

    @Test
    void explicitStatusErrorsPassThroughUnchanged() {
        StatusRuntimeException runtime = Status.ALREADY_EXISTS
                .withDescription("taken").asRuntimeException();
        assertThat(GrpcErrors.map(runtime)).isSameAs(runtime);

        StatusException checked = Status.NOT_FOUND.withDescription("gone").asException();
        assertThat(GrpcErrors.map(checked)).isSameAs(checked);
    }

    @Test
    void storeExceptionKindsMapToWireStatuses() {
        assertThat(statusOf(GrpcErrors.map(AccountStoreException.notFound("acct-1"))))
                .isEqualTo(Status.Code.NOT_FOUND);
        assertThat(statusOf(GrpcErrors.map(AccountStoreException.conflict("acct-1"))))
                .isEqualTo(Status.Code.ALREADY_EXISTS);
        // The kind rides into the description, so operators see WHICH account.
        assertThat(((StatusRuntimeException) GrpcErrors.map(AccountStoreException.notFound("acct-1")))
                .getStatus().getDescription()).contains("acct-1");
    }

    @Test
    void unclassifiedStoreFailureIsInternal() {
        assertThat(statusOf(GrpcErrors.map(
                AccountStoreException.wrap("connection reset", new java.sql.SQLException()))))
                .isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    void illegalArgumentMapsToInvalidArgument() {
        Throwable mapped = GrpcErrors.map(new IllegalArgumentException("bad enum value"));
        StatusRuntimeException error = (StatusRuntimeException) mapped;
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(error.getStatus().getDescription()).isEqualTo("bad enum value");
    }

    @Test
    void unknownFailureIsInternalWithOnlyTheMessage() {
        Throwable mapped = GrpcErrors.map(new RuntimeException("database on fire"));
        StatusRuntimeException error = (StatusRuntimeException) mapped;
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        // The description carries the message — never a stack trace.
        assertThat(error.getStatus().getDescription()).isEqualTo("database on fire");
    }

    @Test
    void blankMessageLeavesNoDescription() {
        StatusRuntimeException error =
                (StatusRuntimeException) GrpcErrors.map(new RuntimeException());
        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(error.getStatus().getDescription()).isNull();
    }

    private static Status.Code statusOf(Throwable mapped) {
        return ((StatusRuntimeException) mapped).getStatus().getCode();
    }
}
