package ai.protomolt.proto.repo.accession;

import com.google.protobuf.Message;
import io.grpc.Status;
import java.util.Optional;

/**
 * A refused or failed DocumentService call: the gRPC status it maps to and the
 * detail message errors.proto assigns to that status, when there is one.
 */
public final class AccessionException extends RuntimeException {
    private final Status.Code code;
    private final Message detail;

    public AccessionException(Status.Code code, String message, Message detail) {
        super(message);
        this.code = code;
        this.detail = detail;
    }

    public AccessionException(Status.Code code, String message, Message detail, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.detail = detail;
    }

    public Status.Code code() { return code; }
    public Optional<Message> detail() { return Optional.ofNullable(detail); }
}
