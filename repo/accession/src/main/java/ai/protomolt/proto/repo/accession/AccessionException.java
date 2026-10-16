package ai.protomolt.proto.repo.accession;

import com.google.protobuf.Message;
import io.grpc.Status;
import java.util.Optional;

/**
 * A refused or failed DocumentService call: the gRPC status it maps to and the
 * detail message errors.proto assigns to that status, when there is one.
 */
final class AccessionException extends RuntimeException {
    private final Status.Code code;
    private final Message detail;

    /**
     * Constructs an accession exception with a status code, message, and error detail.
     *
     * @param code the gRPC status code
     * @param message the detail error message
     * @param detail optional protobuf error detail message, or null
     */
    AccessionException(Status.Code code, String message, Message detail) {
        super(message);
        this.code = code;
        this.detail = detail;
    }

    /**
     * Constructs an accession exception with a status code, message, error detail, and cause.
     *
     * @param code the gRPC status code
     * @param message the detail error message
     * @param detail optional protobuf error detail message, or null
     * @param cause the underlying cause
     */
    AccessionException(Status.Code code, String message, Message detail, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.detail = detail;
    }

    /**
     * Returns the gRPC status code for this exception.
     *
     * @return the gRPC status code
     */
    Status.Code code() { return code; }

    /**
     * Returns the optional protobuf error detail payload.
     *
     * @return an optional containing the detail message, or empty if none
     */
    Optional<Message> detail() { return Optional.ofNullable(detail); }
}
