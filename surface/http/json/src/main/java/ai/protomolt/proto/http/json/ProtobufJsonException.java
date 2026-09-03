package ai.protomolt.proto.http.json;

/**
 * Thrown when JSON ↔ protobuf conversion fails.
 *
 * <p>Sealed for the same reason as {@code ProtoRestException}: the host status mapper keys off
 * this pair of types, and an unrecognised subtype would be answered with a 500 rather than the
 * 400 it meant.
 */
public sealed class ProtobufJsonException extends RuntimeException
        permits MalformedProtobufJsonException {
    public ProtobufJsonException(String message) {
        super(message);
    }

    public ProtobufJsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
