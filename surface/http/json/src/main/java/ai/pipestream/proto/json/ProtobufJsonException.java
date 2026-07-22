package ai.pipestream.proto.json;

/**
 * Thrown when JSON ↔ protobuf conversion fails.
 */
public class ProtobufJsonException extends RuntimeException {
    public ProtobufJsonException(String message) {
        super(message);
    }

    public ProtobufJsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
