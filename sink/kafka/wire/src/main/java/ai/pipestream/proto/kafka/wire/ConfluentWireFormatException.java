package ai.pipestream.proto.kafka.wire;

/**
 * Signals a byte sequence that is not a valid Confluent frame; callers in a Kafka or Connect
 * context translate it to their framework's type.
 */
public final class ConfluentWireFormatException extends RuntimeException {

    public ConfluentWireFormatException(String message) {
        super(message);
    }

    public ConfluentWireFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
