package ai.protomolt.proto.quality;

/**
 * A quality annotation that cannot be honored: a dimension with no id or expression, a negative
 * weight, or CEL that does not compile against the message type. These are schema errors — they
 * surface deterministically on the type's first scoring regardless of the message's values.
 */
public class QualitySchemaException extends RuntimeException {

    public QualitySchemaException(String message) {
        super(message);
    }

    public QualitySchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
