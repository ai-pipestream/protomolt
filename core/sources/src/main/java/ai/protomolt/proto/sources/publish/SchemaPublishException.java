package ai.protomolt.proto.sources.publish;

/** Publishing to a schema registry failed. */
public class SchemaPublishException extends Exception {

    public SchemaPublishException(String message) {
        super(message);
    }

    public SchemaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
