package ai.pipestream.proto.json;

/**
 * Thrown when inbound JSON cannot be merged into the target protobuf message.
 */
public class MalformedProtobufJsonException extends ProtobufJsonException {
    private final String json;

    public MalformedProtobufJsonException(String message, String json) {
        super(message);
        this.json = json;
    }

    public MalformedProtobufJsonException(String message, String json, Throwable cause) {
        super(message, cause);
        this.json = json;
    }

    public String getJson() {
        return json;
    }
}
