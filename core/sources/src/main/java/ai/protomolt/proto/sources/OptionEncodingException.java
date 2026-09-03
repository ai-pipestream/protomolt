package ai.protomolt.proto.sources;

/**
 * Re-encoding a linked {@code Options} map into descriptor bytes failed — an unknown
 * extension, an unresolvable enum identifier, an unparseable scalar, or a value shape that
 * does not match the target field. The message always names the element and the option.
 */
public class OptionEncodingException extends ProtoCompilationException {

    public OptionEncodingException(String message) {
        super(message);
    }

    public OptionEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
