package ai.protomolt.proto.sources;

/** Compilation of a {@link ProtoSourceSet} failed — unresolvable imports, parse errors, or I/O. */
public class ProtoCompilationException extends Exception {

    public ProtoCompilationException(String message) {
        super(message);
    }

    public ProtoCompilationException(String message, Throwable cause) {
        super(message, cause);
    }
}
