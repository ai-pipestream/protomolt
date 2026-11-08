package ai.protomolt.proto.emit.renderer;

import java.io.IOException;

/** A refusal retains its category and original cause across renderer boundaries. */
public final class RenderFailure extends IOException {
    public enum Kind { UNSUPPORTED, INVALID, DENIED, UNAVAILABLE, LIMIT, CANCELLED, FAILED }
    private final Kind kind;
    public RenderFailure(Kind kind, String message) { super(message); this.kind = kind; }
    public RenderFailure(Kind kind, String message, Throwable cause) { super(message, cause); this.kind = kind; }
    public Kind kind() { return kind; }
}
