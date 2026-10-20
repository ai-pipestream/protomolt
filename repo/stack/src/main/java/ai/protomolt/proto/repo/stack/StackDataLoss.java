package ai.protomolt.proto.repo.stack;

import java.io.IOException;

/** Stored bytes are missing, truncated or differ from their reference. Never returned as valid. */
public final class StackDataLoss extends IOException {
    public StackDataLoss(String message) { super(message); }
    public StackDataLoss(String message, Throwable cause) { super(message, cause); }
}
