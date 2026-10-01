package ai.protomolt.proto.samples;

import java.io.IOException;

/** The operation ID is already durably bound to different content. */
public final class FixtureRecordConflictException extends IOException {
    public FixtureRecordConflictException() {
        super("operation ID is already bound to different content");
    }
}
