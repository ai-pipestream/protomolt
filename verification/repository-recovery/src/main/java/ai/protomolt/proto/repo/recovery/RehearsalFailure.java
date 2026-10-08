package ai.protomolt.proto.repo.recovery;

/** A rehearsal step that did not produce the observation it claims; never retried silently. */
final class RehearsalFailure extends RuntimeException {
    RehearsalFailure(String message) { super(message); }
    RehearsalFailure(String message, Throwable cause) { super(message, cause); }
}
