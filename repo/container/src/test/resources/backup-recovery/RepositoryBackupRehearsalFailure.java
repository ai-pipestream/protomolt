package ai.protomolt.proto.repo.container.ledger;

/** A rehearsal step that did not produce the observation it claims; never retried or hidden. */
final class RepositoryBackupRehearsalFailure extends RuntimeException {
    RepositoryBackupRehearsalFailure(String message) { super(message); }
    RepositoryBackupRehearsalFailure(String message, Throwable cause) { super(message, cause); }
}
