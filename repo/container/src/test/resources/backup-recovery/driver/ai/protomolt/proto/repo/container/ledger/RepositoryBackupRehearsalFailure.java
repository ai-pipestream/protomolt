package ai.protomolt.proto.repo.container.ledger;

/** Driver-side terminal failure for the backup rehearsal hosts; never swallowed. */
final class RepositoryBackupRehearsalFailure extends RuntimeException {
    RepositoryBackupRehearsalFailure(String message) { super(message); }
    RepositoryBackupRehearsalFailure(String message, Throwable cause) { super(message, cause); }
}
