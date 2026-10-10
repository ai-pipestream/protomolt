package ai.protomolt.proto.repo.engine;

/**
 * A host resolver's refusal: the binding records a backend generation or storage realm this
 * host does not serve. The host raises it from its {@link ArchiveObjectReader.BackendResolver}
 * and {@link ArchiveObjectRecovery.BackendResolver}; the engine boundary that called the
 * resolver reports it as {@code FAILED_PRECONDITION} with a constant message and this
 * exception as the cause. The message names the generations involved, which are identifiers,
 * never credentials or endpoints.
 */
public final class UnservedBackendGenerationException extends RuntimeException {
    public UnservedBackendGenerationException(String message) {
        super(message);
    }
}
