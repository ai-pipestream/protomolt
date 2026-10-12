package ai.protomolt.proto.repo.blob.spi;

/**
 * Privileged deletion port for exactly one immutable provider version of one
 * object, for lifecycle owners that have fenced new references. This is the
 * exact-version counterpart of {@link ObjectReclaimer}: it never widens to
 * whole-key deletion, never substitutes the current version, and is advertised
 * through {@link BlobCapability#EXACT_VERSION_RECLAMATION} only where a real
 * immutable-version mechanism exists.
 *
 * <p>This mechanism carries no authorization. The caller must establish
 * repository prune eligibility and an irreversible acquisition fence before
 * invoking it, bind the version id from a verified write/read observation, hold
 * no SQL locks during the call, and treat {@link ExactVersionOutcome.Status#UNCERTAIN}
 * as retry-the-same-target. See {@code docs/design/exact-version-reclamation.md}.
 */
@FunctionalInterface
public interface ExactVersionReclaimer {
    /**
     * Reclaim exactly {@code versionId} of {@code bucket}/{@code key}.
     *
     * <p>Throws {@link IllegalArgumentException} before any provider I/O for a
     * null/blank bucket, null/empty key, or a null/blank/literal-{@code "null"}
     * version id. Throws {@link BlobStoreException} for explicit failures:
     * {@code PERMISSION_DENIED}/{@code UNAUTHENTICATED} for authorization
     * failure, {@code NOT_FOUND} for a missing namespace,
     * {@code FAILED_PRECONDITION} for a namespace that is not version-enabled,
     * and {@code UNIMPLEMENTED} when the backend reports it lacks a required
     * semantic. Anything a provider cannot classify explicitly — timeout,
     * connection failure, server error, interruption, corruption evidence —
     * produces an uncertain result with phase, typed failure and original cause; no failure is ever reported as
     * absence. Duplicate or overlapping calls for the same target are safe;
     * other versions and delete markers on the key survive.
     *
     * @return the terminal classification: acknowledged and verified deletion,
     *         verified exact-version absence, or an uncertain result that must
     *         be retried against the same immutable target
     */
    ExactVersionOutcome reclaim(String bucket, String key, String versionId);
}
