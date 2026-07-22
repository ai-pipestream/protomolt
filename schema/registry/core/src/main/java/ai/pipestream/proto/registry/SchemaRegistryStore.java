package ai.pipestream.proto.registry;

import java.util.List;
import java.util.Optional;

/**
 * Subject/version schema store — the persistence contract behind the registry server.
 *
 * <p>Subjects hold ascending 1-based versions; every registered schema also gets a
 * {@code globalId} unique and monotonic across the whole store. Content identity (schema text
 * plus references) makes registration idempotent: registering identical content returns the
 * existing version unchanged.</p>
 *
 * <h2>Registration pipeline</h2>
 * <ol>
 *   <li>every {@link SchemaReference} must already exist ({@link ReferenceNotFoundException});</li>
 *   <li>content-identical registrations short-circuit to the existing version;</li>
 *   <li>the {@link WriteGate}, when configured, sees the subject's full history and effective
 *       compatibility mode (per-subject, falling back to global) unless that mode is
 *       {@code NONE} — violations become {@link IncompatibleRegistrationException};</li>
 *   <li>the candidate is compiled together with its resolved transitive reference texts, so
 *       unparseable or unlinkable schemas are rejected ({@link InvalidSchemaException}).</li>
 * </ol>
 *
 * <p>Compatibility modes are opaque validated strings from the Confluent vocabulary
 * ({@link CompatibilityModes}); setters throw {@link IllegalArgumentException} for anything
 * else.</p>
 */
public interface SchemaRegistryStore extends AutoCloseable {

    /** All subjects holding at least one version, sorted lexicographically. */
    List<String> subjects();

    /** Versions of the subject in ascending order; empty for an unknown subject. */
    List<Integer> versions(String subject);

    /** The given version of the subject, when both exist. */
    Optional<StoredSchema> version(String subject, int version);

    /** The subject's highest version, when the subject exists. */
    Optional<StoredSchema> latest(String subject);

    /** The schema registered under the given store-wide id. */
    Optional<StoredSchema> byGlobalId(int globalId);

    /**
     * The subject's version whose content (schema text plus references) is identical to the
     * candidate — the idempotent-lookup counterpart of {@link #register}.
     */
    Optional<StoredSchema> findByContent(String subject, String schemaText, List<SchemaReference> references);

    /**
     * Registers a schema under the subject, running the full registration pipeline (see the
     * class javadoc). Identical content returns the existing version unchanged.
     *
     * @throws ReferenceNotFoundException        when a reference does not exist in the store
     * @throws IncompatibleRegistrationException when the write gate reports violations
     * @throws InvalidSchemaException            when the candidate fails to compile
     * @throws RegistryStoreException            on storage failure
     */
    StoredSchema register(String subject, String schemaText, List<SchemaReference> references)
            throws RegistryStoreException;

    /** The subject's own compatibility mode; empty when unset (the global mode applies). */
    Optional<String> compatibilityMode(String subject);

    /**
     * Sets the subject's compatibility mode.
     *
     * @throws IllegalArgumentException for a mode outside the Confluent vocabulary
     */
    void setCompatibilityMode(String subject, String mode);

    /** The store-wide compatibility mode; defaults to {@link CompatibilityModes#DEFAULT_GLOBAL}. */
    String globalCompatibilityMode();

    /**
     * Sets the store-wide compatibility mode.
     *
     * @throws IllegalArgumentException for a mode outside the Confluent vocabulary
     */
    void setGlobalCompatibilityMode(String mode);

    /**
     * Drops any cached view so the next read reflects external changes to the backing storage
     * (e.g. a {@code git pull} done out-of-band). No-op for stores without external state.
     */
    default void refresh() {
    }

    @Override
    void close();

    /**
     * Write-gate seam: compatibility enforcement plugs in here without this module depending
     * on any particular checker.
     */
    interface WriteGate {

        /**
         * Returns violations (empty = allowed).
         *
         * @param subject    subject being registered
         * @param mode       the subject's effective compatibility mode (never {@code NONE};
         *                   the store skips the gate for {@code NONE})
         * @param history    the subject's full existing history, ascending by version
         * @param schemaText candidate schema text
         * @param references candidate references, already verified to exist in {@code store}
         * @param store      the store, for resolving reference texts
         */
        List<String> validate(String subject, String mode, List<StoredSchema> history,
                              String schemaText, List<SchemaReference> references,
                              SchemaRegistryStore store);
    }
}
