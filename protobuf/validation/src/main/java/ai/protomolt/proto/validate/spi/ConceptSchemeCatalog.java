package ai.protomolt.proto.validate.spi;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The validator's window onto mounted concept schemes: the runtime data behind the
 * {@code concept_schemes} field rule. The rule itself is schema truth (a ConceptRef
 * field declares which schemes its values belong to, forever) while each scheme's
 * content is mount configuration, typically followed off the config lane and swapped
 * live. Given a mounted version the verdict is deterministic, and the version rides
 * every membership violation as evidence.
 *
 * <p>Implementations must be thread-safe; the validator consults the catalog on every
 * validation of a bound field. A catalog with nothing mounted makes every bound scheme
 * refuse ({@code concept.unmounted}): fail-closed, never a silent pass.</p>
 */
public interface ConceptSchemeCatalog {

    /**
     * The scheme mounted under {@code schemeId}, or empty when none is.
     *
     * @param schemeId the scheme id a schema binds
     * @return the mounted scheme, if any
     */
    Optional<Mounted> scheme(String schemeId);

    /** A catalog with nothing mounted: every bound scheme refuses. */
    static ConceptSchemeCatalog empty() {
        return schemeId -> Optional.empty();
    }

    /**
     * Whether this catalog decides membership. Only {@link #admitted()} answers false.
     *
     * @return true when a bound value's concept is checked against the mounted scheme
     */
    default boolean decidesMembership() {
        return true;
    }

    /**
     * Captures an immutable point-in-time snapshot of all currently mounted
     * concept schemes so a single save evaluates membership and expansion
     * against one consistent multi-scheme view.
     *
     * @return a point-in-time catalog snapshot
     */
    default ConceptSchemeCatalog snapshot() {
        return this;
    }

    /**
     * A catalog for re-reading content that was admitted earlier. The schema half of the
     * rule still holds (a value's scheme must be one the field binds), but membership is
     * mount configuration that was decided when the content was admitted, so it is not
     * decided again: a reader whose host has not mounted the scheme, or mounts another
     * version, still reads what was stored. Never use it to admit new content.
     *
     * @return the re-read catalog
     */
    static ConceptSchemeCatalog admitted() {
        return new ConceptSchemeCatalog() {
            @Override
            public Optional<Mounted> scheme(String schemeId) {
                return Optional.empty();
            }

            @Override
            public boolean decidesMembership() {
                return false;
            }
        };
    }

    /**
     * One mounted scheme's membership.
     *
     * @param schemeId the scheme id, the remainder of its config subject
     * @param version the config source's version of the document, the evidence a
     *        membership violation cites
     * @param conceptIds every concept id of the scheme, deprecated concepts included:
     *        a retired concept stays a member so documents tagged before the
     *        retirement keep validating
     */
    record Mounted(String schemeId, String version, Set<String> conceptIds) {

        public Mounted {
            if (schemeId == null || schemeId.isBlank()) {
                throw new IllegalArgumentException("schemeId must not be blank");
            }
            Objects.requireNonNull(version, "version");
            conceptIds = Set.copyOf(Objects.requireNonNull(conceptIds, "conceptIds"));
        }
    }
}
