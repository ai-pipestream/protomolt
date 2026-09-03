package ai.protomolt.proto.search.embedding;

import java.util.Set;

/**
 * Which classes of field content may reach an embedding provider. An embedding leaves the
 * process, is retained by whatever serves the model, and cannot be un-sent; a vector is
 * also not the harmless summary it looks like, since inversion attacks recover a usable
 * approximation of the source text. So content a schema marked sensitive is refused unless
 * the deployment says otherwise, by name.
 *
 * <p>The default is {@link #unclassifiedOnly()}: a TEXT field carrying any {@code meta.v1}
 * sensitivity class is refused, and a field carrying none embeds exactly as it always has.
 * A deployment that has decided a particular class is safe to vectorize names that class
 * with {@link #permitting}; a deployment that has decided the question does not apply to it
 * says so once with {@link #unrestricted()} rather than leaving a silent hole.
 *
 * <p>This is a gate, not a transform: it never masks, truncates, or rewrites content, and a
 * permitted class embeds verbatim. Masking belongs to the masking layer, which runs earlier
 * and is the right answer when the content should be embedded in redacted form.
 */
public final class VectorizationPolicy {

    private static final VectorizationPolicy UNCLASSIFIED_ONLY =
            new VectorizationPolicy(Set.of(), false);
    private static final VectorizationPolicy UNRESTRICTED =
            new VectorizationPolicy(Set.of(), true);

    private final Set<String> permitted;
    private final boolean unrestricted;

    private VectorizationPolicy(Set<String> permitted, boolean unrestricted) {
        this.permitted = Set.copyOf(permitted);
        this.unrestricted = unrestricted;
    }

    /** The default: only content with no declared sensitivity class may be embedded. */
    public static VectorizationPolicy unclassifiedOnly() {
        return UNCLASSIFIED_ONLY;
    }

    /**
     * Unclassified content plus the named sensitivity classes.
     *
     * @param classes the sensitivity classes this deployment has cleared for embedding;
     *        must be non-empty, and blank entries are refused because a blank class is
     *        already the unclassified case
     */
    public static VectorizationPolicy permitting(Set<String> classes) {
        if (classes == null || classes.isEmpty()) {
            throw new IllegalArgumentException(
                    "name at least one sensitivity class, or use unclassifiedOnly()");
        }
        for (String sensitivity : classes) {
            if (sensitivity == null || sensitivity.isBlank()) {
                throw new IllegalArgumentException(
                        "a blank sensitivity class is the unclassified case, which is"
                                + " always permitted; remove it from the permitted set");
            }
        }
        return new VectorizationPolicy(classes, false);
    }

    /**
     * Every class may be embedded. The escape hatch for a deployment whose model runs
     * inside its own trust boundary; still an explicit decision, never a default.
     */
    public static VectorizationPolicy unrestricted() {
        return UNRESTRICTED;
    }

    /** Whether content in {@code sensitivity} (empty meaning unclassified) may be embedded. */
    public boolean permits(String sensitivity) {
        return unrestricted || sensitivity == null || sensitivity.isEmpty()
                || permitted.contains(sensitivity);
    }

    /** The permitted classes beyond unclassified; empty when none were named. */
    public Set<String> permitted() {
        return permitted;
    }

    /** Whether this policy permits every class. */
    public boolean isUnrestricted() {
        return unrestricted;
    }
}
