package ai.protomolt.proto.descriptors;

import java.util.Locale;
import java.util.Optional;

/**
 * Stability classification for ProtoMolt public and internal surfaces across Java APIs,
 * protobuf contracts, retained stored formats, REST routes, and action envelopes.
 *
 * <p>Compatibility gates use the resolved tier to decide whether an incompatible change
 * fails closed against the recorded release baseline or is permitted as an evolving,
 * experimental, or internal change.</p>
 */
@ApiStability(StabilityTier.STABLE)
public enum StabilityTier {

    /**
     * Public, supported contract. Incompatible changes are rejected by automated release
     * baseline gates unless accompanied by an explicit reviewed migration waiver or a major
     * version boundary.
     */
    STABLE(true, true, 4),

    /**
     * Public contract scheduled for removal. Remains protected by compatibility gates until
     * retired through an explicit reviewed waiver or major release boundary.
     */
    DEPRECATED(true, true, 3),

    /**
     * Public contract under active refinement. Additive evolution is expected; incompatible
     * changes are surfaced as warnings or require explicit release notes.
     */
    EVOLVING(false, true, 2),

    /**
     * Incubating or opt-in surface. May change or be removed between minor releases without
     * a compatibility waiver.
     */
    EXPERIMENTAL(false, true, 1),

    /**
     * Implementation detail not intended for external consumption. Excluded from external
     * compatibility guarantees.
     */
    INTERNAL(false, false, 0);

    private final boolean enforcesCompatibility;
    private final boolean publicContract;
    private final int protectionRank;

    StabilityTier(boolean enforcesCompatibility, boolean publicContract, int protectionRank) {
        this.enforcesCompatibility = enforcesCompatibility;
        this.publicContract = publicContract;
        this.protectionRank = protectionRank;
    }

    /**
     * Whether incompatible changes to a surface at this tier fail the release-baseline
     * compatibility gate unless covered by an explicit reviewed waiver.
     *
     * @return {@code true} for {@link #STABLE} and {@link #DEPRECATED}
     */
    public boolean enforcesCompatibility() {
        return enforcesCompatibility;
    }

    /**
     * Whether a surface at this tier is part of the externally visible contract inventory.
     *
     * @return {@code false} only for {@link #INTERNAL}
     */
    public boolean isPublicContract() {
        return publicContract;
    }

    /**
     * Relative strength of the stability commitment (higher values represent stronger
     * compatibility guarantees).
     *
     * @return integer rank in {@code [0, 4]}
     */
    public int protectionRank() {
        return protectionRank;
    }

    /**
     * Determines whether transitioning from this tier to {@code target} weakens the
     * compatibility guarantee without preserving gate enforcement.
     *
     * <p>Transitioning from {@link #STABLE} to {@link #DEPRECATED} is permitted as normal
     * lifecycle deprecation because {@link #DEPRECATED} continues to enforce compatibility
     * gates until removal. Transitioning from {@link #STABLE} or {@link #DEPRECATED} to
     * {@link #EVOLVING}, {@link #EXPERIMENTAL}, or {@link #INTERNAL} is a breaking
     * stability downgrade because it would strip gate enforcement from a protected surface.</p>
     *
     * @param target the new stability tier
     * @return {@code true} if the transition weakens compatibility enforcement
     */
    public boolean isDowngradeTo(StabilityTier target) {
        if (target == null) {
            return true;
        }
        if (this == STABLE && target == DEPRECATED) {
            return false;
        }
        return target.protectionRank < this.protectionRank;
    }

    /**
     * Parses a stability label (such as a protobuf metadata label or manifest string) into
     * a {@link StabilityTier}.
     *
     * @param raw the raw stability token (may be null or blank)
     * @return the matching tier, or {@link Optional#empty()} if unrecognized
     */
    public static Optional<StabilityTier> parseOptional(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return switch (normalized) {
            case "STABLE", "GA", "RELEASED" -> Optional.of(STABLE);
            case "DEPRECATED", "OBSOLETE" -> Optional.of(DEPRECATED);
            case "EVOLVING", "BETA", "PREVIEW" -> Optional.of(EVOLVING);
            case "EXPERIMENTAL", "ALPHA", "INCUBATING", "UNSTABLE" -> Optional.of(EXPERIMENTAL);
            case "INTERNAL", "PRIVATE" -> Optional.of(INTERNAL);
            default -> Optional.empty();
        };
    }

    /**
     * Parses an explicit stability label into a {@link StabilityTier}, failing closed when a
     * non-blank label is unrecognized.
     *
     * <p>Unlike {@link #parseOptional(String)}, this method rejects misspelled or unknown
     * non-blank stability tokens so invalid metadata cannot silently fall back to an
     * inherited or default tier.</p>
     *
     * @param raw the raw stability token (may be null or blank)
     * @return the matching tier, or {@link Optional#empty()} when {@code raw} is null or blank
     * @throws IllegalArgumentException if {@code raw} is non-blank and does not name a known tier
     */
    public static Optional<StabilityTier> parseExplicit(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String normalized = raw.trim();
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(parseOptional(normalized).orElseThrow(() ->
                new IllegalArgumentException("Unrecognized stability label: '" + raw + "'")));
    }

    /**
     * Parses a stability label or returns {@code fallback} when the input is null, blank,
     * or unrecognized.
     *
     * @param raw the raw stability token
     * @param fallback fallback tier when {@code raw} does not name a known tier
     * @return the resolved tier
     */
    public static StabilityTier fromLabel(String raw, StabilityTier fallback) {
        return parseOptional(raw).orElse(fallback);
    }
}
