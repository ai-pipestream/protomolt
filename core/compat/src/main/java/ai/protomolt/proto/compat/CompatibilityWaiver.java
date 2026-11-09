package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.Objects;

/**
 * Explicit, reviewed migration or waiver record authorizing a specific incompatible change
 * on a {@link StabilityTier#STABLE} or {@link StabilityTier#DEPRECATED} surface.
 *
 * <p>Waivers are fail-closed: every required field must be non-blank, and wildcard
 * tokens ({@code *}) are rejected so a waiver can never silently mask unrelated breaks.</p>
 *
 * @param id          unique waiver identifier (e.g. {@code "WAIVER-827-01"})
 * @param surface     target compatibility surface
 * @param ruleId      exact rule identifier being waived
 * @param targetPath  exact canonical path of the affected element
 * @param approvalRef issue, PR, or ADR reference documenting the review
 * @param reason      human-readable explanation and migration guidance
 * @param before      optional exact baseline state to match (empty matches any baseline value for the path/rule)
 * @param after       optional exact current state to match (empty matches any current value for the path/rule)
 */
@ApiStability(StabilityTier.STABLE)
public record CompatibilityWaiver(
        String id,
        CompatibilitySurface surface,
        String ruleId,
        String targetPath,
        String approvalRef,
        String reason,
        String before,
        String after) {

    public CompatibilityWaiver {
        id = requireExactNonBlank(id, "id");
        Objects.requireNonNull(surface, "surface");
        ruleId = requireExactNonBlank(ruleId, "ruleId");
        targetPath = requireExactNonBlank(targetPath, "targetPath");
        approvalRef = requireNonBlank(approvalRef, "approvalRef");
        reason = requireNonBlank(reason, "reason");
        before = normalizeOptionalExact(before, "before");
        after = normalizeOptionalExact(after, "after");
    }

    /**
     * Convenience constructor for a waiver matching a specific {@code surface}, {@code ruleId},
     * and {@code targetPath}.
     */
    public CompatibilityWaiver(
            String id,
            CompatibilitySurface surface,
            String ruleId,
            String targetPath,
            String approvalRef,
            String reason) {
        this(id, surface, ruleId, targetPath, approvalRef, reason, "", "");
    }

    /**
     * Tests whether this waiver covers the given {@link SurfaceChange}.
     *
     * @param change the detected change
     * @return {@code true} if the surface, ruleId, path, and optional before/after states match
     */
    public boolean covers(SurfaceChange change) {
        Objects.requireNonNull(change, "change");
        if (surface != change.surface()) {
            return false;
        }
        if (!ruleId.equals(change.ruleId())) {
            return false;
        }
        if (!targetPath.equals(change.path())) {
            return false;
        }
        if (!before.isEmpty() && !before.equals(change.before())) {
            return false;
        }
        return after.isEmpty() || after.equals(change.after());
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("CompatibilityWaiver." + fieldName + " must not be blank");
        }
        return value.trim();
    }

    private static String requireExactNonBlank(String value, String fieldName) {
        String trimmed = requireNonBlank(value, fieldName);
        if (trimmed.indexOf('*') >= 0) {
            throw new IllegalArgumentException(
                    "CompatibilityWaiver." + fieldName + " must not contain wildcard '*': " + trimmed);
        }
        return trimmed;
    }

    private static String normalizeOptionalExact(String value, String fieldName) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        if (trimmed.indexOf('*') >= 0) {
            throw new IllegalArgumentException(
                    "CompatibilityWaiver." + fieldName + " must not contain wildcard '*': " + trimmed);
        }
        return trimmed;
    }
}
