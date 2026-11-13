package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.Objects;

/**
 * A single observed difference between a recorded release baseline and the current surface
 * on one of the five {@link CompatibilitySurface}s.
 *
 * @param surface   the contract surface where the change occurred
 * @param ruleId    stable rule identifier (e.g. {@code "JAVA_METHOD_REMOVED"} or {@link ChangeRules#FIELD_TYPE_CHANGED})
 * @param path      canonical element path within the surface
 * @param stability effective stability tier of the affected element
 * @param before    summary of the baseline state (empty for additions)
 * @param after     summary of the current state (empty for removals)
 * @param message   human-readable explanation of the change
 * @param breaking  whether the change is incompatible according to the surface's rules
 */
@ApiStability(StabilityTier.STABLE)
public record SurfaceChange(
        CompatibilitySurface surface,
        String ruleId,
        String path,
        StabilityTier stability,
        String before,
        String after,
        String message,
        boolean breaking) {

    public SurfaceChange {
        Objects.requireNonNull(surface, "surface");
        Objects.requireNonNull(ruleId, "ruleId");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(stability, "stability");
        before = before == null ? "" : before;
        after = after == null ? "" : after;
        Objects.requireNonNull(message, "message");
    }

    /**
     * Whether this change fails the release-baseline compatibility gate in the absence of
     * an explicit reviewed {@link CompatibilityWaiver}.
     *
     * <p>Only breaking changes on surfaces whose {@link StabilityTier#enforcesCompatibility()}
     * is {@code true} ({@link StabilityTier#STABLE} and {@link StabilityTier#DEPRECATED})
     * fail the gate.</p>
     *
     * @return {@code true} if this change is breaking on a gate-enforced stability tier
     */
    public boolean violatesGate() {
        return breaking && stability.enforcesCompatibility();
    }
}
