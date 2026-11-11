package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.List;
import java.util.Objects;

/**
 * Outcome of evaluating a candidate {@link ReleaseBaselineBundle} against a recorded
 * baseline {@link ReleaseBaselineBundle} across all five {@link CompatibilitySurface}s.
 *
 * @param baselineManifest   the baseline manifest that governed the evaluation
 * @param changes            every observed change across all five surfaces
 * @param waivedChanges      breaking changes on gate-enforced tiers covered by a {@link CompatibilityWaiver}
 * @param unwaivedViolations breaking changes on gate-enforced tiers that lack a matching waiver
 * @param matchedWaivers     waivers that matched at least one breaking change
 * @param unusedWaivers      declared waivers that did not match any breaking change
 */
@ApiStability(StabilityTier.STABLE)
public record ReleaseBaselineReport(
        ReleaseBaselineManifest baselineManifest,
        List<SurfaceChange> changes,
        List<SurfaceChange> waivedChanges,
        List<SurfaceChange> unwaivedViolations,
        List<CompatibilityWaiver> matchedWaivers,
        List<CompatibilityWaiver> unusedWaivers) {

    public ReleaseBaselineReport {
        Objects.requireNonNull(baselineManifest, "baselineManifest");
        changes = List.copyOf(Objects.requireNonNull(changes, "changes"));
        waivedChanges = List.copyOf(Objects.requireNonNull(waivedChanges, "waivedChanges"));
        unwaivedViolations = List.copyOf(Objects.requireNonNull(unwaivedViolations, "unwaivedViolations"));
        matchedWaivers = List.copyOf(Objects.requireNonNull(matchedWaivers, "matchedWaivers"));
        unusedWaivers = List.copyOf(Objects.requireNonNull(unusedWaivers, "unusedWaivers"));
    }

    /**
     * Whether the candidate bundle is compatible with the recorded baseline (no unwaived
     * breaking changes on {@link StabilityTier#STABLE} or {@link StabilityTier#DEPRECATED}
     * surfaces and no unused compatibility waivers).
     *
     * @return {@code true} when both {@link #unwaivedViolations()} and {@link #unusedWaivers()} are empty
     */
    public boolean isCompatible() {
        return unwaivedViolations.isEmpty() && unusedWaivers.isEmpty();
    }

    /**
     * Whether the baseline manifest represents a cut external release baseline tag.
     *
     * @return {@code true} only when {@link ReleaseBaselineManifest#status()} is {@link ReleaseBaselineStatus#RELEASED_BASELINE}
     */
    public boolean claimsExternalReleaseGuarantee() {
        return baselineManifest.status().hasExternalReleaseGuarantee();
    }

    /**
     * Returns all observed changes for a specific {@link CompatibilitySurface}.
     *
     * @param surface the surface to filter by
     * @return immutable list of changes on that surface
     */
    public List<SurfaceChange> changesForSurface(CompatibilitySurface surface) {
        Objects.requireNonNull(surface, "surface");
        return changes.stream()
                .filter(c -> c.surface() == surface)
                .toList();
    }

    /**
     * Fails closed by throwing {@link IncompatibleSurfaceException} if any unwaived
     * violations or unused waivers were found.
     *
     * @throws IncompatibleSurfaceException if {@link #isCompatible()} is {@code false}
     */
    public void throwIfIncompatible() {
        if (!isCompatible()) {
            throw new IncompatibleSurfaceException(unwaivedViolations, unusedWaivers);
        }
    }
}
