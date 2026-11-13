package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

/**
 * Lifecycle state of a recorded compatibility baseline snapshot.
 *
 * <p>Distinguishes an initial in-repository development snapshot from an externally released
 * baseline tag so ProtoMolt never claims a released external stability guarantee before an
 * official release baseline has been cut.</p>
 */
@ApiStability(StabilityTier.STABLE)
public enum ReleaseBaselineStatus {

    /**
     * Pre-release baseline snapshot recorded in the repository to gate development changes
     * before an external release tag has been cut. Does not claim an external released
     * stability guarantee.
     */
    UNRELEASED_INITIAL_SNAPSHOT(false),

    /**
     * Baseline snapshot pinned to a published release tag. Carries an external compatibility
     * guarantee for all {@link StabilityTier#STABLE} and {@link StabilityTier#DEPRECATED}
     * surfaces.
     */
    RELEASED_BASELINE(true);

    private final boolean externalReleaseGuarantee;

    ReleaseBaselineStatus(boolean externalReleaseGuarantee) {
        this.externalReleaseGuarantee = externalReleaseGuarantee;
    }

    /**
     * Whether this baseline status represents a cut external release with an external
     * compatibility guarantee.
     *
     * @return {@code true} only for {@link #RELEASED_BASELINE}
     */
    public boolean hasExternalReleaseGuarantee() {
        return externalReleaseGuarantee;
    }
}
