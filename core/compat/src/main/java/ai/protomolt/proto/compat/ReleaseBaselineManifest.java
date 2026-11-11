package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Metadata and waiver manifest for a compatibility baseline directory.
 *
 * @param schemaVersion  format version identifier (must be {@link #SCHEMA_VERSION})
 * @param status         whether the baseline is an unreleased initial snapshot or a released tag baseline
 * @param releaseVersion project version associated with the snapshot
 * @param releaseTag     Git release tag for {@link ReleaseBaselineStatus#RELEASED_BASELINE}, or empty for {@link ReleaseBaselineStatus#UNRELEASED_INITIAL_SNAPSHOT}
 * @param baselineNote   explicit statement of the baseline's guarantee scope
 * @param waivers        reviewed compatibility waivers for intentional breaking migrations
 */
@ApiStability(StabilityTier.STABLE)
public record ReleaseBaselineManifest(
        String schemaVersion,
        ReleaseBaselineStatus status,
        String releaseVersion,
        String releaseTag,
        String baselineNote,
        List<CompatibilityWaiver> waivers) {

    /** Canonical schema version for {@code manifest.json}. */
    public static final String SCHEMA_VERSION = "protomolt-compatibility-baseline/v1";

    /** Standard note used when no external release baseline has been cut yet. */
    public static final String UNRELEASED_BASELINE_NOTE =
            "Pre-release initial compatibility snapshot. No external release baseline has been cut yet; "
            + "automated gates enforce stability rules within the repository without claiming an external "
            + "released stability guarantee until the first RELEASED_BASELINE tag is published.";

    public ReleaseBaselineManifest {
        if (!SCHEMA_VERSION.equals(schemaVersion)) {
            throw new IllegalArgumentException(
                    "Unsupported baseline manifest schemaVersion '" + schemaVersion
                            + "'; expected '" + SCHEMA_VERSION + "'");
        }
        Objects.requireNonNull(status, "status");
        if (releaseVersion == null || releaseVersion.trim().isEmpty()) {
            throw new IllegalArgumentException("releaseVersion must not be blank");
        }
        releaseVersion = releaseVersion.trim();
        releaseTag = releaseTag == null ? "" : releaseTag.trim();
        if (status == ReleaseBaselineStatus.UNRELEASED_INITIAL_SNAPSHOT && !releaseTag.isEmpty()) {
            throw new IllegalArgumentException(
                    "UNRELEASED_INITIAL_SNAPSHOT must not claim a releaseTag ('" + releaseTag
                            + "'); do not claim a released stability guarantee before a release baseline exists");
        }
        if (status == ReleaseBaselineStatus.RELEASED_BASELINE && releaseTag.isEmpty()) {
            throw new IllegalArgumentException(
                    "RELEASED_BASELINE requires a non-blank releaseTag");
        }
        if (baselineNote == null || baselineNote.trim().isEmpty()) {
            throw new IllegalArgumentException("baselineNote must not be blank");
        }
        baselineNote = baselineNote.trim();

        List<CompatibilityWaiver> waiverCopy = new ArrayList<>(waivers == null ? List.of() : waivers);
        Set<String> ids = new HashSet<>();
        for (CompatibilityWaiver waiver : waiverCopy) {
            Objects.requireNonNull(waiver, "waiver");
            if (!ids.add(waiver.id())) {
                throw new IllegalArgumentException("Duplicate CompatibilityWaiver id: " + waiver.id());
            }
        }
        waiverCopy.sort(Comparator.comparing(CompatibilityWaiver::id));
        waivers = List.copyOf(waiverCopy);
    }

    /**
     * Creates a manifest for an unreleased initial snapshot (no external release tag yet).
     *
     * @param releaseVersion current development version (e.g. {@code "0.1.0-SNAPSHOT"})
     * @param waivers        reviewed compatibility waivers
     * @return validated unreleased baseline manifest
     */
    public static ReleaseBaselineManifest unreleasedInitial(
            String releaseVersion,
            List<CompatibilityWaiver> waivers) {
        return new ReleaseBaselineManifest(
                SCHEMA_VERSION,
                ReleaseBaselineStatus.UNRELEASED_INITIAL_SNAPSHOT,
                releaseVersion,
                "",
                UNRELEASED_BASELINE_NOTE,
                waivers);
    }

    /**
     * Creates a manifest for a cut external release baseline tag.
     *
     * @param releaseVersion released version (e.g. {@code "1.0.0"})
     * @param releaseTag     Git release tag (e.g. {@code "v1.0.0"})
     * @param baselineNote   release baseline description
     * @param waivers        reviewed compatibility waivers
     * @return validated released baseline manifest
     */
    public static ReleaseBaselineManifest released(
            String releaseVersion,
            String releaseTag,
            String baselineNote,
            List<CompatibilityWaiver> waivers) {
        return new ReleaseBaselineManifest(
                SCHEMA_VERSION,
                ReleaseBaselineStatus.RELEASED_BASELINE,
                releaseVersion,
                releaseTag,
                baselineNote,
                waivers);
    }
}
