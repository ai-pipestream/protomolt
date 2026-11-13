package ai.protomolt.proto.compat;

import ai.protomolt.proto.compat.StoredFormatSnapshot.StoredFormatEntry;
import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Evaluates compatibility between a baseline {@link StoredFormatSnapshot} and a current
 * {@link StoredFormatSnapshot}.
 *
 * <p>Stored formats are retained on disk, in object storage, or in database rows across
 * process upgrades. Consequently, in addition to wire and proto3 JSON compatibility under
 * the format's declared {@link CompatibilityMode}, removing a field without reserving both
 * its field number and field name ({@link ChangeRules#FIELD_REMOVED_NOT_RESERVED}) is
 * treated as a breaking change to prevent silent corruption of historical records.</p>
 */
@ApiStability(StabilityTier.STABLE)
public final class StoredFormatCompatibilityChecker {

    public static final String STORED_FORMAT_ADDED = "STORED_FORMAT_ADDED";
    public static final String STORED_FORMAT_REMOVED = "STORED_FORMAT_REMOVED";
    public static final String STORED_FORMAT_ROOT_TYPE_CHANGED = "STORED_FORMAT_ROOT_TYPE_CHANGED";
    public static final String STORED_FORMAT_VERSION_REGRESSED = "STORED_FORMAT_VERSION_REGRESSED";
    public static final String STORED_FORMAT_STABILITY_DOWNGRADED = "STORED_FORMAT_STABILITY_DOWNGRADED";
    public static final String STORED_FORMAT_CLOSURE_PIN_CHANGED = "STORED_FORMAT_CLOSURE_PIN_CHANGED";

    private static final CompatibilityChecker STORED_SCHEMA_CHECKER = CompatibilityChecker.builder()
            .includeJsonRules(true)
            .build();

    private StoredFormatCompatibilityChecker() {
    }

    /**
     * Compares {@code current} against {@code baseline} and returns all detected
     * {@link SurfaceChange}s in deterministic order.
     *
     * @param baseline the baseline stored format snapshot
     * @param current  the current stored format snapshot
     * @return immutable list of detected changes
     */
    public static List<SurfaceChange> compare(
            StoredFormatSnapshot baseline,
            StoredFormatSnapshot current) {
        Objects.requireNonNull(baseline, "baseline");
        Objects.requireNonNull(current, "current");

        Map<String, StoredFormatEntry> oldFormats = baseline.formatsById();
        Map<String, StoredFormatEntry> newFormats = current.formatsById();
        List<SurfaceChange> changes = new ArrayList<>();

        for (StoredFormatEntry oldEntry : baseline.formats()) {
            String formatId = oldEntry.formatId();
            StoredFormatEntry newEntry = newFormats.get(formatId);
            if (newEntry == null) {
                changes.add(change(
                        STORED_FORMAT_REMOVED,
                        formatId,
                        oldEntry.stability(),
                        oldEntry.rootTypeName() + "@v" + oldEntry.formatVersion(),
                        "",
                        "Retained stored format removed: " + formatId,
                        true));
                continue;
            }
            compareFormat(oldEntry, newEntry, changes);
        }

        for (StoredFormatEntry newEntry : current.formats()) {
            if (!oldFormats.containsKey(newEntry.formatId())) {
                changes.add(change(
                        STORED_FORMAT_ADDED,
                        newEntry.formatId(),
                        newEntry.stability(),
                        "",
                        newEntry.rootTypeName() + "@v" + newEntry.formatVersion(),
                        "Retained stored format added: " + newEntry.formatId(),
                        false));
            }
        }

        return List.copyOf(changes);
    }

    private static void compareFormat(
            StoredFormatEntry oldEntry,
            StoredFormatEntry newEntry,
            List<SurfaceChange> changes) {
        String formatId = oldEntry.formatId();
        StabilityTier tier = oldEntry.stability();

        if (!oldEntry.rootTypeName().equals(newEntry.rootTypeName())) {
            changes.add(change(
                    STORED_FORMAT_ROOT_TYPE_CHANGED,
                    formatId,
                    tier,
                    oldEntry.rootTypeName(),
                    newEntry.rootTypeName(),
                    "Stored format " + formatId + " changed root message type from "
                            + oldEntry.rootTypeName() + " to " + newEntry.rootTypeName(),
                    true));
        }

        if (newEntry.formatVersion() < oldEntry.formatVersion()) {
            changes.add(change(
                    STORED_FORMAT_VERSION_REGRESSED,
                    formatId,
                    tier,
                    "v" + oldEntry.formatVersion(),
                    "v" + newEntry.formatVersion(),
                    "Stored format " + formatId + " regressed formatVersion from "
                            + oldEntry.formatVersion() + " to " + newEntry.formatVersion(),
                    true));
        }

        if (tier.isDowngradeTo(newEntry.stability())) {
            changes.add(change(
                    STORED_FORMAT_STABILITY_DOWNGRADED,
                    formatId,
                    tier,
                    tier.name(),
                    newEntry.stability().name(),
                    "Stored format " + formatId + " downgraded stability from "
                            + tier + " to " + newEntry.stability(),
                    true));
        }

        if (!oldEntry.pinnedClosureSha256().isEmpty()) {
            String currentSha = newEntry.pinnedClosureSha256().isEmpty()
                    ? MessageFingerprints.sha256Hex(newEntry.descriptorClosure().toByteArray())
                    : newEntry.pinnedClosureSha256();
            if (!oldEntry.pinnedClosureSha256().equals(currentSha)
                    || !oldEntry.closureFingerprint().equals(newEntry.closureFingerprint())) {
                changes.add(change(
                        STORED_FORMAT_CLOSURE_PIN_CHANGED,
                        formatId,
                        tier,
                        oldEntry.pinnedClosureSha256(),
                        currentSha,
                        "Pinned stored format closure changed for " + formatId
                                + " (expected SHA-256 " + oldEntry.pinnedClosureSha256()
                                + ", found " + currentSha + ")",
                        true));
            }
        }

        CompatibilityResult schemaResult = STORED_SCHEMA_CHECKER.check(
                oldEntry.descriptorClosure(),
                newEntry.descriptorClosure(),
                oldEntry.compatibilityMode());
        Set<SchemaChange> violations = new HashSet<>(schemaResult.violations());
        for (SchemaChange sc : schemaResult.changes()) {
            boolean breaking = violations.contains(sc)
                    || ChangeRules.FIELD_REMOVED_NOT_RESERVED.equals(sc.ruleId());
            changes.add(change(
                    sc.ruleId(),
                    formatId + ":" + sc.path(),
                    tier,
                    sc.before(),
                    sc.after(),
                    "[" + formatId + "] " + sc.message(),
                    breaking));
        }
    }

    private static SurfaceChange change(
            String ruleId,
            String path,
            StabilityTier stability,
            String before,
            String after,
            String message,
            boolean breaking) {
        return new SurfaceChange(
                CompatibilitySurface.STORED_FORMAT,
                ruleId,
                path,
                stability,
                before,
                after,
                message,
                breaking);
    }
}
