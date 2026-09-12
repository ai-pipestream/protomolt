package ai.protomolt.proto.asset.catalog;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.v1.AssetCatalogRow;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;

import java.util.Locale;

/**
 * The projection: an archive entry and the manifest of its current version
 * become one flat catalog row.
 *
 * <p>Every value the row carries is already stored. Nothing here computes a
 * new fact, reads an object, or asks another service — which is what keeps
 * the catalog a view of the archive rather than a second copy of it that can
 * disagree.
 *
 * <p>Two conventions the row commits to, both so a facet reads as itself:
 * enums project as their bare names ({@code VERIFIED}, {@code OCR_TEXT})
 * rather than numbers, and an unmeasured quality projects as -1 rather than
 * 0, because "nobody scored this" and "this scored zero" are different
 * answers.
 */
public final class AssetCatalogRows {

    /** The quality a row reports when no rendition measured one. */
    public static final double UNMEASURED_QUALITY = -1.0;

    private static final String STATE_PREFIX = "CLASSIFICATION_STATE_";
    private static final String CLASS_PREFIX = "CONTENT_CLASS_";

    private AssetCatalogRows() {
    }

    /**
     * Projects one asset.
     *
     * @param info the entry's stored metadata
     * @param manifest the current version's manifest
     * @return the catalog row
     */
    public static AssetCatalogRow of(EntryInfo info, VersionManifest manifest) {
        Classification classification = info.getClassification();
        ClassificationState state = classification.getState();
        AssetCatalogRow.Builder row = AssetCatalogRow.newBuilder()
                .setAccountId(info.getAddress().getAccountId())
                .setArchive(info.getAddress().getArchive())
                .setEntryUuid(info.getEntryUuid())
                .setEntryId(info.getAddress().getEntryId())
                .setFilename(info.getFilename())
                .setVersion(info.getCurrentVersion())
                .setClassificationState(stateName(state))
                .setFormatKind(formatKind(classification))
                .setVerified(state == ClassificationState.CLASSIFICATION_STATE_VERIFIED)
                .setContentQuality(UNMEASURED_QUALITY);
        if (info.hasUpdatedAt()) {
            row.setUpdatedAt(info.getUpdatedAt());
        }
        if (classification.hasClassifiedAt()) {
            row.setClassifiedAt(classification.getClassifiedAt());
        }

        long bytes = 0;
        long renditions = 0;
        long derived = 0;
        double quality = UNMEASURED_QUALITY;
        String contentClass = "";
        for (RenditionManifestEntry rendition : manifest.getRenditionsList()) {
            if (rendition.getState() != RenditionState.RENDITION_STATE_PRESENT) {
                // A tombstone keeps its size as provenance, but the asset no
                // longer stores those bytes and the catalog must not claim it
                // does.
                continue;
            }
            renditions++;
            bytes += rendition.getSizeBytes();
            if (Bridges.derivedName(rendition.getRendition().getName())) {
                derived++;
            }
            if (!rendition.hasContentProfile()) {
                continue;
            }
            ContentProfile profile = rendition.getContentProfile();
            if (contentClass.isEmpty()) {
                contentClass = contentClassName(profile);
            }
            if (profile.hasQuality()) {
                quality = Math.max(quality, profile.getQuality().getScore());
            }
        }
        return row.setSizeBytes(bytes)
                .setRenditionCount(renditions)
                .setDerivedRenditions(derived)
                .setBridged(derived > 0)
                .setContentClass(contentClass)
                .setContentQuality(quality)
                .setQualityMeasured(quality >= 0)
                .build();
    }

    /** The state's stored name, without the enum's prefix. */
    private static String stateName(ClassificationState state) {
        String name = state.name();
        return name.startsWith(STATE_PREFIX) ? name.substring(STATE_PREFIX.length()) : name;
    }

    /** The content class's name, without the enum's prefix. */
    private static String contentClassName(ContentProfile profile) {
        String name = profile.getContentClass().name();
        return name.startsWith(CLASS_PREFIX) ? name.substring(CLASS_PREFIX.length()) : name;
    }

    /**
     * The format of record's kind, lower case. Blank when the state names no
     * single format, so "we do not know" groups as its own facet value
     * instead of hiding among the formats.
     */
    private static String formatKind(Classification classification) {
        FormatFact format = Bridges.formatOfRecord(classification);
        if (format == null || format.getFormatCase() == FormatFact.FormatCase.FORMAT_NOT_SET) {
            return "";
        }
        return format.getFormatCase().name().toLowerCase(Locale.ROOT);
    }
}
