package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;

import java.util.TreeMap;

/**
 * Manifest-slot calculations. A manifest holds one rendition instance per
 * (name, sub key) slot, ordered by name and then sub key; these functions
 * read and overlay those slots without touching storage.
 */
final class ArchiveSlots {

    private ArchiveSlots() {
    }

    /** Sorted map key for one rendition instance inside a manifest. */
    record Slot(String name, String subKey) implements Comparable<Slot> {
        @Override
        public int compareTo(Slot other) {
            int byName = name.compareTo(other.name);
            return byName != 0 ? byName : subKey.compareTo(other.subKey);
        }
    }

    /** The slot a rendition descriptor occupies. */
    static Slot of(RenditionDescriptor descriptor) {
        return new Slot(descriptor.getName(), descriptor.getSubKey());
    }

    /** The current manifest's renditions keyed by slot; empty when there is no current manifest. */
    static TreeMap<Slot, RenditionManifestEntry> slotsOf(VersionManifest current) {
        TreeMap<Slot, RenditionManifestEntry> slots = new TreeMap<>();
        if (current != null) {
            for (RenditionManifestEntry entry : current.getRenditionsList()) {
                slots.put(of(entry.getRendition()), entry);
            }
        }
        return slots;
    }

    /**
     * The manifest's primary rendition: "original", else the first rendition
     * a bridge did not produce. Derived renditions are excluded on purpose:
     * an entry must never end up characterized from its own bridge output.
     */
    static RenditionManifestEntry primaryOf(VersionManifest manifest) {
        RenditionManifestEntry first = null;
        for (RenditionManifestEntry item : manifest.getRenditionsList()) {
            String name = item.getRendition().getName();
            if (name.equals("original")) {
                return item;
            }
            if (first == null && !Bridges.derivedName(name)) {
                first = item;
            }
        }
        return first;
    }

    /** The primary rendition's name among a save's slots, or null when none. */
    static String primaryName(TreeMap<Slot, RenditionManifestEntry> slots) {
        if (slots.containsKey(new Slot("original", ""))) {
            return "original";
        }
        return slots.keySet().stream()
                .map(Slot::name)
                .filter(name -> !Bridges.derivedName(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * The primary rendition's windows when THIS save carries it; null when
     * the primary's bytes are not in the request (identification is then
     * skipped rather than invented, and ClassifyEntry re-reads from the store
     * on demand). A unary save holds the whole rendition already, so both
     * windows are resident and nothing is truncated away.
     */
    static ByteWindows primaryWindows(TreeMap<Slot, RenditionManifestEntry> slots,
                                      PutEntryRequest request) {
        String primary = primaryName(slots);
        if (primary == null) {
            return null;
        }
        for (RenditionContent content : request.getRenditionsList()) {
            if (content.getRendition().getName().equals(primary)
                    && !content.getData().isEmpty()) {
                return ByteWindows.ofWhole(content.getData().toByteArray());
            }
        }
        return null;
    }

    /**
     * The candidate carrying the retained slot's physical reference when both
     * hold the same present content, so unchanged bytes are never written
     * again; otherwise the candidate unchanged.
     */
    static RenditionManifestEntry shareRetainedObject(
            RenditionManifestEntry candidate, RenditionManifestEntry retained) {
        if (retained != null
                && candidate.getState() == RenditionState.RENDITION_STATE_PRESENT
                && retained.getState() == RenditionState.RENDITION_STATE_PRESENT
                && candidate.getSha256().equals(retained.getSha256())
                && candidate.getSizeBytes() == retained.getSizeBytes()) {
            return candidate.toBuilder().setObjectKey(retained.getObjectKey())
                    .setStorageObjectId(retained.getStorageObjectId()).build();
        }
        return candidate;
    }
}
