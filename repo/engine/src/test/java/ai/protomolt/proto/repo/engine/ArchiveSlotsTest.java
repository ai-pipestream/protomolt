package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.engine.ArchiveSlots.Slot;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Slots order a manifest by rendition name and then sub key, which is the order the root
 * checksum is computed over; the primary rendition is "original" or the first non-derived
 * slot; unchanged present content shares the retained physical object.
 */
class ArchiveSlotsTest {

    private static final String TEXT = Bridges.renditionName(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);

    private static RenditionManifestEntry present(String name, String subKey, String sha, long size) {
        return RenditionManifestEntry.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name).setSubKey(subKey))
                .setState(RenditionState.RENDITION_STATE_PRESENT)
                .setSha256(sha)
                .setSizeBytes(size)
                .setObjectKey("key/" + name + "/" + subKey)
                .setStorageObjectId("object-" + name + "-" + subKey)
                .build();
    }

    private static VersionManifest manifest(RenditionManifestEntry... renditions) {
        return VersionManifest.newBuilder().addAllRenditions(List.of(renditions)).build();
    }

    @Test
    void slotsOrderByNameBeforeSubKeyAtEveryBoundaryAndMatchTheManifestOrder() {
        List<RenditionManifestEntry> scrambled = List.of(
                present("ab", "", "1", 1),
                present("a", "z", "2", 1),
                present("B", "", "3", 1),
                present("a", "", "4", 1),
                present("a", "a", "5", 1));

        TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(manifest(
                scrambled.toArray(RenditionManifestEntry[]::new)));

        assertThat(slots.keySet()).containsExactly(
                new Slot("B", ""), new Slot("a", ""), new Slot("a", "a"),
                new Slot("a", "z"), new Slot("ab", ""));
        List<RenditionManifestEntry> expected = new ArrayList<>(scrambled);
        expected.sort(ArchiveManifests.ORDER);
        assertThat(new ArrayList<>(slots.values())).containsExactlyElementsOf(expected);
    }

    @Test
    void absentManifestHasNoSlotsAndTheSameSlotOverwritesRatherThanDuplicates() {
        assertThat(ArchiveSlots.slotsOf(null)).isEmpty();

        TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(manifest(
                present("original", "", "old", 1)));
        RenditionManifestEntry replacement = present("original", "", "new", 2);
        slots.put(ArchiveSlots.of(replacement.getRendition()), replacement);

        assertThat(slots).containsOnlyKeys(new Slot("original", ""));
        assertThat(slots.get(new Slot("original", "")).getSha256()).isEqualTo("new");
    }

    @Test
    void primaryIsOriginalWhereverItSortsElseTheFirstRenditionABridgeDidNotProduce() {
        RenditionManifestEntry original = present("original", "", "o", 1);
        RenditionManifestEntry derived = present(TEXT, "", "t", 1);
        RenditionManifestEntry page = present("page", "1", "p", 1);

        assertThat(ArchiveSlots.primaryOf(manifest(derived, page, original))).isEqualTo(original);
        assertThat(ArchiveSlots.primaryOf(manifest(derived, page))).isEqualTo(page);
        assertThat(ArchiveSlots.primaryOf(manifest(derived))).isNull();

        assertThat(ArchiveSlots.primaryName(ArchiveSlots.slotsOf(manifest(page, original))))
                .isEqualTo("original");
        assertThat(ArchiveSlots.primaryName(ArchiveSlots.slotsOf(manifest(derived, page))))
                .isEqualTo("page");
        assertThat(ArchiveSlots.primaryName(ArchiveSlots.slotsOf(manifest(derived)))).isNull();
        assertThat(ArchiveSlots.primaryName(new TreeMap<>())).isNull();
    }

    @Test
    void primaryWindowsComeOnlyFromNonEmptyBytesThisSaveCarriesForThePrimary() {
        TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(manifest(
                present("original", "", "o", 3), present("page", "1", "p", 1)));
        PutEntryRequest carriesOriginal = PutEntryRequest.newBuilder()
                .addRenditions(content("page", "x"))
                .addRenditions(content("original", "abc"))
                .build();
        PutEntryRequest emptyOriginal = PutEntryRequest.newBuilder()
                .addRenditions(content("original", ""))
                .build();
        PutEntryRequest otherOnly = PutEntryRequest.newBuilder()
                .addRenditions(content("page", "x"))
                .build();

        assertThat(ArchiveSlots.primaryWindows(slots, carriesOriginal)).isNotNull();
        assertThat(ArchiveSlots.primaryWindows(slots, emptyOriginal)).isNull();
        assertThat(ArchiveSlots.primaryWindows(slots, otherOnly)).isNull();
        assertThat(ArchiveSlots.primaryWindows(new TreeMap<>(), carriesOriginal)).isNull();
    }

    @Test
    void identicalPresentContentSharesTheRetainedObjectAndAnyDifferenceKeepsTheCandidate() {
        RenditionManifestEntry retained = present("original", "", "sha", 10);
        RenditionManifestEntry candidate = retained.toBuilder()
                .setObjectKey("fresh/key").clearStorageObjectId().build();

        RenditionManifestEntry shared = ArchiveSlots.shareRetainedObject(candidate, retained);
        assertThat(shared.getObjectKey()).isEqualTo(retained.getObjectKey());
        assertThat(shared.getStorageObjectId()).isEqualTo(retained.getStorageObjectId());

        RenditionManifestEntry otherSize = candidate.toBuilder().setSizeBytes(11).build();
        RenditionManifestEntry otherSha = candidate.toBuilder().setSha256("other").build();
        RenditionManifestEntry empty = candidate.toBuilder()
                .setState(RenditionState.RENDITION_STATE_EMPTY).build();
        RenditionManifestEntry retainedEmpty = retained.toBuilder()
                .setState(RenditionState.RENDITION_STATE_EMPTY).build();
        assertThat(ArchiveSlots.shareRetainedObject(candidate, null)).isSameAs(candidate);
        assertThat(ArchiveSlots.shareRetainedObject(otherSize, retained)).isSameAs(otherSize);
        assertThat(ArchiveSlots.shareRetainedObject(otherSha, retained)).isSameAs(otherSha);
        assertThat(ArchiveSlots.shareRetainedObject(empty, retained)).isSameAs(empty);
        assertThat(ArchiveSlots.shareRetainedObject(candidate, retainedEmpty)).isSameAs(candidate);
    }

    private static RenditionContent content(String name, String data) {
        return RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name))
                .setData(ByteString.copyFromUtf8(data))
                .build();
    }
}
