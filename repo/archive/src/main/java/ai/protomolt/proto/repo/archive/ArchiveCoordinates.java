package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartSelector;
import java.util.Optional;

/**
 * Archive coordinates onto v2 call numbers and rendition names onto blob IDs.
 * Both mappings are field copies with no hashing, so every view field that
 * names an entry is a projection path and a read needs no lookup table.
 */
public final class ArchiveCoordinates {

    /** The reserved graph namespace of archive documents: {@code archive:<account_id>}. */
    public static final String GRAPH_PREFIX = "archive:";

    /** The rendition stored as the document's PARSED part rather than a raw blob. */
    public static final String PARSED = "parsed";

    /** The rendition whose blob carries the entry's filename. */
    public static final String ORIGINAL = "original";

    /** The separator between a rendition name and its sub-key in a blob ID. */
    public static final char SUB_KEY_SEPARATOR = '/';

    static final PartSelector CORE = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
    static final PartSelector BLOBS = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS).build();
    static final PartSelector PARSED_PART = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build();

    private ArchiveCoordinates() {
    }

    /** The graph every entry of {@code accountId}'s archives lives under. */
    public static String graphId(String accountId) {
        return GRAPH_PREFIX + accountId;
    }

    /** The call number of an entry: account, {@code archive:<account>}, archive name, entry ID. */
    public static NodeAddress address(EntryAddress entry) {
        return NodeAddress.newBuilder()
                .setAccountId(entry.getAccountId())
                .setGraphId(graphId(entry.getAccountId()))
                .setGraphAddressId(entry.getArchive())
                .setDocId(entry.getEntryId())
                .build();
    }

    /**
     * The archive entry address of a v2 document address under {@code archive:<account_id>}.
     *
     * @param address the v2 node address
     * @return the archive {@link EntryAddress}, or empty when {@code address} is not in the archive graph
     */
    public static Optional<EntryAddress> entryAddress(NodeAddress address) {
        if (address.getAccountId().isEmpty()
                || !address.getGraphId().equals(graphId(address.getAccountId()))
                || address.getGraphAddressId().isEmpty()
                || address.getDocId().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(EntryAddress.newBuilder()
                .setAccountId(address.getAccountId())
                .setArchive(address.getGraphAddressId())
                .setEntryId(address.getDocId())
                .build());
    }

    /**
     * The blob ID of a raw-blob rendition: the name, or {@code name/sub_key}.
     * Both halves are {@code path_safe_name}, which admits no {@code /}, so the
     * separator is unambiguous.
     */
    public static String blobId(String name, String subKey) {
        return subKey.isEmpty() ? name : name + SUB_KEY_SEPARATOR + subKey;
    }
}
