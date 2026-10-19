package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Field ownership for the Document v1 part layout. This checks a parsed semantic
 * view, never rewrites original bytes, and does not validate schema annotations,
 * ownership authority, chunk subkeys/order, checksums or a complete revision.
 * Admission evidence must retain this policy ID and the actual descriptor identity.
 * <p>
 * CORE owns everything but {@code blob_bag}, {@code parser_results} and the three
 * semantic lists under {@code search_metadata}. A CHUNKS fragment holds either one
 * run of {@code semantic_results} or, when selected by segmentation ID, exactly
 * one {@code segmentations} entry with that ID. An EMBEDDINGS fragment holds
 * exactly one {@code embedding_sets} entry with the selected key.
 */
public final class DocumentFragmentConfinement {
    public static final String POLICY_ID = "protomolt-document-parts/v1";
    private static final Set<String> SEMANTIC_LISTS = Set.of("semantic_results", "segmentations", "embedding_sets");
    private DocumentFragmentConfinement() {}

    /**
     * Checks one PRESENT fragment. The caller bounds parsing and proves that the
     * pinned descriptor matches this generated Document type; this method does neither.
     */
    public static void requireConfined(Document fragment, DocumentPart part, String expectedDocId) {
        requireConfined((Message) fragment, part, expectedDocId);
    }

    /** Same policy for a dynamically decoded fragment under the caller's pinned Document descriptor. */
    public static void requireConfined(Message fragment, DocumentPart part, String expectedDocId) {
        Objects.requireNonNull(fragment, "fragment");
        Objects.requireNonNull(part, "part");
        requireIdentity(fragment, expectedDocId);
        switch (part) {
            case DOCUMENT_PART_CORE -> {
                var search = (Message) value(fragment, "search_metadata");
                if (present(fragment, "blob_bag") || present(fragment, "parser_results")
                        || SEMANTIC_LISTS.stream().anyMatch(list -> present(search, list)))
                    throw new IllegalArgumentException("CORE contains fields owned by another part");
            }
            case DOCUMENT_PART_BLOBS -> {
                requireRootFields(fragment, "blob_bag");
                if (!present(fragment, "blob_bag")) throw new IllegalArgumentException("BLOBS has no owned field");
            }
            case DOCUMENT_PART_PARSED -> {
                requireRootFields(fragment, "parser_results");
                if (!present(fragment, "parser_results")) throw new IllegalArgumentException("PARSED has no entries");
            }
            case DOCUMENT_PART_CHUNKS -> {
                var parent = semanticParent(fragment, "CHUNKS", "semantic_results");
                if (!present(parent, "semantic_results")) throw new IllegalArgumentException("CHUNKS has no entries");
            }
            default -> throw new IllegalArgumentException("Unsupported document part");
        }
    }

    /**
     * Checks one PRESENT CHUNKS fragment selected by segmentation ID: {@code doc_id}
     * plus exactly one {@code search_metadata.segmentations} entry carrying that ID.
     *
     * @param fragment the decoded fragment
     * @param segmentationId the selector's segmentation ID
     * @param expectedDocId the destination document's ID
     * @throws IllegalArgumentException when the fragment holds anything else
     */
    public static void requireConfinedSegmentation(Message fragment, String segmentationId, String expectedDocId) {
        Objects.requireNonNull(fragment, "fragment");
        requireIdentity(fragment, expectedDocId);
        var parent = semanticParent(fragment, "CHUNKS", "segmentations");
        requireSingleKeyed(parent, "segmentations", "CHUNKS", List.of("segmentation_id"), List.of(segmentationId));
    }

    /**
     * Checks one PRESENT EMBEDDINGS fragment: {@code doc_id} plus exactly one
     * {@code search_metadata.embedding_sets} entry carrying the selected key.
     *
     * @param fragment the decoded fragment
     * @param segmentationId the selector's segmentation ID
     * @param embeddingSetId the selector's embedding set ID
     * @param expectedDocId the destination document's ID
     * @throws IllegalArgumentException when the fragment holds anything else
     */
    public static void requireConfinedEmbeddingSet(Message fragment, String segmentationId, String embeddingSetId,
                                                   String expectedDocId) {
        Objects.requireNonNull(fragment, "fragment");
        requireIdentity(fragment, expectedDocId);
        var parent = semanticParent(fragment, "EMBEDDINGS", "embedding_sets");
        requireSingleKeyed(parent, "embedding_sets", "EMBEDDINGS", List.of("segmentation_id", "embedding_set_id"),
                List.of(segmentationId, embeddingSetId));
    }

    private static void requireIdentity(Message fragment, String expectedDocId) {
        if (expectedDocId == null || expectedDocId.isBlank() || !expectedDocId.equals(value(fragment, "doc_id")))
            throw new IllegalArgumentException("Fragment document identity differs from destination");
    }

    /** The fragment's {@code search_metadata}, which may carry only the named list. */
    private static Message semanticParent(Message fragment, String partName, String owned) {
        requireRootFields(fragment, "search_metadata");
        var parent = (Message) value(fragment, "search_metadata");
        if (!parent.getUnknownFields().asMap().isEmpty()
                || parent.getAllFields().keySet().stream().anyMatch(field -> !field.getName().equals(owned)))
            throw new IllegalArgumentException(partName + " parent contains unowned fields");
        return parent;
    }

    private static void requireSingleKeyed(Message parent, String list, String partName, List<String> keyFields,
                                           List<String> expectedKey) {
        var field = parent.getDescriptorForType().findFieldByName(list);
        if (field == null) throw new IllegalArgumentException("Document layout field missing: " + list);
        if (parent.getRepeatedFieldCount(field) != 1)
            throw new IllegalArgumentException(partName + " holds " + parent.getRepeatedFieldCount(field)
                    + " entries; a selected object holds exactly one");
        var element = (Message) parent.getRepeatedField(field, 0);
        for (int i = 0; i < keyFields.size(); i++) {
            var expected = expectedKey.get(i);
            if (expected == null || expected.isBlank())
                throw new IllegalArgumentException(partName + " selector " + keyFields.get(i) + " is blank");
            if (!expected.equals(value(element, keyFields.get(i))))
                throw new IllegalArgumentException(partName + " entry " + keyFields.get(i) + " differs from its selector");
        }
    }

    private static Object value(Message message, String name) {
        var field = message.getDescriptorForType().findFieldByName(name);
        if (field == null) throw new IllegalArgumentException("Document layout field missing: " + name);
        return message.getField(field);
    }

    private static boolean present(Message message, String name) {
        var field = message.getDescriptorForType().findFieldByName(name);
        if (field == null) throw new IllegalArgumentException("Document layout field missing: " + name);
        return field.isRepeated() ? message.getRepeatedFieldCount(field) != 0 : message.hasField(field);
    }

    private static void requireRootFields(Message fragment, String owned) {
        var allowed = Set.of("doc_id", owned);
        if (!fragment.getUnknownFields().asMap().isEmpty()
                || fragment.getAllFields().keySet().stream().anyMatch(field -> !allowed.contains(field.getName())))
            throw new IllegalArgumentException("Non-CORE fragment contains unowned root fields");
    }
}
