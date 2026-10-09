package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.Set;

/**
 * Field ownership for the Document v1 part layout. This checks a parsed semantic
 * view, never rewrites original bytes, and does not validate schema annotations,
 * ownership authority, chunk subkeys/order, checksums or a complete revision.
 * Admission evidence must retain this policy ID and the actual descriptor identity.
 */
public final class DocumentFragmentConfinement {
    public static final String POLICY_ID = "protomolt-document-parts/v1";
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
        if (expectedDocId == null || expectedDocId.isBlank() || !expectedDocId.equals(value(fragment, "doc_id")))
            throw new IllegalArgumentException("Fragment document identity differs from destination");
        switch (part) {
            case DOCUMENT_PART_CORE -> {
                if (present(fragment, "blob_bag") || present(fragment, "parser_results")
                        || present((Message) value(fragment, "search_metadata"), "semantic_results"))
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
                requireRootFields(fragment, "search_metadata");
                var parent = (Message) value(fragment, "search_metadata");
                if (!present(parent, "semantic_results")) throw new IllegalArgumentException("CHUNKS has no entries");
                if (!parent.getUnknownFields().asMap().isEmpty()
                        || parent.getAllFields().keySet().stream().anyMatch(field -> !field.getName().equals("semantic_results")))
                    throw new IllegalArgumentException("CHUNKS parent contains unowned fields");
            }
            default -> throw new IllegalArgumentException("Unsupported document part");
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
