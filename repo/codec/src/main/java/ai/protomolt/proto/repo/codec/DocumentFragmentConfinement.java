package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
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
        Objects.requireNonNull(fragment, "fragment");
        Objects.requireNonNull(part, "part");
        if (expectedDocId == null || expectedDocId.isBlank() || !expectedDocId.equals(fragment.getDocId()))
            throw new IllegalArgumentException("Fragment document identity differs from destination");
        switch (part) {
            case DOCUMENT_PART_CORE -> {
                if (fragment.hasBlobBag() || fragment.getParserResultsCount() != 0
                        || fragment.getSearchMetadata().getSemanticResultsCount() != 0)
                    throw new IllegalArgumentException("CORE contains fields owned by another part");
            }
            case DOCUMENT_PART_BLOBS -> {
                requireRootFields(fragment, "blob_bag");
                if (!fragment.hasBlobBag()) throw new IllegalArgumentException("BLOBS has no owned field");
            }
            case DOCUMENT_PART_PARSED -> {
                requireRootFields(fragment, "parser_results");
                if (fragment.getParserResultsCount() == 0) throw new IllegalArgumentException("PARSED has no entries");
            }
            case DOCUMENT_PART_CHUNKS -> {
                requireRootFields(fragment, "search_metadata");
                var parent = fragment.getSearchMetadata();
                if (parent.getSemanticResultsCount() == 0) throw new IllegalArgumentException("CHUNKS has no entries");
                if (!parent.getUnknownFields().asMap().isEmpty()
                        || parent.getAllFields().keySet().stream().anyMatch(field -> !field.getName().equals("semantic_results")))
                    throw new IllegalArgumentException("CHUNKS parent contains unowned fields");
            }
            default -> throw new IllegalArgumentException("Unsupported document part");
        }
    }

    private static void requireRootFields(Document fragment, String owned) {
        var allowed = Set.of("doc_id", owned);
        if (!fragment.getUnknownFields().asMap().isEmpty()
                || fragment.getAllFields().keySet().stream().anyMatch(field -> !allowed.contains(field.getName())))
            throw new IllegalArgumentException("Non-CORE fragment contains unowned root fields");
    }
}
