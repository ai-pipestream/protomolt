package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;

/**
 * Canned {@link PartLayout}s for the platform's message types.
 */
public final class PartLayouts {

    private static final PartLayout DOCUMENT = buildDocument(Document.getDescriptor());

    private PartLayouts() {
    }

    /**
     * The default part layout for {@link Document}: identity field
     * {@code doc_id}; BLOBS ← {@code blob_bag}; PARSED ← {@code parser_results};
     * CHUNKS ← {@code search_metadata.semantic_results} chunked by
     * {@code result_id} runs, and one CHUNKS object per
     * {@code search_metadata.segmentations} entry keyed by
     * {@code segmentation_id}; EMBEDDINGS ← one object per
     * {@code search_metadata.embedding_sets} entry keyed by
     * {@code segmentation_id} and {@code embedding_set_id}. CORE is implicit:
     * everything unmapped lands there.
     *
     * @return the default Document part layout
     */
    public static PartLayout document() {
        return DOCUMENT;
    }

    /**
     * The same layout over another registered version of the {@code Document}
     * message, resolved by field name.
     *
     * @param documentType a {@code Document} descriptor
     * @return the default part layout over that descriptor
     * @throws IllegalArgumentException when the descriptor lacks a layout field
     */
    public static PartLayout document(com.google.protobuf.Descriptors.Descriptor documentType) {
        if (documentType == Document.getDescriptor()) {
            return DOCUMENT;
        }
        return buildDocument(documentType);
    }

    private static PartLayout buildDocument(com.google.protobuf.Descriptors.Descriptor documentType) {
        return PartLayout.builder(documentType)
                .identityField("doc_id")
                .partField(DocumentPart.DOCUMENT_PART_BLOBS, "blob_bag")
                .partField(DocumentPart.DOCUMENT_PART_PARSED, "parser_results")
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS,
                        "search_metadata.semantic_results", "result_id")
                .keyedPart(DocumentPart.DOCUMENT_PART_CHUNKS,
                        "search_metadata.segmentations", "segmentation_id")
                .keyedPart(DocumentPart.DOCUMENT_PART_EMBEDDINGS,
                        "search_metadata.embedding_sets", "segmentation_id", "embedding_set_id")
                .build();
    }
}
