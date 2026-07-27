package ai.pipestream.proto.repo.container.codec;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;

/**
 * Canned {@link PartLayout}s for the platform's message types.
 */
public final class PartLayouts {

    private PartLayouts() {
    }

    /**
     * The default four-part layout for {@link Document}: identity field
     * {@code doc_id}; BLOBS ← {@code blob_bag}; PARSED ← {@code parsed_metadata};
     * CHUNKS ← {@code search_metadata.semantic_results} keyed by
     * {@code result_id}. CORE is implicit — everything unmapped lands there.
     *
     * @return the default Document part layout
     */
    public static PartLayout document() {
        return PartLayout.builder(Document.getDescriptor())
                .identityField("doc_id")
                .partField(DocumentPart.DOCUMENT_PART_BLOBS, "blob_bag")
                .partField(DocumentPart.DOCUMENT_PART_PARSED, "parsed_metadata")
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS,
                        "search_metadata.semantic_results", "result_id")
                .build();
    }
}
