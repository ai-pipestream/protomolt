package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import java.util.Objects;

/**
 * Validates CHUNKS in manifest order without copying or normalizing original bytes.
 * One instance per revision; not thread-safe. After rejection it cannot be reused.
 * This checks run partition/names and field confinement, not schema or authority.
 * Naming rules are part of {@link DocumentFragmentConfinement#POLICY_ID}; changes
 * require a new retained layout policy, not reinterpretation of existing revisions.
 */
public final class DocumentChunkSequence {
    private final String docId;
    private final long maxElements;
    private final ChunkRunNames names = new ChunkRunNames();
    private long elements;
    private String priorRun;
    private boolean failed;

    public DocumentChunkSequence(String docId, long maxElements) {
        if (docId == null || docId.isBlank() || maxElements < 1)
            throw new IllegalArgumentException("Document identity and positive chunk element bound required");
        this.docId = docId;
        this.maxElements = maxElements;
    }

    public void accept(String subKey, Document fragment) {
        if (failed) throw new IllegalStateException("Chunk sequence previously failed");
        failed = true;
        Objects.requireNonNull(subKey, "subKey");
        DocumentFragmentConfinement.requireConfined(fragment, DocumentPart.DOCUMENT_PART_CHUNKS, docId);
        var results = fragment.getSearchMetadata().getSemanticResultsList();
        if (results.size() > maxElements - elements)
            throw new IllegalArgumentException("Chunk sequence exceeds element bound");
        String run = ChunkRunNames.key(results.getFirst().getResultId(), elements);
        if (run.equals(priorRun)) throw new IllegalArgumentException("Adjacent fragments split one chunk run");
        for (int i = 1; i < results.size(); i++) {
            if (!run.equals(ChunkRunNames.key(results.get(i).getResultId(), elements + i)))
                throw new IllegalArgumentException("Fragment combines distinct chunk runs");
        }
        if (!names.claim(run).equals(subKey)) throw new IllegalArgumentException("Chunk subkey differs from ordered run identity");
        elements += results.size();
        priorRun = run;
        failed = false;
    }
}
