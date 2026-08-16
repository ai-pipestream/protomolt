package ai.pipestream.proto.search.door;

import java.util.Set;

/**
 * The slice of the door's index surface reconciliation works through:
 * enumerate what a subject currently serves, and remove what the source of
 * truth no longer has. Both methods refuse an unknown subject by name.
 */
public interface SubjectIndex {

    /**
     * The doc ids currently indexed under a subject.
     *
     * @param subjectName the mapping subject
     * @return the indexed doc ids, deleted-but-unmerged blocks excluded
     */
    Set<String> indexedDocIds(String subjectName);

    /**
     * Removes one document's block from a subject's index. Idempotent:
     * deleting an id the index does not hold succeeds and removes nothing.
     *
     * @param subjectName the mapping subject
     * @param docId the document identity to remove
     * @return the number of chunk children removed; 0 when the document had
     *         no chunks or was never indexed
     */
    int delete(String subjectName, String docId);
}
