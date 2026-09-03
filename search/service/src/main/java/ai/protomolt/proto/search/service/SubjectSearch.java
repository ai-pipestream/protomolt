package ai.protomolt.proto.search.service;

import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SubjectInfo;
import java.util.List;

/**
 * The query half of a served index: what a caller needs to ask a subject a question and to
 * find out which subjects there are to ask.
 *
 * <p>{@link SubjectIndex} is the other half, and the two are deliberately separate. That one
 * hands out the power to delete; this one hands out the power to read. A caller that only
 * searches should not be holding a handle that can also remove a document.
 */
public interface SubjectSearch {

    /**
     * Runs one query against a subject.
     *
     * @param subjectName the mapping subject to search
     * @param request the query, whose declared rules the caller has already enforced
     * @return the hits, best first, at most the request's {@code k}
     */
    List<SearchHit> search(String subjectName, SearchRequest request);

    /** The subjects this index serves, with the shape of each. */
    List<SubjectInfo> describeSubjects();
}
