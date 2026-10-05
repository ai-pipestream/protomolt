package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryReadControl;

/** Provider port for retained candidate fragments, never a public document or semantic-review API. */
@FunctionalInterface
public interface DocumentAssessmentReader {
    /**
     * Return this member's nonempty fragments in full revision-ordinal order, using
     * the frozen backend and provider versions. Keep a Use through actual workers
     * and returned batch lifetime. Reauthorize bytes and detailed provider errors
     * using the capture-bound delivery gate after I/O; never hold SQL locks during I/O.
     */
    DocumentRetainedReader.Batch readAssessment(DocumentReadLedger.PinnedAssessment assessment,
            String member, RepositoryReadControl control);
}
