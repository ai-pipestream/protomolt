package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Frozen physical bindings. Only an open assessment Use protects these during I/O. */
record DocumentAssessmentReadPlan(DocumentAssessmentCreation.Created stage, List<Entry> entries) {
    record Entry(String member, int revisionOrdinal, UUID object, DocumentPublicationLedger.BoundPart part) {}
    DocumentAssessmentReadPlan {
        Objects.requireNonNull(stage);
        entries = List.copyOf(entries);
        if (entries.isEmpty() || entries.size() > 10000)
            throw new IllegalArgumentException("Assessment read plan requires 1..10000 retained slots");
    }
}
