package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Frozen physical bindings. Only an open assessment Use protects these during I/O. */
public final class DocumentAssessmentReadPlan {
    public record Entry(String member, int revisionOrdinal, UUID object, DocumentPublicationLedger.BoundPart part) {}
    private final DocumentAssessmentCreation.Created stage;
    private final List<Entry> entries;
    private final java.util.Set<String> members;

    DocumentAssessmentReadPlan(DocumentAssessmentCreation.Created stage, List<Entry> entries, java.util.Set<String> members) {
        this.stage = Objects.requireNonNull(stage);
        this.entries = List.copyOf(entries);
        this.members = java.util.Set.copyOf(members);
        if (entries.isEmpty() || entries.size() > 10000)
            throw new IllegalArgumentException("Assessment read plan requires 1..10000 retained slots");
    }
    DocumentAssessmentCreation.Created stage() { return stage; }
    public List<Entry> entries() { return entries; }
    public List<Entry> entries(String member) {
        if (!members.contains(Objects.requireNonNull(member))) throw new IllegalArgumentException("Member is absent from assessment");
        return entries.stream().filter(entry -> entry.member().equals(member)).toList();
    }
}
