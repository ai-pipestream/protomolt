package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.PublicationReuse;
import java.util.List;
import java.util.Objects;

/**
 * Ledger-issued, point-in-time evidence for retained reads in one canonical command.
 * This is neither a reader pin nor publication authority. The host must protect
 * objects through I/O and recheck authorization and revisions afterwards.
 * Fresh upload selections and their leases are deliberately not represented here.
 */
public final class DocumentRetainedReadPlan {
    public record Entry(String memberId, int revisionOrdinal, DocumentPublicationSlot destinationSlot,
            PublicationReuse source, DocumentPublicationLedger.Binding binding) {
        public Entry {
            Objects.requireNonNull(memberId); Objects.requireNonNull(destinationSlot);
            Objects.requireNonNull(source); Objects.requireNonNull(binding);
            if (revisionOrdinal < 0) throw new IllegalArgumentException("Negative revision ordinal");
        }
    }

    private final DocumentPublicationCommand command;
    private final String principal;
    private final long generation;
    private final List<Entry> entries;

    // Only the ledger issues plans after durable command, source and policy checks.
    DocumentRetainedReadPlan(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            List<Entry> entries) {
        this.command = Objects.requireNonNull(command);
        this.principal = owner.key().principal();
        this.generation = owner.generation();
        this.entries = List.copyOf(entries);
    }

    public DocumentPublicationCommand command() { return command; }
    /** Account and operation ID are retained by command(); this is its authenticated principal. */
    public String principal() { return principal; }
    public long generation() { return generation; }
    /** Command member order, then full revision ordinals, including gaps for non-reused parts. */
    public List<Entry> entries() { return entries; }
}
