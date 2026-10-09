package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.*;

/** Copies selected provider fragments while borrowing an accepted generation's source protection. */
final class DocumentHistoricalFragmentPreparation {
    private record Source(NodeAddress address, UUID revision) {}
    private record Selected(Source source, int ordinal) {}

    private DocumentHistoricalFragmentPreparation() {}

    /** Ordinary fragments stay borrowed; returned copies own their byte reservation. */
    static DocumentPublicationFragments capture(DocumentPublicationCommand command,
            DocumentHistoricalAssessmentSources sources, DocumentHistoricalAssessmentSources.Work accepted,
            DocumentHistoricalRetainedReader reader, Map<String, Map<Integer, ByteString>> ordinary,
            PayloadBudget budget, RepositoryReadControl control) {
        Objects.requireNonNull(reader); Objects.requireNonNull(ordinary); Objects.requireNonNull(budget);
        try (var work = accepted.fork(); var batches = new Batches()) {
            var references = work.references(command, control::check);
            work.authorize(control);
            work.histories(sources); // Reject a Work borrowed from a different source owner.
            var verified = new HashMap<PublicationHistoricalReuse, DocumentHistoricalReadPlan.Entry>();
            for (var reference : references) {
                var selectors = reference.selectors();
                var entries = reference.entries();
                for (int i = 0; i < selectors.size(); i++) verified.put(selectors.get(i), entries.get(i));
            }
            var supplied = new HashMap<String, Map<Integer, ByteString>>();
            ordinary.forEach((member, parts) -> supplied.put(member, new HashMap<>(parts)));
            // Validate the complete mapping before the first provider read.
            var memberIds = new HashSet<String>();
            for (var member : command.intent().getMembersList()) {
                memberIds.add(member.getMemberId());
                var parts = supplied.computeIfAbsent(member.getMemberId(), ignored -> new HashMap<>());
                var expectedOrdinary = new HashSet<Integer>();
                for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                    control.check();
                    var part = member.getParts(ordinal);
                    if (!part.hasHistoricalReuse()) {
                        if (!part.hasEmpty()) expectedOrdinary.add(ordinal);
                        continue;
                    }
                    var selector = part.getHistoricalReuse();
                    if (parts.containsKey(ordinal) || !part.getSlot().equals(selector.getSourceSlot())
                            || !verified.containsKey(selector))
                        throw new IllegalArgumentException("Historical fragment mapping differs from command");
                }
                if (!parts.keySet().equals(expectedOrdinary))
                    throw new IllegalArgumentException("Ordinary fragment ordinals differ from command");
            }
            if (!supplied.keySet().equals(memberIds))
                throw new IllegalArgumentException("Fragment members differ from command");
            var read = new HashMap<Selected, ByteString>();
            for (var member : command.intent().getMembersList()) {
                for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                    control.check();
                    var part = member.getParts(ordinal);
                    if (!part.hasHistoricalReuse()) continue;
                    var selector = part.getHistoricalReuse();
                    var entry = verified.get(selector);
                    var source = new Source(selector.getSource(), UUID.fromString(selector.getRevisionId()));
                    var selected = new Selected(source, entry.revisionOrdinal());
                    var bytes = read.get(selected);
                    if (bytes == null) {
                        var history = work.history(source.address(), source.revision());
                        var batch = Objects.requireNonNull(reader.readHistorical(history, selected.ordinal(), control));
                        batches.values.add(batch);
                        var fragments = batch.parts();
                        if (fragments.size() != 1) throw new IllegalArgumentException("Historical reader returned multiple fragments");
                        var fragment = fragments.getFirst();
                        if (fragment.part() != part.getSlot().getPart() || !fragment.subKey().equals(part.getSlot().getSubKey()))
                            throw new IllegalArgumentException("Historical reader returned a different slot");
                        // Borrow only while the batch is open. captureHistorical makes the owned copy.
                        bytes = UnsafeByteOperations.unsafeWrap(fragment.bytes());
                        read.put(selected, bytes);
                    }
                    supplied.get(member.getMemberId()).put(ordinal, bytes);
                }
            }
            var result = DocumentPublicationFragments.captureHistorical(command, supplied, references, budget, control::check);
            try { work.authorize(control); batches.close(); return result; }
            catch (RuntimeException | Error failure) { result.close(); throw failure; }
        }
    }

    private static final class Batches implements AutoCloseable {
        private final List<DocumentRetainedReader.Batch> values = new ArrayList<>();
        @Override public void close() {
            Throwable first = null;
            for (int i = values.size() - 1; i >= 0; i--) {
                try { values.remove(i).close(); }
                catch (RuntimeException | Error failure) {
                    if (first == null) first = failure;
                    else if (first != failure) first.addSuppressed(failure);
                }
            }
            if (first instanceof Error error) throw error;
            if (first instanceof RuntimeException failure) throw failure;
        }
    }
}
