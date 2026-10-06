package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentCompositeSchemaResolution;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import java.util.*;

/** Owns source Uses for one whole-command assessment; grants no destination or publication authority. */
final class DocumentHistoricalAssessmentSources implements AutoCloseable {
    private record Key(NodeAddress address, UUID revision) {}
    private record Source(DocumentReadLedger.PinnedHistory history,
            DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use use) {}
    private final DocumentPublicationCommand command;
    private final Map<Key, Source> sources = new LinkedHashMap<>();
    private final Map<PublicationHistoricalReuse, DocumentHistoricalReadPlan.Entry> entries = new HashMap<>();
    private List<DocumentHistoricalReferenceAdmission.Prepared> references = List.of();
    private boolean closed;

    private DocumentHistoricalAssessmentSources(DocumentPublicationCommand command) { this.command = command; }

    static DocumentHistoricalAssessmentSources open(DocumentPublicationCommand command,
            ai.protomolt.proto.repo.spi.RepositoryCaller caller,
            List<DocumentReadLedger.PinnedHistory> histories, RepositoryReadControl control) {
        Objects.requireNonNull(command); Objects.requireNonNull(histories); Objects.requireNonNull(control).check();
        if (histories.size() > DocumentPublicationCommand.MAX_PARTS)
            throw new IllegalArgumentException("Historical source count exceeds command bound");
        var result = new DocumentHistoricalAssessmentSources(command);
        boolean delivered = false;
        try {
            for (var history : List.copyOf(histories)) {
                control.check();
                history.requireCaller(caller);
                var use = history.use();
                boolean retained = false;
                try {
                    var plan = use.plan();
                    var key = new Key(plan.address(), plan.revision());
                    if (!plan.address().getAccountId().equals(command.intent().getAccountId()) || result.sources.containsKey(key))
                        throw new IllegalArgumentException("Duplicate or cross-account historical capture");
                    result.sources.put(key, new Source(history, use)); retained = true;
                } finally { if (!retained) use.close(); }
            }
            var selected = new LinkedHashMap<Key, LinkedHashSet<PublicationHistoricalReuse>>();
            for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
                control.check();
                if (!part.hasHistoricalReuse()) continue;
                var selector = part.getHistoricalReuse();
                if (!part.getSlot().equals(selector.getSourceSlot()))
                    throw new IllegalArgumentException("Historical target slot differs from source slot");
                selected.computeIfAbsent(key(selector), ignored -> new LinkedHashSet<>()).add(selector);
            }
            if (selected.isEmpty() || !selected.keySet().equals(result.sources.keySet()))
                throw new IllegalArgumentException("Historical captures differ from complete command sources");
            var references = new ArrayList<DocumentHistoricalReferenceAdmission.Prepared>();
            for (var entry : selected.entrySet()) {
                var source = result.sources.get(entry.getKey());
                var selectors = List.copyOf(entry.getValue());
                var reference = DocumentHistoricalReferenceAdmission.prepare(source.history(), source.use(), selectors, control);
                var exact = reference.entries();
                for (int i = 0; i < selectors.size(); i++) result.entries.put(selectors.get(i), exact.get(i));
                references.add(reference);
            }
            result.references = DocumentHistoricalReferenceAdmission.requireComplete(command, references, control::check);
            result.authorize(control); delivered = true;
            return result;
        } finally { if (!delivered) result.close(); }
    }

    List<DocumentHistoricalReferenceAdmission.Prepared> references(DocumentPublicationCommand expected, Runnable control) {
        requireOpen();
        if (!command.equals(expected)) throw new IllegalArgumentException("Historical source command differs");
        return DocumentHistoricalReferenceAdmission.requireComplete(command, references, control);
    }

    void authorize(RepositoryReadControl control) {
        requireOpen();
        for (var source : sources.values()) {
            control.check(); source.use().plan(); source.history().authorizeDelivery(control); source.use().plan();
        }
    }

    void requireOpaque(DocumentPublicationMember member, RepositoryReadControl control) {
        requireOpen();
        if (!command.intent().getMembersList().contains(member)) throw new IllegalArgumentException("Historical assessment member differs");
        var checked = new HashSet<Key>();
        for (var part : member.getPartsList()) {
            control.check();
            if (!part.hasHistoricalReuse()) continue;
            var selector = part.getHistoricalReuse();
            if (!entries.containsKey(selector)) throw new IllegalArgumentException("Historical opaque selector differs from captured command");
            var key = key(selector);
            if (checked.add(key)) {
                var source = sources.get(key);
                source.history().requireOpaqueAdmission(source.use(), control::check);
            }
        }
    }

    MemberResolution resolve(DocumentPublicationMember member, Optional<DocumentSchemaAdmission.Definition> ordinaryContainer,
            DocumentPublicationCandidate.Resolver ordinary, DocumentSchemaAdmission.Limits limits,
            PayloadBudget budget, RepositoryReadControl control) {
        requireOpen();
        if (!command.intent().getMembersList().contains(member)) throw new IllegalArgumentException("Historical assessment member differs");
        var mappings = new LinkedHashMap<Key, Map<Integer, Integer>>();
        var selected = new LinkedHashMap<Key, List<DocumentHistoricalReadPlan.Entry>>();
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            control.check();
            var part = member.getParts(ordinal);
            if (!part.hasHistoricalReuse()) continue;
            var selector = part.getHistoricalReuse(); var key = key(selector);
            mappings.computeIfAbsent(key, ignored -> new LinkedHashMap<>()).put(ordinal, selector.getRevisionOrdinal());
            selected.computeIfAbsent(key, ignored -> new ArrayList<>()).add(Objects.requireNonNull(entries.get(selector)));
        }
        var loaders = new ArrayList<DocumentHistoricalSchemaResolution>();
        DocumentCompositeSchemaResolution composite = null;
        boolean delivered = false;
        try {
            for (var entry : mappings.entrySet()) {
                var source = sources.get(entry.getKey());
                loaders.add(DocumentHistoricalSchemaResolution.open(source.history(), source.use(), entry.getValue(),
                        selected.get(entry.getKey()), budget, control));
            }
            boolean needsOrdinaryContainer = mappings.isEmpty()
                    || member.getPartsList().stream().anyMatch(part -> part.hasUpload() || part.hasReuse());
            composite = DocumentCompositeSchemaResolution.open(member,
                    loaders.stream().map(DocumentHistoricalSchemaResolution::resolution).toList(),
                    needsOrdinaryContainer ? ordinaryContainer : Optional.empty(),
                    occurrence -> ordinary.select(member, occurrence), limits,
                    bytes -> {
                        try { var lease = budget.reserve(bytes); return lease::close; }
                        catch (PayloadBudget.CapacityExceededException exhausted) {
                            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                                    ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED,
                                    "Historical composite schema capacity exhausted", exhausted);
                        }
                    }, control::check);
            var result = new MemberResolution(loaders, composite); delivered = true; return result;
        } finally {
            if (!delivered) {
                if (composite != null) composite.close();
                for (int i = loaders.size() - 1; i >= 0; i--) loaders.get(i).close();
            }
        }
    }

    static final class MemberResolution implements AutoCloseable {
        private final List<DocumentHistoricalSchemaResolution> loaders;
        private final DocumentCompositeSchemaResolution composite;
        private MemberResolution(List<DocumentHistoricalSchemaResolution> loaders, DocumentCompositeSchemaResolution composite) {
            this.loaders = List.copyOf(loaders); this.composite = composite;
        }
        DocumentCompositeSchemaResolution resolver() { return composite; }
        @Override public void close() {
            composite.close();
            for (int i = loaders.size() - 1; i >= 0; i--) loaders.get(i).close();
        }
    }

    private static Key key(PublicationHistoricalReuse selector) {
        return new Key(selector.getSource(), UUID.fromString(selector.getRevisionId()));
    }
    private void requireOpen() { if (closed) throw new IllegalStateException("Historical assessment sources are closed"); }
    @Override public void close() {
        if (closed) return;
        closed = true;
        for (var source : sources.values()) source.use().close();
        sources.clear(); entries.clear(); references = List.of();
    }
}
