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
    private final DocumentHistoricalSourceLifetime lifetime = new DocumentHistoricalSourceLifetime(this::releaseSources);

    /**
     * Hold through actual work and cleanup. For asynchronous submission, the submitter
     * must release rejected or confirmed never-started work; once started, only the
     * worker's completion releases it. Future cancellation alone is not completion.
     */
    Work work() {
        try { return new Work(lifetime.enter()); }
        catch (ai.protomolt.proto.repo.spi.RepositoryException closed) {
            if (closed.code() != ai.protomolt.proto.repo.spi.RepositoryException.Code.UNAVAILABLE) throw closed;
            throw new IllegalStateException("Historical assessment sources are closed", closed);
        }
    }
    final class Work implements AutoCloseable {
        private final DocumentHistoricalSourceLifetime.Work permit;
        private Work(DocumentHistoricalSourceLifetime.Work permit) { this.permit = permit; }
        List<DocumentHistoricalReferenceAdmission.Prepared> references(DocumentPublicationCommand expected, Runnable control) {
            permit.requireActive();
            if (!command.equals(expected)) throw new IllegalArgumentException("Historical source command differs");
            return DocumentHistoricalReferenceAdmission.requireComplete(command, references, control);
        }
        void authorize(RepositoryReadControl control) { permit.requireActive(); authorizeAccepted(control); }
        List<DocumentReadLedger.PinnedHistory> histories(DocumentHistoricalAssessmentSources expected) {
            permit.requireActive();
            if (expected != DocumentHistoricalAssessmentSources.this) throw new IllegalArgumentException("Source Work owner differs");
            return sources.values().stream().map(Source::history).toList();
        }
        void requireCaller(ai.protomolt.proto.repo.spi.RepositoryCaller caller) {
            permit.requireActive();
            for (var source : sources.values()) source.history().requireCaller(caller);
        }
        void requireOpaque(DocumentPublicationMember member, RepositoryReadControl control) {
            permit.requireActive(); requireOpaqueAccepted(member, control);
        }
        /** Accepted continuation from the same source owner; never reacquires source admission. */
        Work fork() { return new Work(permit.fork()); }

        /** The resolver owns a child permit, even if source admission has closed. */
        MemberResolution resolve(DocumentPublicationMember member, Optional<DocumentSchemaAdmission.Definition> container,
                DocumentPublicationCandidate.Resolver ordinary, DocumentSchemaAdmission.Limits limits,
                PayloadBudget budget, RepositoryReadControl control) {
            return resolveOwned(member, container, ordinary, limits, budget, control, new Work(permit.fork()));
        }
        @Override public void close() { permit.close(); }
    }

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
        try (var work = work()) { return work.references(expected, control); }
    }

    void authorize(RepositoryReadControl control) {
        try (var work = work()) { work.authorize(control); }
    }

    private void authorizeAccepted(RepositoryReadControl control) {
        for (var source : sources.values()) {
            control.check(); source.use().plan(); source.history().authorizeDelivery(control); source.use().plan();
        }
    }

    void requireCaller(ai.protomolt.proto.repo.spi.RepositoryCaller caller) {
        try (var work = work()) { work.requireCaller(caller); }
    }

    void requireOpaque(DocumentPublicationMember member, RepositoryReadControl control) {
        try (var work = work()) { requireOpaqueAccepted(member, control); }
    }

    private void requireOpaqueAccepted(DocumentPublicationMember member, RepositoryReadControl control) {
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
        return resolveOwned(member, ordinaryContainer, ordinary, limits, budget, control, work());
    }

    private MemberResolution resolveOwned(DocumentPublicationMember member, Optional<DocumentSchemaAdmission.Definition> ordinaryContainer,
            DocumentPublicationCandidate.Resolver ordinary, DocumentSchemaAdmission.Limits limits,
            PayloadBudget budget, RepositoryReadControl control, Work work) {
        try {
            return resolveAccepted(member, ordinaryContainer, ordinary, limits, budget, control, work);
        } catch (RuntimeException | Error failure) {
            try { work.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private MemberResolution resolveAccepted(DocumentPublicationMember member, Optional<DocumentSchemaAdmission.Definition> ordinaryContainer,
            DocumentPublicationCandidate.Resolver ordinary, DocumentSchemaAdmission.Limits limits,
            PayloadBudget budget, RepositoryReadControl control, Work work) {
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
        Throwable primary = null;
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
            var result = new MemberResolution(loaders, composite, work); delivered = true; return result;
        } catch (RuntimeException | Error failure) {
            primary = failure; throw failure;
        } finally {
            if (!delivered) {
                try { closeResolution(composite, loaders, work); }
                catch (RuntimeException | Error cleanup) {
                    if (primary == null) throw cleanup;
                    if (primary != cleanup) primary.addSuppressed(cleanup);
                }
            }
        }
    }

    static final class MemberResolution implements AutoCloseable {
        private final List<DocumentHistoricalSchemaResolution> loaders;
        private final DocumentCompositeSchemaResolution composite;
        private final Work work;
        private MemberResolution(List<DocumentHistoricalSchemaResolution> loaders, DocumentCompositeSchemaResolution composite, Work work) {
            this.loaders = List.copyOf(loaders); this.composite = composite; this.work = work;
        }
        DocumentCompositeSchemaResolution resolver() { return composite; }
        @Override public void close() {
            closeResolution(composite, loaders, work);
        }
    }

    private static void closeResolution(DocumentCompositeSchemaResolution composite,
            List<DocumentHistoricalSchemaResolution> loaders, Work work) {
        var cleanup = new ArrayList<AutoCloseable>();
        if (composite != null) cleanup.add(composite);
        for (int i = loaders.size() - 1; i >= 0; i--) cleanup.add(loaders.get(i));
        cleanup.add(work); // Last: drain cannot be observed while component cleanup still runs.
        closeAll(cleanup);
    }

    static void closeAll(List<? extends AutoCloseable> resources) {
        Throwable first = null;
        for (var resource : resources) {
            try { resource.close(); }
            catch (Exception | Error failure) {
                if (first == null) first = failure;
                else if (first != failure) first.addSuppressed(failure);
            }
        }
        if (first instanceof RuntimeException failure) throw failure;
        if (first instanceof Error failure) throw failure;
        if (first != null) throw new IllegalStateException("Historical schema cleanup failed", first);
    }

    private static Key key(PublicationHistoricalReuse selector) {
        return new Key(selector.getSource(), UUID.fromString(selector.getRevisionId()));
    }
    private void releaseSources() {
        for (var source : sources.values()) source.use().close();
        sources.clear(); entries.clear(); references = List.of();
    }
    @Override public void close() { lifetime.close(); }
    boolean awaitDrained(java.time.Duration timeout) throws InterruptedException { return lifetime.awaitDrained(timeout); }
}
