package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/** Private historical ownership from proposal through execution. Does not enable public recovery. */
final class RepositoryInstalledHistoricalAttempts implements AutoCloseable {
    record Drain(int active, int unresolved) {}
    enum Retirement { NOT_PROVEN, RETAINED, RETIRED }
    /** A borrowed generation's local state, not authorization or evidence of a durable outcome. */
    record Progress(UUID identity, boolean disposalOnly, boolean sourcesAttached, boolean assessmentPrepared,
            Optional<DocumentHistoricalExecution.Progress> execution,
            Optional<DocumentAssessmentCreation.Created> acknowledgedCreation) {}
    private enum RetirementProof { NONE, TERMINAL, FENCED }
    private final Tx tx;
    private final PayloadBudget budget;
    private final DriveLedger drives;
    private final int capacity;
    private final Map<RepositoryOperationLedger.Key, Entry> entries = new HashMap<>();
    // Selected retry route and all retained generations are separate indexes.
    private final Map<UUID, Entry> generations = new LinkedHashMap<>();
    private boolean closed;
    private boolean detaching;
    private int active;

    private static final class Entry {
        final UUID id = UUID.randomUUID();
        UUID predecessorId;
        volatile boolean supersessionPending;
        final RepositoryCaller caller;
        final RepositoryOperationLedger.Key key;
        final DocumentPublicationCommand command;
        final RepositoryCoordinatorReservation.Proposal reservation;
        final RepositoryHistoricalAttemptPreparation preparation;
        final Initial initial;
        RepositorySuccessorInstall.Plan plan;
        final DocumentPublicationPreparationRecord retention;
        final DocumentSuccessorFingerprint fingerprint;
        final ByteString retentionDigest;
        final PayloadBudget.Lease bytes;
        final DocumentPublicationScopeCalls scopes = new DocumentPublicationScopeCalls();
        final DocumentPublicationScopeCalls.Call parent = scopes.enter();
        boolean borrowed;
        volatile RetirementProof retirement = RetirementProof.NONE;
        volatile DocumentHistoricalAssessmentSources sources;
        DocumentHistoricalAssessmentSources.Work work;
        List<DocumentReadLedger.PinnedHistory> histories = List.of();
        RepositoryHistoricalSuccessorActivation activation;
        RepositoryInitialHistoricalAttempt initialAttempt;
        DocumentHistoricalExecution execution;
        DocumentPublicationAssessment.Historical assessment;
        DocumentAssessmentCreation.Created stage;
        Entry(RepositoryCaller caller, RepositorySuccessorInstall.Plan plan,
                DocumentPublicationPreparationRecord retention, DocumentSuccessorFingerprint fingerprint,
                ByteString retentionDigest, PayloadBudget.Lease bytes) {
            this.caller = caller; this.plan = plan; this.retention = retention;
            this.fingerprint = fingerprint; this.retentionDigest = retentionDigest; this.bytes = bytes;
            key = plan.next().key(); command = plan.next().command(); reservation = plan.reservation(); preparation = null; initial = null;
        }
        Entry(RepositoryCaller caller, DocumentPublicationPreparationRecord retention,
                RepositoryCoordinatorReservation.Proposal reservation, RepositoryHistoricalAttemptPreparation preparation,
                PayloadBudget.Lease bytes, ByteString retentionDigest) {
            this.caller = caller; this.retention = retention; this.reservation = reservation;
            this.preparation = preparation; this.bytes = bytes; initial = null;
            key = retention.key(); command = retention.command(); fingerprint = null; this.retentionDigest = retentionDigest;
        }
        Entry(RepositoryCaller caller, DocumentPublicationPreparationRecord record, Initial initial,
                PayloadBudget.Lease bytes, ByteString digest) {
            this.caller = caller; retention = record; this.initial = initial; this.bytes = bytes;
            key = record.key(); command = record.command(); retentionDigest = digest;
            reservation = null; preparation = null; fingerprint = null;
        }
        RepositoryCoordinatorReservation.Proposal reservation() {
            return preparation == null ? reservation : preparation.proposal();
        }
        void requireSettled() { if (preparation != null) preparation.requireSettled(); }
        void releaseBytes() {
            if (preparation != null) preparation.close();
            bytes.close();
        }
    }

    private record Initial(Map<String, DocumentPublicationCandidate.Mode> modes, UUID incarnation) {
        Initial { modes = Map.copyOf(modes); Objects.requireNonNull(incarnation); }
    }

    RepositoryInstalledHistoricalAttempts(Tx tx, PayloadBudget budget, DriveLedger drives, int capacity) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
        this.drives = Objects.requireNonNull(drives);
        if (capacity < 1) throw new IllegalArgumentException("Historical attempt capacity must be positive");
        this.capacity = capacity;
    }

    /** Reserve the generation slot before opening or accepting its initial capture. No SQL. */
    synchronized Attempt beginInitial(RepositoryCaller caller, DocumentPublicationPreparationRecord record,
            Map<String, DocumentPublicationCandidate.Mode> modes, UUID incarnation) {
        if (closed) throw unavailable();
        DocumentAdmissionAuthorization.requireCaller(caller, record.key(), record.key().account());
        if (record.predecessorGeneration() != 0 || DocumentPreparationHistoryRoots.roots(record.command()).isEmpty())
            throw new IllegalArgumentException("Initial historical ownership requires an initial historical command");
        var initial = new Initial(modes, incarnation);
        var encodedModes = DocumentPublicationModesJournal.encode(record.command(), initial.modes());
        var existing = entries.get(record.key());
        if (existing != null) {
            if (!existing.caller.equals(caller) || !initial.equals(existing.initial))
                throw conflict("Initial historical retry identity changed");
            if (!existing.retention.equals(record)) {
                try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
                    if (!existing.retentionDigest.equals(digest(DocumentPublicationPreparationCodec.encode(record))))
                        throw conflict("Initial historical retry preparation changed");
                }
            }
            return resume(caller, record.command()).orElseThrow();
        }
        if (generations.size() >= capacity || active >= capacity)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical attempt capacity exhausted");
        try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var encoded = DocumentPublicationPreparationCodec.encode(record);
            var bytes = budget.reserve((long) encoded.size() + encodedModes.length() * 2L);
            try {
                var entry = new Entry(caller, record, initial, bytes, digest(encoded));
                retain(entry); entry.borrowed = true; active++;
                return new Attempt(entry);
            } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
        }
    }

private void retain(Entry entry) {
    generations.put(entry.id, entry);
    entries.put(entry.key, entry);
}
private boolean forget(Entry entry) {
    if (!generations.remove(entry.id, entry)) return false;
    entries.remove(entry.key, entry);
    return true;
}

/** Allocate one successor before SQL; old accepted workers stay owned and are fenced by V97. */
synchronized Attempt beginSuccessor(RepositoryCaller coordinator, RepositoryCaller caller, UUID predecessorId,
        DocumentPublicationCommand command, Map<String, DocumentPublicationCandidate.Mode> modes,
        RepositoryCoordinatorRecoveryDiscovery.Observation observed, Duration lease, SqlTimeouts timeouts) {
    if (closed) throw unavailable();
    Objects.requireNonNull(command); Objects.requireNonNull(predecessorId);
    var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
    DocumentAdmissionAuthorization.requireCaller(coordinator, key, key.account());
    if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
            "Historical generation takeover requires private process authority");
    var selected = entries.get(key);
        if (selected != null && predecessorId.equals(selected.predecessorId)) {
            if (!selected.caller.equals(caller) || !selected.command.canonical().equals(command.canonical()))
                throw conflict("Historical successor retry identity changed");
            return beginProposed(caller, selected.retention, modes, observed, lease, timeouts);
        }
    var old = generations.get(predecessorId);
    if (old == null || selected != old || !old.key.equals(key) || !old.caller.equals(caller)
            || !old.command.canonical().equals(command.canonical()))
        throw conflict("Historical predecessor differs from retained generation");
    if (old.sources == null || old.plan == null && old.initialAttempt == null
            || old.retirement != RetirementProof.NONE || old.supersessionPending)
        throw conflict("Historical predecessor is not an attached current generation");
    if (!(old.initial == null ? old.plan.modes() : old.initial.modes()).equals(modes)) throw conflict("Historical generation modes changed");
    var source = Objects.requireNonNull(observed).candidate().orElseThrow(() ->
            conflict("Historical generation takeover requires an expired bound predecessor"));
    var expected = claimIdentity(old);
    var previous = old.initial == null ? old.plan.next() : old.retention;
    var expectedOwner = new RepositoryCoordinatorReservation.OwnerIdentity(
            Math.addExact(previous.predecessorGeneration(), 1), previous.seeds().ownerNonce());
    if (!expected.equals(source.predecessor()) || !expectedOwner.equals(source.owner()))
        throw conflict("Historical takeover observation differs from retained predecessor");
    // Admission is memory-only and exclusive under this monitor. Rollback restores routing on refusal.
    entries.remove(key, old);
    try {
        var successor = beginProposed(caller, old.retention, modes, observed, lease, timeouts);
        successor.entry.predecessorId = old.id;
        old.supersessionPending = true;
        return successor;
    } catch (RuntimeException | Error failure) {
        entries.put(key, old);
        throw failure;
    }
}

/** Private cleanup lookup; an old generation is never selected for ordinary retry routing. */
synchronized Optional<Attempt> resumeGeneration(RepositoryCaller coordinator, RepositoryCaller caller,
        DocumentPublicationCommand command, UUID id) {
    if (closed) throw unavailable();
    var entry = generations.get(Objects.requireNonNull(id));
    var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
    DocumentAdmissionAuthorization.requireCaller(coordinator, key, key.account());
    if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
            "Historical generation disposal requires private process authority");
    if (entry == null) return Optional.empty();
    if (!entry.key.equals(key) || !entry.caller.equals(caller) || !entry.command.canonical().equals(command.canonical()))
        throw conflict("Historical disposal generation identity changed");
    if (entry.borrowed) throw conflict("Historical attempt is in use");
    if (active >= capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
            "Historical call capacity exhausted");
    entry.borrowed = true; active++;
    return Optional.of(new Attempt(entry));
}

    /** No SQL; reserves capacity before capture transfer or activation registration. */
    synchronized Attempt beginInstalled(RepositoryCaller caller, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention) {
        if (closed) throw unavailable();
        Objects.requireNonNull(caller); Objects.requireNonNull(plan); Objects.requireNonNull(retention);
        var key = plan.next().key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!retention.key().equals(key) || !retention.command().canonical().equals(plan.next().command().canonical())
                || retention.predecessorGeneration() >= plan.next().predecessorGeneration())
            throw new IllegalArgumentException("Historical retention differs from installed plan");
        var existing = entries.get(key);
        if (existing != null && existing.borrowed) throw conflict("Historical attempt is in use");
        if (active >= capacity || existing == null && generations.size() >= capacity)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical attempt capacity exhausted");
        if (existing != null && existing.caller.equals(caller) && Objects.equals(existing.plan, plan)
                && existing.retention.equals(retention)) {
            existing.borrowed = true; active++;
            return new Attempt(existing);
        }
        if (existing != null && (existing.preparation != null || existing.initial != null)) throw conflict("Historical retry must resume retained preparation");
        // Scratch is temporary; retained accounting uses the actual bounded encoded sizes.
        try (var scratch = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES
                + DocumentPublicationModesJournal.MAX_BYTES)) {
            var previous = DocumentPublicationPreparationCodec.encode(plan.previous());
            var next = DocumentPublicationPreparationCodec.encode(plan.next());
            var retained = DocumentPublicationPreparationCodec.encode(retention);
            var modes = ByteString.copyFromUtf8(RepositorySuccessorInstall.encodeModes(plan));
            var fingerprint = new DocumentSuccessorFingerprint(plan.reservation(), digest(previous), digest(next), digest(modes));
            var retentionDigest = digest(retained);
            if (existing != null) {
                if (!existing.caller.equals(caller) || !existing.fingerprint.equals(fingerprint)
                        || !existing.retentionDigest.equals(retentionDigest))
                    throw conflict("Historical retry identity changed");
            } else {
                var bytes = budget.reserve((long) previous.size() + next.size() + retained.size() + modes.size());
                try {
                    existing = new Entry(caller, plan, retention, fingerprint, retentionDigest, bytes);
                    retain(existing);
                } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
            }
            existing.borrowed = true; active++;
            return new Attempt(existing);
        }
    }

    /** No SQL. Store one proposal before reservation; newer discovery never replaces a retained entry. */
    synchronized Attempt beginProposed(RepositoryCaller caller, DocumentPublicationPreparationRecord retention,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            RepositoryCoordinatorRecoveryDiscovery.Observation observed, Duration lease, SqlTimeouts timeouts) {
        if (closed) throw unavailable();
        Objects.requireNonNull(caller); Objects.requireNonNull(retention); Objects.requireNonNull(timeouts);
        Objects.requireNonNull(lease);
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Historical recovery lease requires one second to one day");
        var key = retention.key(); var command = retention.command();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var requested = Map.copyOf(modes);
        var encodedModes = DocumentPublicationModesJournal.encode(command, requested);
        var existing = entries.get(key);
        if (existing != null) {
            if (!existing.caller.equals(caller) || existing.preparation == null || !existing.preparation.matches(requested, lease, timeouts))
                throw conflict("Historical retry identity changed");
            if (!existing.retention.equals(retention)) {
                try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
                    if (!existing.retentionDigest.equals(digest(DocumentPublicationPreparationCodec.encode(retention))))
                        throw conflict("Historical retry retention changed");
                }
            }
            return resume(caller, command).orElseThrow();
        }
        if (generations.size() >= capacity || active >= capacity)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical attempt capacity exhausted");
        try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var retained = DocumentPublicationPreparationCodec.encode(retention);
            var bytes = budget.reserve((long) retained.size() + command.canonical().size()
                    + command.intent().getSerializedSize() + encodedModes.length() * 2L);
            try {
                Objects.requireNonNull(observed);
                RepositoryCoordinatorReservation.Proposal proposal;
                if (observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND) {
                    var source = observed.candidate().orElseThrow();
                    proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(source.predecessor(),
                            UUID.randomUUID(), UUID.randomUUID(), lease, source.owner());
                } else if (observed.unactivated().isPresent()) {
                    var source = observed.unactivated().orElseThrow();
                    proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                            UUID.randomUUID(), UUID.randomUUID(), lease, source.owner(), source.preparationSha256(), source.installation());
                } else throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Historical recovery state is not eligible: " + observed.status());
                var owner = RepositoryCoordinatorReservation.owner(proposal).orElseThrow();
                if (!proposal.predecessor().key().equals(key) || !proposal.predecessor().commandSha256().equals(command.sha256())
                        || owner.generation() <= retention.predecessorGeneration()
                        || owner.generation() == retention.predecessorGeneration() + 1 && !owner.nonce().equals(retention.seeds().ownerNonce()))
                    throw conflict("Historical recovery observation differs from retained command");
                var preparation = new RepositoryHistoricalAttemptPreparation(tx, budget, timeouts, proposal, command, requested, lease,
                        retention, DocumentPublicationPreparationJournal.digest(retained));
                var entry = new Entry(caller, retention, proposal, preparation, bytes, digest(retained));
                retain(entry); entry.borrowed = true; active++;
                return new Attempt(entry);
            } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
        }
    }

    /** Resume before discovery or allocating another capture. The exact entry's plan remains fixed. */
    synchronized Optional<Attempt> resume(RepositoryCaller caller, DocumentPublicationCommand command) {
        if (closed) throw unavailable();
        Objects.requireNonNull(caller); Objects.requireNonNull(command);
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        var entry = entries.get(key);
        if (entry == null) return Optional.empty();
        if (!entry.caller.equals(caller) || !entry.command.canonical().equals(command.canonical()))
            throw conflict("Historical retry identity changed");
        if (entry.borrowed) throw conflict("Historical attempt is in use");
        if (active >= capacity) throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                "Historical call capacity exhausted");
        entry.borrowed = true; active++;
        return Optional.of(new Attempt(entry));
    }

    final class Attempt implements AutoCloseable {
        private final Entry entry;
        private boolean ended;
        private Attempt(Entry entry) { this.entry = entry; }
        synchronized UUID identity() { requireActive(RepositoryReadControl.NONE); return entry.id; }
        synchronized Progress progress(RepositoryReadControl control) {
            requireActive(control);
            return new Progress(entry.id, entry.supersessionPending || entry.retirement != RetirementProof.NONE,
                    entry.sources != null, entry.assessment != null,
                    entry.execution == null ? Optional.empty() : Optional.of(entry.execution.progress()),
                    Optional.ofNullable(entry.stage));
        }
        private void requireActive(RepositoryReadControl control) {
            if (ended) throw new IllegalStateException("Historical attempt call is closed");
            Objects.requireNonNull(control).check();
        }
        private void authorize(RepositoryReadControl control) {
            requireMutable(control);
            if (entry.work == null) throw new IllegalStateException("Historical sources are not attached");
            entry.work.requireCaller(entry.caller); entry.work.authorize(control);
        }

        private void requireMutable(RepositoryReadControl control) {
            requireActive(control);
            if (entry.supersessionPending || entry.retirement != RetirementProof.NONE)
                throw conflict("Historical attempt is retained for disposal only");
        }

        synchronized RepositoryHistoricalAttemptPreparation.Phase advancePreparation(RepositoryCaller coordinator,
                Map<String, DocumentPublicationCandidate.Mode> modes,
                Map<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject> payloads,
                RepositoryReadControl control) {
            requireMutable(control);
            if (entry.preparation == null) throw conflict("Historical entry already began installed");
            var phase = entry.preparation.advance(coordinator, entry.caller, modes, payloads, control);
            if (phase == RepositoryHistoricalAttemptPreparation.Phase.INSTALLED)
                entry.plan = entry.preparation.installedPlan(); // Same immutable object; preparation owns its byte lease.
            if (phase != RepositoryHistoricalAttemptPreparation.Phase.PROPOSED && entry.predecessorId != null) {
                synchronized (RepositoryInstalledHistoricalAttempts.this) {
                    var predecessor = generations.get(entry.predecessorId);
                    if (predecessor != null && predecessor.retirement == RetirementProof.NONE)
                        predecessor.retirement = RetirementProof.FENCED;
                }
            }
            return phase;
        }

        synchronized boolean reconcileUnactivated(RepositoryCaller coordinator,
                Map<String, DocumentPublicationCandidate.Mode> modes,
                Map<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject> payloads,
                RepositoryReadControl control) {
            requireMutable(control);
            if (entry.preparation == null) throw conflict("Historical entry already began installed");
            if (entry.sources != null || entry.activation != null || entry.execution != null)
                throw conflict("Attached historical sources require activation reconciliation");
            boolean changed = entry.preparation.reconcileUnactivated(coordinator, entry.caller, modes, payloads, control);
            if (changed) entry.plan = null;
            return changed;
        }

        /** Private coordinator view of a confirmed plan; this grants no execution authority. */
        synchronized RepositorySuccessorInstall.Plan installedPlan(RepositoryCaller coordinator, RepositoryReadControl control) {
            requireMutable(control);
            if (entry.initial != null) throw conflict("Initial historical entry has no successor installation");
            RepositoryCoordinatorReservation.require(coordinator, entry.reservation(), control);
            entry.requireSettled();
            if (entry.plan == null) throw conflict("Historical installation is not confirmed");
            return entry.plan;
        }

        /** Acquire each source revision for this reserved generation; failed captures remain ledger-owned. */
        synchronized void captureSources(DocumentReadLedger ledger, RepositoryReadControl control) {
            requireMutable(control);
            entry.requireSettled();
            if (entry.plan == null && entry.initial == null) throw conflict("Historical installation is not confirmed");
            if (entry.sources != null) throw conflict("Historical sources are already attached");
            Objects.requireNonNull(ledger);
            record Source(ai.protomolt.proto.repo.v1.NodeAddress address, UUID revision) {}
            var selected = new LinkedHashSet<Source>();
            for (var member : entry.command.intent().getMembersList()) for (var part : member.getPartsList()) {
                control.check();
                if (part.hasHistoricalReuse()) {
                    var source = part.getHistoricalReuse();
                    selected.add(new Source(source.getSource(), UUID.fromString(source.getRevisionId())));
                }
            }
            var histories = new ArrayList<DocumentReadLedger.PinnedHistory>();
            DocumentHistoricalAssessmentSources sources = null;
            DocumentHistoricalAssessmentSources.Work work = null;
            boolean transferred = false;
            Throwable pending = null;
            try {
                for (var source : selected) {
                    control.check();
                    histories.add(ledger.captureHistorical(entry.caller, source.address(), source.revision()));
                }
                sources = DocumentHistoricalAssessmentSources.open(entry.command, entry.caller, histories, control);
                work = sources.work();
                attachSources(sources, work, control);
                transferred = true;
                sources.close();
            } catch (RuntimeException | Error failure) { pending = failure; throw failure; }
            finally {
                if (!transferred) {
                    var cleanup = new ArrayList<AutoCloseable>();
                    if (work != null) cleanup.add(work);
                    if (sources != null) cleanup.add(sources);
                    cleanup.addAll(histories);
                    // Closed handles remain ledger-owned until bounded SQL release succeeds.
                    try { DocumentHistoricalAssessmentSources.closeAll(cleanup); }
                    catch (RuntimeException | Error failure) {
                        if (pending == null) throw failure;
                        if (pending != failure) pending.addSuppressed(failure);
                    }
                }
            }
        }

        /** Success transfers sources, root Work and exact histories. Failure transfers nothing. */
        synchronized void attachSources(DocumentHistoricalAssessmentSources sources,
                DocumentHistoricalAssessmentSources.Work work, RepositoryReadControl control) {
            requireMutable(control);
            entry.requireSettled();
            if (entry.plan == null && entry.initial == null) throw conflict("Historical installation is not confirmed");
            if (entry.sources != null) throw conflict("Historical sources are already attached");
            Objects.requireNonNull(sources); Objects.requireNonNull(work);
            var histories = work.histories(sources);
            work.requireCaller(entry.caller);
            work.references(entry.command, control::check);
            work.authorize(control);
            var activation = entry.initial == null
                    ? new RepositoryHistoricalSuccessorActivation(tx, budget, entry.plan, entry.retention, sources, drives) : null;
            var initial = entry.initial == null ? null : new RepositoryInitialHistoricalAttempt(tx, budget, entry.retention,
                    entry.initial.modes(), entry.initial.incarnation(), sources, work, entry.scopes, drives, control);
            control.check();
            entry.work = work; entry.histories = histories; entry.activation = activation; entry.initialAttempt = initial;
            entry.sources = sources;
        }

        /** Retains the same handle, including acknowledged START and sticky mutation flags, across calls. */
        synchronized void openExecution(RepositoryCaller coordinator, RepositoryReadControl control) {
            authorize(control);
            requireCoordinator(entry, coordinator, control);
            if (entry.execution == null) entry.execution = entry.initial == null
                    ? entry.activation.openAcceptedExecution(coordinator, entry.caller, entry.work, entry.scopes, entry.parent, control)
                    : entry.initialAttempt.open(coordinator, entry.caller, control);
        }
        private DocumentHistoricalExecution execution(RepositoryReadControl control) {
            authorize(control);
            if (entry.execution == null) throw new IllegalStateException("Historical execution is not attached");
            return entry.execution;
        }
        synchronized DocumentAssessmentStartJournal.Started start(Duration retention, RepositoryReadControl control) {
            return execution(control).start(entry.caller, retention, control);
        }
        synchronized DocumentOperationUploadAdmission.Admission admitUploads(RepositoryReadControl control) {
            return execution(control).admitUploads(entry.caller, control);
        }
        synchronized DocumentUploadCoordinator.Staged stageUploads(DocumentUploadCoordinator coordinator,
                Map<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.codec.PartObject> bodies,
                Map<String, String> attributes, RepositoryReadControl control) {
            return execution(control).stageUploads(entry.caller, coordinator, bodies, attributes, control);
        }
        synchronized void prepareAssessment(DocumentSchemaPolicies.Selection policy,
                Map<String, Map<Integer, ByteString>> fragments, Optional<DocumentSchemaAdmission.Definition> container,
                DocumentPublicationCandidate.Resolver resolver, DocumentRevisionAssembly.Limits limits,
                Instant evaluatedAt, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment != null) throw conflict("Historical assessment is already retained");
            entry.assessment = execution.prepareAssessment(entry.caller, policy, fragments, container, resolver,
                    limits, evaluatedAt, control);
        }
        /** Read and assess under this attached generation; retain one owned fragment snapshot. */
        synchronized void prepareAssessmentFromReader(DocumentSchemaPolicies.Selection policy,
                Map<String, Map<Integer, ByteString>> ordinary, Optional<DocumentSchemaAdmission.Definition> container,
                DocumentPublicationCandidate.Resolver resolver, DocumentRevisionAssembly.Limits limits,
                Instant evaluatedAt, DocumentHistoricalRetainedReader reader, RepositoryReadControl control)
                throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment != null) throw conflict("Historical assessment is already retained");
            entry.assessment = execution.prepareAssessmentFromReader(entry.caller, policy, ordinary, container, resolver,
                    limits, evaluatedAt, entry.sources, reader, control);
        }

        /** Synchronous borrowed access only. Workers must retain their own existing source/scope permits. */
        synchronized <T> T withAssessment(Function<DocumentPublicationAssessment.Historical, T> action,
                RepositoryReadControl control) {
            execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            return Objects.requireNonNull(action).apply(entry.assessment);
        }
        synchronized DocumentAssessmentCreation.Created createAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositorySchemaArtifacts storage,
                DocumentAssessmentStartJournal.Started started, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            if (entry.stage != null) throw conflict("Historical CREATE already acknowledged; continue publication");
            entry.stage = execution.createAssessment(entry.caller, entry.assessment, selections, observation, storage, started, control);
            return entry.stage;
        }
        synchronized Optional<DocumentAssessmentCreation.Created> reconcileAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositoryReadControl control)
                throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.assessment == null) throw new IllegalStateException("Historical assessment is not prepared");
            var verified = execution.reconcileAssessment(entry.caller, entry.assessment, selections, observation, control);
            if (verified.isPresent()) entry.stage = verified.orElseThrow();
            return verified;
        }
        synchronized ai.protomolt.proto.repo.v1.DocumentPublicationResult publishAssessment(
                Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                DocumentAssessmentRuntimeObserver.Observation observation, RepositorySchemaArtifacts storage,
                DocumentPublicationCommit publication, RepositoryReadControl control) throws InvalidProtocolBufferException {
            var execution = execution(control);
            if (entry.stage == null) throw new IllegalStateException("Historical CREATE is not acknowledged");
            return execution.publishAssessment(entry.caller, entry.assessment, selections, observation,
                    storage, publication, entry.stage, control);
        }
        /** Terminal replay authorizes cleanup only, never receipt delivery or replacement execution. */
        synchronized Retirement retireTerminal(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control) throws InterruptedException {
            return retire(coordinator, timeout, control, true);
        }

        /** A lease expiring is not proof that the retained claim can never execute again. */
        synchronized Retirement retireFenced(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control) throws InterruptedException {
            return retire(coordinator, timeout, control, false);
        }

        private Retirement retire(RepositoryCaller coordinator, Duration timeout,
                RepositoryReadControl control, boolean terminal) throws InterruptedException {
            requireActive(control);
            long nanos = checkedNanos(timeout), start = System.nanoTime();
            requireCoordinator(entry, coordinator, control);
            if (entry.retirement == RetirementProof.NONE) {
                var command = entry.command;
                if (terminal) {
                    // Observe the global terminal outcome, which may belong to a later generation.
                    var observed = new DocumentPublicationReplay(tx).observe(entry.caller, command);
                    if (observed.state() != DocumentPublicationReplay.State.COMMITTED
                            && observed.state() != DocumentPublicationReplay.State.TERMINATED) {
                        control.check();
                        return Retirement.NOT_PROVEN;
                    }
                    entry.retirement = RetirementProof.TERMINAL;
                } else {
                    if (entry.initial != null && entry.initialAttempt == null) return Retirement.NOT_PROVEN;
                    var identity = claimIdentity(entry);
                    if (!RepositoryClaimRetirement.fenced(tx, command, entry.preparation == null ? List.of(identity) : entry.preparation.retainedClaims(), control)) return Retirement.NOT_PROVEN;
                    entry.retirement = RetirementProof.FENCED;
                }
            }
            control.check();
            if (!disposeEntry(entry, coordinator, nanos, start, control)) return Retirement.RETAINED;
            control.check();
            synchronized (RepositoryInstalledHistoricalAttempts.this) {
                if (!forget(entry))
                    throw new IllegalStateException("Historical entry lost exclusive retirement ownership");
                entry.releaseBytes();
                ended = true; entry.borrowed = false; active--;
                RepositoryInstalledHistoricalAttempts.this.notifyAll();
            }
            return Retirement.RETIRED;
        }

        @Override public synchronized void close() {
            if (ended) return;
            ended = true;
            synchronized (RepositoryInstalledHistoricalAttempts.this) {
                entry.borrowed = false; active--; RepositoryInstalledHistoricalAttempts.this.notifyAll();
            }
        }
    }

    @Override public synchronized void close() { closed = true; }
    synchronized Drain drain() { return new Drain(active, generations.size()); }
    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (!closed) throw new IllegalStateException("Close historical admission before waiting");
        long remaining = checkedNanos(timeout), start = System.nanoTime();
        while (active != 0) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - start);
        }
        return true;
    }

    /** Local shutdown only. Timeout/failure leaves the entry retained; never fences a shared reader. */
    boolean detachClosed(Duration timeout, Function<RepositoryOperationLedger.Key, RepositoryCaller> authority,
            RepositoryReadControl control) throws InterruptedException {
        long nanos = checkedNanos(timeout), start = System.nanoTime();
        Objects.requireNonNull(authority); Objects.requireNonNull(control).check();
        if (!await(this::awaitIdle, nanos, start, control)) return false;
        final List<Entry> retained;
        synchronized (this) {
            if (detaching) return false;
            detaching = true; retained = List.copyOf(generations.values());
        }
        try {
            boolean complete = true;
            for (var entry : retained) {
                control.check();
                var coordinator = Objects.requireNonNull(authority.apply(entry.key));
                requireCoordinator(entry, coordinator, control);
                if (!disposeEntry(entry, coordinator, nanos, start, control)) { complete = false; continue; }
                control.check();
                entry.releaseBytes();
                synchronized (this) { forget(entry); }
            }
            return complete;
        } finally { synchronized (this) { detaching = false; notifyAll(); } }
    }
    /** No owner-map monitor or SQL transaction spans the local drainage waits. */
    private boolean disposeEntry(Entry entry, RepositoryCaller coordinator, long nanos, long start,
            RepositoryReadControl control) throws InterruptedException {
        entry.scopes.close();
        if (entry.assessment != null) entry.assessment.close();
        if (entry.execution != null) entry.execution.close();
        entry.parent.close();
        if (!await(entry.scopes::awaitIdle, nanos, start, control)) return false;
        if (entry.work != null) entry.work.close();
        if (entry.activation != null) {
            var disposed = entry.activation.disposeCapture(coordinator, remaining(nanos, start), control);
            if (disposed.isEmpty()) return false;
            if (disposed.orElseThrow() == RepositoryHistoricalSuccessorActivation.Disposal.NO_CAPTURE
                    && !releaseUnregistered(entry, nanos, start, control)) return false;
        } else if (entry.initialAttempt != null) {
            var disposed = entry.initialAttempt.dispose(coordinator, remaining(nanos, start), control);
            if (disposed.isEmpty()) return false;
            if (disposed.orElseThrow() == RepositoryHistoricalSuccessorActivation.Disposal.NO_CAPTURE
                    && !releaseUnregistered(entry, nanos, start, control)) return false;
        }
        return true;
    }
    private static void requireCoordinator(Entry entry, RepositoryCaller caller, RepositoryReadControl control) {
        if (entry.initial != null) RepositoryInitialHistoricalAttempt.requireCoordinator(caller, entry.key, control);
        else RepositoryCoordinatorReservation.require(caller, entry.reservation(), control);
    }
    private static RepositoryCoordinatorDrain.Identity claimIdentity(Entry entry) {
        if (entry.initial != null) return entry.initialAttempt.identity();
        var reservation = entry.reservation();
        return new RepositoryCoordinatorDrain.Identity(entry.key, entry.command.sha256(),
                Math.addExact(reservation.predecessor().epoch(), 1), reservation.successorToken(), reservation.successorIncarnation());
    }
    private static boolean releaseUnregistered(Entry entry, long nanos, long start, RepositoryReadControl control)
            throws InterruptedException {
        entry.sources.close();
        entry.histories.forEach(DocumentReadLedger.PinnedHistory::close);
        if (!await(entry.sources::awaitDrained, nanos, start, control)) return false;
        for (var history : entry.histories) {
            control.check();
            if (!await(history::awaitDrained, nanos, start, control)) return false;
            history.release();
        }
        control.check();
        return entry.histories.stream().allMatch(DocumentReadLedger.PinnedHistory::isReleased);
    }
    private static long checkedNanos(Duration timeout) {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative historical shutdown wait");
        return timeout.toNanos();
    }
    @FunctionalInterface private interface DrainWait { boolean await(Duration timeout) throws InterruptedException; }
    private static boolean await(DrainWait wait, long nanos, long start, RepositoryReadControl control)
            throws InterruptedException {
        while (true) {
            control.check();
            long slice = Math.min(remaining(nanos, start).toNanos(),
                    Math.min(100_000_000L, Math.max(0, control.remainingNanos())));
            boolean drained = wait.await(Duration.ofNanos(slice));
            control.check();
            if (drained) return true;
            if (System.nanoTime() - start >= nanos) return false;
        }
    }
    private static ByteString digest(ByteString bytes) {
        return ByteString.copyFrom(DocumentPublicationPreparationJournal.digest(bytes));
    }
    private static Duration remaining(long nanos, long start) {
        return Duration.ofNanos(Math.max(0, nanos - (System.nanoTime() - start)));
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Historical attempt admission is closed");
    }
    private static RepositoryException conflict(String message) {
        return new RepositoryException(RepositoryException.Code.CONFLICT, message);
    }
}
