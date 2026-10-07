package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.*;

/** Initial registered historical source owner. No public session or publication capability. */
final class DocumentHistoricalExecution implements AutoCloseable {
    private final DocumentHistoricalAssessmentSources.Work work;
    private final PayloadBudget.Lease retained;
    private final RepositoryOperationLedger.Owner owner;
    private final DocumentOperationUploadAdmission.Prepared prepared;
    private final Map<String, DocumentPublicationCandidate.Mode> modes;
    private final DocumentPreparationSourcePins.Prepared pins;
    private final DocumentPublicationScopeCalls.Call registration;
    private final Tx tx;
    private final PayloadBudget budget;
    private final DriveLedger drives;
    private final DocumentPublicationPreparationRecord record;
    private final DocumentPreparationCaptureDrain.Identity capture;
    private final byte[] preparationDigest;
    private final String encodedModes;
    private boolean closed;

    private DocumentHistoricalExecution(DocumentHistoricalAssessmentSources.Work work, PayloadBudget.Lease retained,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared prepared,
            Map<String, DocumentPublicationCandidate.Mode> modes, DocumentPreparationSourcePins.Prepared pins,
            DocumentPublicationScopeCalls.Call registration, Tx tx, PayloadBudget budget, DriveLedger drives,
            DocumentPublicationPreparationRecord record, DocumentPreparationCaptureDrain.Identity capture, byte[] preparationDigest) {
        this.work = work; this.retained = retained; this.owner = owner; this.prepared = prepared;
        this.modes = Map.copyOf(modes); this.pins = pins;
        this.registration = registration;
        this.tx = tx; this.budget = budget; this.drives = drives; this.record = record; this.capture = capture;
        this.preparationDigest = preparationDigest.clone();
        this.encodedModes = DocumentPublicationModesJournal.encode(record.command(), modes);
    }

    static DocumentHistoricalExecution open(Tx tx, PayloadBudget budget,
            DocumentPublicationRegistration.JournalAccess access, DocumentPublicationPreparationRecord record,
            DocumentHistoricalAssessmentSources sources, DocumentPreparationCaptureDrain.Capture capture,
            RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            Map<String, DocumentPublicationCandidate.Mode> modes, DriveLedger drives, RepositoryReadControl control,
            DocumentPublicationScopeCalls.Call registration) {
        Objects.requireNonNull(capture); Objects.requireNonNull(control).check();
        access.requireOwner(caller, owner, record.command(), control);
        if (record.predecessorGeneration() != 0 || owner.generation() != 1)
            throw new IllegalArgumentException("Historical execution requires the initial registered owner");
        var claim = owner.executionClaim().orElseThrow();
        var identity = capture.identity();
        if (claim.epoch() != 1 || identity.generation() != 0 || !identity.owner().key().equals(owner.key())
                || !identity.owner().commandSha256().equals(record.command().sha256())
                || identity.owner().epoch() != claim.epoch() || !identity.owner().token().equals(claim.token()))
            throw new IllegalArgumentException("Historical execution capture differs from registered owner");
        var work = sources.work();
        PayloadBudget.Lease retained = null;
        try {
            work.requireCaller(caller); work.authorize(control);
            retained = budget.reserve(DocumentPreparationSourcePins.MAX_BYTES + DocumentPublicationModesJournal.MAX_BYTES);
            var references = work.references(record.command(), control::check);
            var prepared = DocumentOperationUploadAdmission.prepareHistorical(record.command(), record.placements(),
                    record.seeds().attempts(), record.lease(), record.seeds().uploadTokens(), references, control::check);
            var plan = prepared.plan();
            var authorization = DocumentAdmissionAuthorization.prepare(plan, references);
            var creation = DocumentCreationAuthorization.prepare(plan, drives, caller);
            // Current caller authority precedes private journal loading; repeat under the final fence.
            tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
                return null;
            });
            var fixed = new DocumentPublicationModesJournal(tx, budget)
                    .loadOwned(access, caller, claim, 0, control).orElseThrow(() ->
                            new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Fixed publication modes are absent"));
            if (!fixed.equals(modes)) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Historical execution modes differ from registration");
            var encodedModes = DocumentPublicationModesJournal.encode(record.command(), fixed);
            var pins = DocumentPreparationSourcePins.prepare(record.command(), references, control::check);
            if (!identity.pinsSha256().equals(HexFormat.of().formatHex(pins.digest())))
                throw new IllegalArgumentException("Historical execution sources differ from registered capture");
            var reuse = DocumentReuseAdmission.prepare(plan);
            var objects = plan.members().stream().flatMap(member -> member.intent().getPartsList().stream())
                    .filter(part -> part.hasReuse() || part.hasHistoricalReuse())
                    .map(part -> UUID.fromString(part.hasReuse() ? part.getReuse().getObject().getObjectId()
                            : part.getHistoricalReuse().getObject().getObjectId()))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            final byte[] digest;
            try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
                var encoded = DocumentPublicationPreparationCodec.encode(record);
                digest = DocumentPublicationPreparationJournal.digest(encoded);
                tx.inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, claim);
                    var rows = em.createNativeQuery("""
                            SELECT preparation_bytes=:bytes AND preparation_sha256=:digest AND owner_nonce=:nonce
                            FROM repository_publication_preparations
                            WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=0 FOR UPDATE
                            """).setParameter("bytes", encoded.toByteArray()).setParameter("digest", digest)
                            .setParameter("nonce", owner.token()).setParameter("a", owner.key().account())
                            .setParameter("p", owner.key().principal()).setParameter("o", owner.key().operationId()).getResultList();
                    if (rows.size() != 1 || !Boolean.TRUE.equals(rows.getFirst()))
                        throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical execution preparation differs from registration");
                    em.createNativeQuery("""
                            SELECT predecessor_generation FROM repository_preparation_history_sets
                            WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=0 FOR UPDATE
                            """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                            .setParameter("o", owner.key().operationId()).getResultList();
                    RepositoryOperationLedger.fenceLiveOwner(em, owner);
                    RepositoryOperationLedger.requireCommand(em, owner.key(), record.command());
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
                    DocumentPublicationModesJournal.requireBoundModes(em, owner.key(), record.command(), owner.generation(), encodedModes);
                    if (DocumentPreparationHistoryRoots.coverage(em, record, digest) != DocumentPreparationHistoryRoots.Coverage.EXACT)
                        throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical execution retention is unknown");
                    for (var placement : record.placements().values().stream()
                            .sorted(Comparator.comparing(value -> value.drive().id())).toList()) {
                        placement.drive().lock(em, drives);
                        if (!ManagedBackendLedger.find(em, placement.generation()).filter(placement.profile()::equals).isPresent())
                            throw new IllegalArgumentException("Historical execution placement differs from registered backend");
                    }
                    DocumentReuseAdmission.requireBoundSources(em, reuse);
                    var origins = DocumentPublicationLocks.lockIndependentOrigins(em, authorization.destinations(), objects, Set.of());
                    DocumentPublicationLocks.lockIndependentRetention(em, origins);
                    for (var reference : references)
                        DocumentHistoricalReferenceAdmission.requireBoundSources(em, reference, origins, control);
                    DocumentPreparationSourcePins.requireInitial(em, record, pins, claim, identity.owner().incarnation(), control::check);
                    control.check(); return null;
                });
            }
            work.authorize(control);
            return new DocumentHistoricalExecution(work, retained, owner, prepared, fixed, pins, registration,
                    tx, budget, drives, record, identity, digest);
        } catch (RuntimeException | Error failure) {
            var cleanup = new ArrayList<AutoCloseable>(); cleanup.add(work);
            if (retained != null) cleanup.add(retained);
            try { DocumentHistoricalAssessmentSources.closeAll(cleanup); }
            catch (RuntimeException | Error failed) { if (failed != failure) failure.addSuppressed(failed); }
            throw failure;
        }
    }

    /** Synchronous accepted operation; close cannot release either lifetime while SQL is running. */
    synchronized DocumentAssessmentStartJournal.Started start(RepositoryCaller caller, java.time.Duration retention,
            RepositoryReadControl control) {
        return mutate(caller, control, em -> DocumentAssessmentStartJournal.startOrLoadHistorical(
                em, owner, record.command(), retention, control));
    }

    /** SQL selection admission only; provider execution must retain its own accepted lifetime. */
    synchronized DocumentOperationUploadAdmission.Admission admitUploads(RepositoryCaller caller, RepositoryReadControl control) {
        if (closed) throw new IllegalStateException("Historical execution is closed");
        work.requireCaller(caller); work.authorize(control);
        try (var scratch = budget.reserve(DocumentOperationUploadAdmission.initialEncodingBytes(prepared))) {
            var uploads = DocumentOperationUploadAdmission.encodeInitial(prepared);
            control.check();
            return mutate(caller, control, em -> DocumentOperationUploadAdmission.admitHistoricalInitial(em, owner, prepared, uploads));
        }
    }

    private <T> T mutate(RepositoryCaller caller, RepositoryReadControl control,
            java.util.function.Function<jakarta.persistence.EntityManager, T> mutation) {
        if (closed) throw new IllegalStateException("Historical execution is closed");
        work.requireCaller(caller); work.authorize(control);
        var command = record.command();
        var references = work.references(command, control::check);
        var plan = prepared.plan();
        var authorization = DocumentAdmissionAuthorization.prepare(plan, references);
        var creation = DocumentCreationAuthorization.prepare(plan, drives, caller);
        var reuse = DocumentReuseAdmission.prepare(plan);
        var objects = plan.members().stream().flatMap(member -> member.intent().getPartsList().stream())
                .filter(part -> part.hasReuse() || part.hasHistoricalReuse())
                .map(part -> UUID.fromString(part.hasReuse() ? part.getReuse().getObject().getObjectId()
                        : part.getHistoricalReuse().getObject().getObjectId()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        var result = tx.inTransaction(em -> {
            var claim = owner.executionClaim().orElseThrow();
            RepositoryExecutionClaimLedger.lockLive(em, claim);
            var rows = em.createNativeQuery("""
                    SELECT p.owner_nonce=:nonce AND p.preparation_sha256=:digest
                      AND h.preparation_sha256=p.preparation_sha256 AND h.command_sha256=p.command_sha256 AND h.sealed
                    FROM repository_publication_preparations p JOIN repository_preparation_history_sets h
                      USING(account_id,principal,operation_id,predecessor_generation)
                    WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o AND p.predecessor_generation=0
                    FOR UPDATE OF p,h
                    """).setParameter("nonce", owner.token()).setParameter("digest", preparationDigest)
                    .setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).getResultList();
            if (rows.size() != 1 || !Boolean.TRUE.equals(rows.getFirst()))
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical execution preparation binding changed");
            em.createNativeQuery("""
                    SELECT owner_nonce FROM repository_publication_modes
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=0 FOR UPDATE
                    """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).getSingleResult();
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
            DocumentPublicationModesJournal.requireBoundModes(em, owner.key(), command, owner.generation(), encodedModes);
            for (var placement : record.placements().values().stream()
                    .sorted(Comparator.comparing(value -> value.drive().id())).toList()) {
                placement.drive().lock(em, drives);
                if (!ManagedBackendLedger.find(em, placement.generation()).filter(placement.profile()::equals).isPresent())
                    throw new IllegalArgumentException("Historical execution placement differs from registered backend");
            }
            DocumentReuseAdmission.requireBoundSources(em, reuse);
            var origins = DocumentPublicationLocks.lockIndependentOrigins(em, authorization.destinations(), objects, Set.of());
            DocumentPublicationLocks.lockIndependentRetention(em, origins);
            for (var reference : references) DocumentHistoricalReferenceAdmission.requireBoundSources(em, reference, origins, control);
            DocumentPreparationSourcePins.requireActiveInitial(em, record, pins, claim, capture.owner().incarnation(), control::check);
            var value = mutation.apply(em);
            control.check(); return value;
        });
        work.authorize(control);
        return result;
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        DocumentHistoricalAssessmentSources.closeAll(List.of(work, retained, registration));
    }
}
