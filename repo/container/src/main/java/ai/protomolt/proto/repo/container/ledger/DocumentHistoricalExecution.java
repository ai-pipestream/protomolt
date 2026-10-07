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
    private final Object assessmentIdentity = new Object();
    private DocumentAssessmentStartJournal.Started acknowledgedStart;
    private boolean assessmentCreateAttempted;
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
        var result = mutate(caller, control, em -> DocumentAssessmentStartJournal.startOrLoadHistoricalOwned(
                em, owner, record.command(), retention, control));
        // Only a positively acknowledged INSERT grants this handle CREATE authority.
        // Loading coordinates after an uncertain acknowledgement is reconciliation-only.
        if (result.inserted()) acknowledgedStart = result.started();
        else if (acknowledgedStart != null && !acknowledgedStart.equals(result.started()))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical assessment start changed");
        return result.started();
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

    /** Owns a child of this exact source/registration scope through all assessment work and cleanup. */
    synchronized DocumentPublicationAssessment.Historical prepareAssessment(RepositoryCaller caller,
            DocumentSchemaPolicies.Selection policy,
            Map<String, Map<Integer, com.google.protobuf.ByteString>> fragments,
            Optional<ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Definition> container,
            DocumentPublicationCandidate.Resolver resolver,
            ai.protomolt.proto.repo.codec.DocumentRevisionAssembly.Limits limits, java.time.Instant evaluatedAt,
            RepositoryReadControl control) throws com.google.protobuf.InvalidProtocolBufferException {
        mutate(caller, control, em -> null);
        var child = registration.forkAccepted();
        var assessment = DocumentPublicationAssessment.prepareHistoricalAccepted(record.command(), policy, modes,
                fragments, container, resolver, budget, limits, evaluatedAt, work, child, assessmentIdentity, control);
        try {
            // Resolution can outlast credential, claim or source changes; authorize findings at delivery.
            mutate(caller, control, em -> null);
            return assessment;
        } catch (RuntimeException | Error failure) {
            try { assessment.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Private create-only staging; a failed SQL acknowledgement requires reconciliation, never blind retry. */
    synchronized DocumentAssessmentCreation.Created createAssessment(RepositoryCaller caller,
            DocumentPublicationAssessment.Historical assessment,
            Map<String, DocumentSelectedAttemptLedger.Selected> selections,
            DocumentAssessmentRuntimeObserver.Observation observation, RepositorySchemaArtifacts storage,
            DocumentAssessmentStartJournal.Started started, RepositoryReadControl control)
            throws com.google.protobuf.InvalidProtocolBufferException {
        mutate(caller, control, em -> null);
        if (assessmentCreateAttempted)
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Historical assessment CREATE requires reconciliation before another attempt");
        Objects.requireNonNull(started);
        var selected = Map.copyOf(selections);
        return assessment.withRetainedEvidence(assessmentIdentity, caller, work, owner, record.command(), modes,
                observation, control, evidence -> createObserved(caller, selected, evidence, storage, started, control));
    }

    private DocumentAssessmentCreation.Created createObserved(RepositoryCaller caller,
            Map<String, DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentEvidence evidence,
            RepositorySchemaArtifacts storage, DocumentAssessmentStartJournal.Started started, RepositoryReadControl control) {
        if (!started.equals(acknowledgedStart))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Historical assessment start requires reconciliation on this handle");
        evidence.requireOwner(owner, control::check);
        var command = record.command();
        if (!evidence.command(control::check).canonical().equals(command.canonical())
                || !evidence.modes(control::check).equals(modes))
            throw new IllegalArgumentException("Observed assessment differs from registered identity");
        var references = work.references(command, control::check);
        var plan = prepared.plan();
        var authorization = DocumentAdmissionAuthorization.prepare(plan, references);
        var creation = DocumentCreationAuthorization.prepare(plan, drives, caller);
        var reuse = DocumentReuseAdmission.prepare(plan);
        var slotPlan = DocumentAssessmentSlots.prepare(command, references, control::check);
        var policy = evidence.policy(control::check);
        var artifacts = evidence.artifacts(control::check);
        var roots = evidence.roots(control::check);
        var manifest = evidence.manifestBytes(control::check);
        var manifestSha = evidence.manifestSha256(control::check);
        int count = command.intent().getMembersList().stream().mapToInt(member ->
                (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum();
        int largestRoot = roots.stream().mapToInt(root -> root.bytes().size()).max().orElse(0);
        // Hold current authority through the artifact claim transaction, including after evidence preparation.
        storage.stageAuthorized(owner, command, List.copyOf(artifacts.values()), control::check, em -> {
            lockRegistration(em);
            DocumentAssessmentStartJournal.requireCreation(em, owner, command, started.assessment(), started.retainUntil());
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            DocumentSchemaPolicies.lockCurrent(em, policy, control::check);
            DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
            DocumentPublicationModesJournal.requireBoundModes(em, owner.key(), command, owner.generation(), encodedModes);
            evidence.check(control::check);
        });
        try (var scratch = budget.reserve(2L * (manifest.size() + largestRoot))) {
            var writes = new DocumentAssessmentCreationWrites.Prepared(command, plan, selected, reuse, slotPlan,
                    true, manifest.toByteArray(), manifestSha, count, artifacts, roots);
            // Sticky before SQL: even a lost acknowledgement cannot lead this handle to CREATE twice.
            assessmentCreateAttempted = true;
            var result = tx.inTransaction(em -> {
                lockRegistration(em);
                DocumentAssessmentStartJournal.requireCreation(em, owner, command, started.assessment(), started.retainUntil());
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                DocumentSchemaPolicies.lockCurrent(em, policy, control::check);
                DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
                DocumentPublicationModesJournal.requireBoundModes(em, owner.key(), command, owner.generation(), encodedModes);
                for (var placement : record.placements().values().stream()
                        .sorted(Comparator.comparing(value -> value.drive().id())).toList()) {
                    placement.drive().lock(em, drives);
                    if (!ManagedBackendLedger.find(em, placement.generation()).filter(placement.profile()::equals).isPresent())
                        throw new IllegalArgumentException("Historical execution placement differs from registered backend");
                }
                // One complete origin set: never acquire the historical subset before fresh attempts.
                var physical = DocumentCommitParts.bindHistoricalAssessment(em, owner, plan, selected, reuse, control::check);
                var slots = DocumentAssessmentSlots.bind(em, slotPlan, physical.physical(), physical.locks(), control::check);
                DocumentPreparationSourcePins.requireActiveInitial(em, record, pins, owner.executionClaim().orElseThrow(),
                        capture.owner().incarnation(), control::check);
                evidence.check(control::check);
                return DocumentAssessmentCreationWrites.writeBound(em, owner, writes, evidence,
                        started.assessment(), started.retainUntil(), budget, control::check, slots);
            });
            work.authorize(control);
            return result;
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
            lockRegistration(em);
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

    private void lockRegistration(jakarta.persistence.EntityManager em) {
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
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        DocumentHistoricalAssessmentSources.closeAll(List.of(work, retained, registration));
    }
}
