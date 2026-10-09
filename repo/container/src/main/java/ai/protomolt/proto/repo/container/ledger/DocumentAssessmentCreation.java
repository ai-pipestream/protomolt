package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Internal create-only transaction for observed evidence. A duplicate is a
 * conflict, never implicit adoption. The host must reconcile uncertain commits
 * separately; no terminal decision, replay read or public staging API is enabled.
 */
final class DocumentAssessmentCreation {
    record Created(UUID assessment, String manifestSha256, Instant retainUntil) {}
    private final Tx tx;
    private final DriveLedger drives;
    DocumentAssessmentCreation(Tx tx, DriveLedger drives) {
        this.tx = Objects.requireNonNull(tx); this.drives = Objects.requireNonNull(drives);
    }

    /** Schema catalog claims must already exist for this exact operation generation. */
    Created create(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentSelectedAttemptLedger.Selected> selections,
            DocumentAssessmentEvidence evidence, UUID assessment, Instant retainUntil, PayloadBudget scratch, Runnable control) {
        prepared.plan().command().requireExecutionSupported();
        return createInternal(caller, owner, prepared, selections, evidence, assessment, retainUntil, scratch, control, null);
    }

    Created createHistorical(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentSelectedAttemptLedger.Selected> selections,
            DocumentAssessmentEvidence evidence, UUID assessment, Instant retainUntil, PayloadBudget scratch, Runnable control,
            java.util.List<DocumentHistoricalReferenceAdmission.Prepared> historical) {
        historical = DocumentHistoricalReferenceAdmission.requireComplete(prepared.plan().command(), historical, control);
        if (historical.isEmpty() || !prepared.plan().historical().equals(historical) || owner.executionClaim().isPresent())
            throw new IllegalArgumentException("Historical CREATE requires the owner's exact unclaimed source preparations");
        return createInternal(caller, owner, prepared, selections, evidence, assessment, retainUntil, scratch, control, historical);
    }

    private Created createInternal(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<String,DocumentSelectedAttemptLedger.Selected> selections,
            DocumentAssessmentEvidence evidence, UUID assessment, Instant retainUntil, PayloadBudget scratch, Runnable control,
            java.util.List<DocumentHistoricalReferenceAdmission.Prepared> historical) {
        Objects.requireNonNull(assessment); Objects.requireNonNull(retainUntil); Objects.requireNonNull(scratch);
        if (retainUntil.getNano() % 1000 != 0) throw new IllegalArgumentException("Assessment deadline requires exact microsecond precision");
        evidence.requireOwner(owner, control);
        var command = evidence.command(control);
        var plan = prepared.plan();
        if (!command.operationId().equals(plan.command().operationId()) || !command.canonical().equals(plan.command().canonical()))
            throw new IllegalArgumentException("Assessment and physical plan differ from the same canonical command");
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        var selected = Map.copyOf(selections);
        var authorization = historical == null ? DocumentAdmissionAuthorization.prepare(plan)
                : DocumentAdmissionAuthorization.prepare(plan, historical);
        var creation = DocumentCreationAuthorization.prepare(plan, drives, caller);
        if (owner.executionClaim().isPresent()) {
            // Do not hold a SQL connection across private journal loading/decoding.
            Runnable authorize = () -> tx.inTransaction(em -> {
                control.run();
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
                return null;
            });
            authorize.run();
            try {
                new DocumentPublicationModesJournal(tx, scratch).requireObservedModes(caller, owner, command, evidence.modes(control),
                        new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                            @Override public boolean isCancelled() { return false; }
                            @Override public long remainingNanos() { return Long.MAX_VALUE; }
                            @Override public void check() { control.run(); }
                        });
            } catch (RuntimeException failure) {
                // Current denial takes precedence over a private journal failure.
                authorize.run(); throw failure;
            }
        }
        var reuse = DocumentReuseAdmission.prepare(plan);
        var slotPlan = historical == null ? DocumentAssessmentSlots.prepare(command, control)
                : DocumentAssessmentSlots.prepare(command, historical, control);
        var placements = plan.members().stream().map(DocumentUploadPlan.Member::placement).distinct()
                .sorted(Comparator.comparing(p -> p.drive().id())).toList();
        var policy = evidence.policy(control);
        var artifacts = evidence.artifacts(control);
        var roots = evidence.roots(control);
        var manifest = evidence.manifestBytes(control);
        var manifestSha = evidence.manifestSha256(control);
        int count = command.intent().getMembersList().stream().mapToInt(member ->
                (int) member.getPartsList().stream().filter(part -> !part.hasEmpty()).count()).sum();
        int largestRoot = roots.stream().mapToInt(root -> root.bytes().size()).max().orElse(0);
        // Account for the explicit byte-array copy and JDBC payload retention.
        // Roots execute individually so JDBC never retains the complete root set.
        try (var reservation = scratch.reserve(2L * (manifest.size() + largestRoot))) {
            byte[] manifestBytes = manifest.toByteArray();
            var writes = new DocumentAssessmentCreationWrites.Prepared(command, plan, selected, reuse, slotPlan,
                    historical != null, manifestBytes, manifestSha, count, artifacts, roots);
            return tx.inTransaction(em -> {
                evidence.check(control);
                DocumentAssessmentStartJournal.requireCreation(em, owner, command, assessment, retainUntil);
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                DocumentSchemaPolicies.lockCurrent(em, policy, control);
                DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization, creation);
                // Scoped creation already checked these placements before taking authority locks.
                for (var placement : creation == null ? placements : java.util.List.<DocumentUploadPlan.Placement>of()) {
                    evidence.check(control);
                    placement.drive().lock(em, drives);
                    if (!ManagedBackendLedger.find(em, placement.generation()).orElseThrow(
                            () -> new IllegalArgumentException("Assessment backend profile is missing")).equals(placement.profile()))
                        throw new IllegalArgumentException("Assessment backend profile changed");
                }
                return DocumentAssessmentCreationWrites.write(em, owner, writes, evidence, assessment, retainUntil,
                        scratch, control);
            });
        }
    }
}
