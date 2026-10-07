package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real provider publication through a retained initial owner, without a successor installation. */
final class HistoricalInitialOwnerProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement, DocumentPublishedRevision revision,
            Map<Integer, ByteString> fragments, PayloadBudget budget, DocumentAssessmentRuntimeObserver.Observation observation,
            HistoricalCreateCommitFault fault) throws Exception {
        require(!caller.processAuthority(), "initial owner executes as a credential-bound scoped caller");
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), Duration.ofMinutes(2), 0);
        var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
        var freshDefinition = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
        long before = budget.reservedBytes();
        var attempts = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        DocumentReadLedger.PinnedHistory history = null;
        DocumentHistoricalAssessmentSources sources = null;
        DocumentHistoricalAssessmentSources.Work accepted = null;
        boolean transferred = false;
        Throwable primary = null;
        try {
            DocumentAssessmentStartJournal.Started started;
            DocumentOperationUploadAdmission.Admission admitted;
            try (var request = attempts.beginInitial(caller, record, modes, UUID.randomUUID())) {
                require(count(tx, key, "repository_execution_claims") == 0, "slot reservation has no claim effects");
                history = reads.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
                sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                accepted = sources.work();
                request.attachSources(sources, accepted, RepositoryReadControl.NONE); transferred = true;
                sources.close();
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                admitted = request.admitUploads(RepositoryReadControl.NONE);
                started = request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE);
                request.prepareAssessment(policy, Map.of("a", fragments), Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                        (member, occurrence) -> freshDefinition,
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), RepositoryReadControl.NONE);
            }
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 1)), "initial entry survives first call");
            var owner = tx.inTransaction(em -> {
                var token = (UUID) em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=1")
                        .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult();
                var claim = RepositoryExecutionClaimLedger.lockLive(em, key, command.sha256(), 1, token);
                return RepositoryOperationLedger.lockLiveOwner(em, key, 1, record.seeds().ownerNonce(), Optional.of(claim));
            });
            require(admitted.attempts().size() == 1, "mixed initial entry admits one upload");
            var upload = admitted.attempts().getFirst();
            require(upload.id().equals(record.seeds().attempts().get("a")) && upload.token().equals(record.seeds().uploadTokens().get("a")),
                    "initial upload retains original identities");
            var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, upload.id(), upload.token());
            var selections = Map.of("a", selected);
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                require(request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE).equals(started), "initial START survives separate calls");
                var physical = request.withAssessment(assessment -> assessment.preparePhysical(record.placements(),
                        record.seeds().attempts(), record.lease(), record.seeds().uploadTokens(), RepositoryReadControl.NONE), RepositoryReadControl.NONE);
                var member = physical.plan().members().getFirst();
                var measured = member.attempt().orElseThrow().uploads().stream().map(part -> {
                    var object = part.object();
                    var actual = DocumentPartTransfer.upload(provider.store(), placement.drive().namespace(), object,
                            fragments.get(part.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), actual.version(), actual.etag());
                }).toList();
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
                if (fault == null) request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                else {
                    fault.arm(started.assessment());
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Initial CREATE did not lose its reply");
                    } catch (RuntimeException failure) { fault.requireFailure(failure); }
                }
            }
            if (fault != null) {
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    try {
                        request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        throw new AssertionError("Initial uncertain CREATE repeated");
                    } catch (RepositoryException refused) {
                        require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION, "uncertain CREATE stays sticky");
                    }
                    var reconciled = request.reconcileAssessment(selections, observation, RepositoryReadControl.NONE).orElseThrow();
                    require(reconciled.assessment().equals(started.assessment()) && reconciled.retainUntil().equals(started.retainUntil()),
                            "initial reconciliation keeps exact START coordinates");
                }
            }
            DocumentPublicationResult result;
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                result = request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                        new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
            }
            require(result.getOwnerGeneration() == 1, "receipt belongs to initial generation");
            HistoricalClaimedMixedPublicationProbe.verify(tx, provider, caller, command, owner, selections, fragments, result);
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                try {
                    request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                    throw new AssertionError("Initial publication repeated across calls");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && refused.getMessage().contains("reconciliation"), "initial publication remains sticky");
                }
            }
            require(count(tx, key, "repository_publication_assessment_starts") == 1
                    && count(tx, key, "document_assessment_owners") == 1 && count(tx, key, "document_revision_commits") == 1,
                    "initial command has one START, CREATE and publication");
            require(count(tx, key, "repository_coordinator_expirations") == 0 && count(tx, key, "repository_successor_installs") == 0,
                    "initial publication never fabricates a successor reservation or installation");
            try (var worker = accepted.fork()) {
                try (var request = attempts.resume(caller, command).orElseThrow()) {
                    require(request.retireTerminal(coordinator, Duration.ZERO, RepositoryReadControl.NONE)
                            == RepositoryInstalledHistoricalAttempts.Retirement.RETAINED, "terminal entry waits for actual worker");
                }
                require(!history.isReleased() && count(tx, key, "repository_preparation_capture_drains") == 0,
                        "held worker prevents capture drain evidence");
            }
            try (var request = attempts.resume(caller, command).orElseThrow()) {
                require(request.retireTerminal(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                        == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "initial terminal generation retires after worker drainage");
            }
            require(history.isReleased() && count(tx, key, "repository_preparation_capture_drains") == 1,
                    "exact initial capture drains once");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                    "exact initial receipt replays after retirement");
            require(budget.reservedBytes() == before, "initial retirement returns all retained memory");
        } catch (Exception | Error failure) { primary = failure; throw failure; }
        finally {
            try {
                attempts.close();
                require(attempts.detachClosed(Duration.ofSeconds(1), ignored -> coordinator, RepositoryReadControl.NONE), "initial owner shutdown drains");
                if (!transferred) {
                    if (accepted != null) accepted.close();
                    if (sources != null) sources.close();
                    if (history != null) { history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "untransferred capture drains"); history.release(); }
                }
                require(budget.reservedBytes() == before, "initial probe returns memory on every exit path");
            } catch (Exception | Error cleanup) {
                if (primary == null) throw cleanup;
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
        }
        System.out.println(fault == null ? "SCOPED_INITIAL_HISTORICAL_PUBLICATION_OK" : "SCOPED_INITIAL_HISTORICAL_CREATE_RECONCILED_OK");
    }
    private static long count(Tx tx, RepositoryOperationLedger.Key key, String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
