package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real expiry, fresh source capture and provider reads, followed by successor CREATE. */
final class HistoricalSuccessorCreateProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller,
            DocumentPublicationPreparationRecord original, RepositoryOperationLedger.Owner oldOwner,
            DocumentHistoricalExecution oldExecution, DocumentAssessmentStartJournal.Started oldStart,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation, RepositoryCoordinatorDrain.Identity oldIdentity) throws Exception {
        var command = original.command();
        var limits = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
        try (var oldAssessment = oldExecution.prepareAssessment(caller, policy, Map.of("a", fragments), Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Historical schemas must be retained"); },
                limits, Instant.now(), RepositoryReadControl.NONE)) {
            tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:op
                    """).setParameter("op", command.operationId()).getSingleResult());
            var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(oldIdentity, UUID.randomUUID(),
                    UUID.randomUUID(), Duration.ofMinutes(2), new RepositoryCoordinatorReservation.OwnerIdentity(
                            oldOwner.generation(), oldOwner.token()));
            RepositoryCoordinatorExpiration.reserve(tx, caller, reservation, RepositoryReadControl.NONE);
            var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
            var plan = RepositorySuccessorInstall.prepare(reservation, original, Duration.ofMinutes(2), modes);
            RepositorySuccessorInstall.install(tx, budget, caller, plan, RepositoryReadControl.NONE);
            var selector = command.intent().getMembers(0).getPartsList().stream().filter(p -> p.hasHistoricalReuse())
                    .findFirst().orElseThrow().getHistoricalReuse();
            var reads = new DocumentReadLedger(tx, UUID.randomUUID());
            var history = reads.captureHistorical(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()));
            var scopes = new DocumentPublicationScopeCalls();
            try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                 var accepted = sources.work()) {
                // Independently reread the exact immutable provider versions selected by the new capture.
                var fresh = new HashMap<Integer, ByteString>();
                try (var use = history.use()) {
                    for (var entry : use.plan().entries()) {
                        var part = entry.part();
                        var bytes = ByteString.copyFrom(provider.store().getBounded(part.binding().namespace(),
                                part.part().key(), part.part().providerVersion(), Math.toIntExact(part.part().size())).data());
                        for (int ordinal = 0; ordinal < command.intent().getMembers(0).getPartsCount(); ordinal++) {
                            var selected = command.intent().getMembers(0).getParts(ordinal).getHistoricalReuse();
                            if (selected.getObject().getObjectId().equals(entry.objectId().toString())) {
                                require(bytes.equals(fragments.get(ordinal)), "fresh capture rereads original bytes");
                                fresh.put(ordinal, bytes);
                            }
                        }
                    }
                }
                require(fresh.size() == fragments.size(), "all selected fragments reread from provider");
                var activation = new RepositoryHistoricalSuccessorActivation(tx, budget, plan, original, sources, new DriveLedger(tx));
                try (var execution = activation.openExecution(caller, caller, accepted, scopes, RepositoryReadControl.NONE)) {
                    try {
                        oldExecution.createAssessment(caller, oldAssessment, Map.of(), observation,
                                new RepositorySchemaArtifacts(tx), oldStart, RepositoryReadControl.NONE);
                        throw new AssertionError("Expired predecessor performed late CREATE");
                    } catch (RepositoryExecutionClaimLedger.Fenced expected) { /* Exact predecessor claim refused. */ }
                    var admitted = execution.admitUploads(caller, RepositoryReadControl.NONE);
                    require(admitted.attempts().isEmpty(), "reuse-only successor records selections without new uploads");
                    var started = execution.start(caller, Duration.ofMinutes(1), RepositoryReadControl.NONE);
                    require(!started.assessment().equals(oldStart.assessment()), "successor owns a distinct start identity");
                    try (var assessment = execution.prepareAssessment(caller, policy, Map.of("a", fresh), Optional.empty(),
                            (member, occurrence) -> { throw new AssertionError("Successor must use retained schemas"); },
                            limits, Instant.now(), RepositoryReadControl.NONE)) {
                        var created = execution.createAssessment(caller, assessment, Map.of(), observation,
                                new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        var owner = tx.inTransaction(em -> {
                            var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), command.sha256(),
                                    reservation.predecessor().epoch()+1, reservation.successorToken());
                            return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration()+1,
                                    plan.next().seeds().ownerNonce(), Optional.of(claim));
                        });
                        var found = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
                        require(found.stage().equals(created), "successor stage has exact discovered identity");
                        tx.inTransaction(em -> {
                            require(DocumentAssessmentReconciliation.verifyRetainedInTransaction(em, caller, owner,
                                    command, found.selections(), created, budget, () -> {}).orElseThrow().equals(created),
                                    "same-transaction retained verification matches CREATE");
                            try {
                                tx.inTransaction(contender -> {
                                    contender.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE NOWAIT")
                                            .setParameter("id", created.assessment()).getSingleResult();
                                });
                                throw new AssertionError("Retained assessment lock escaped the caller transaction");
                            } catch (RuntimeException locked) {
                                Throwable cause = locked;
                                while (cause != null && !(cause instanceof java.sql.SQLException)) cause = cause.getCause();
                                require(cause instanceof java.sql.SQLException sql && "55P03".equals(sql.getSQLState()),
                                        "independent transaction observes exact PostgreSQL lock refusal");
                            }
                        });
                        require(new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command, found.selections(),
                                created.assessment(), created.manifestSha256(), created.retainUntil(), budget, () -> {})
                                .orElseThrow().equals(created), "successor retained evidence reconciles exactly");
                    }
                    long starts = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_publication_assessment_starts WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    long assessments = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(starts == 2 && assessments == 1, "exactly one original and one successor start, one assessment");
                    var originalId = tx.readOnly(em -> em.createNativeQuery("""
                            SELECT assessment_id FROM repository_publication_assessment_starts
                            WHERE operation_id=:op AND predecessor_generation=0
                            """).setParameter("op", command.operationId()).getSingleResult());
                    require(originalId.equals(oldStart.assessment()), "original start remains unchanged");
                    long published = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(published == 0, "successor CREATE is not publication");
                }
                accepted.close();
                require(activation.tentativeCapture().orElseThrow().complete(caller, Duration.ZERO, RepositoryReadControl.NONE).isPresent(),
                        "new capture drains after its own accepted work closes");
            } finally {
                scopes.close(); history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "new history drains");
                history.release(); reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
