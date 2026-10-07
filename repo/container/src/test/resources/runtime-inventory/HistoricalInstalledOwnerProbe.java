package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Real provider mixed publication with assessment ownership spanning separate client calls. */
final class HistoricalInstalledOwnerProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord original, RepositorySuccessorInstall.Plan plan,
            DocumentHistoricalAssessmentSources sources, DocumentHistoricalAssessmentSources.Work accepted,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            DocumentRevisionAssembly.Limits limits, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        require(!caller.processAuthority(), "installed owner uses scoped execution caller");
        var command = plan.next().command();
        long before = budget.reservedBytes();
        var attempts = new RepositoryInstalledHistoricalAttempts(tx, budget, new DriveLedger(tx), 1);
        var runtime = new DocumentPublicationScopeCalls();
        boolean transferred = false;
        try {
            DocumentAssessmentStartJournal.Started started;
            DocumentOperationUploadAdmission.Admission admitted;
            try (var call = runtime.enter(); var request = attempts.beginInstalled(caller, plan, original)) {
                request.attachSources(sources, accepted, RepositoryReadControl.NONE); transferred = true;
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                admitted = request.admitUploads(RepositoryReadControl.NONE);
                started = request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE);
                request.prepareAssessment(policy, Map.of("a", fragments), container, resolver, limits,
                        Instant.now(), RepositoryReadControl.NONE);
            }
            require(runtime.isIdle(), "first client call releases runtime barrier while assessment stays retained");
            require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 1)), "entry survives first call");
            sources.close(); // Subsequent requests must use the retained Work and assessment.
            var owner = tx.inTransaction(em -> {
                var claim = RepositoryExecutionClaimLedger.lockLive(em, plan.next().key(), command.sha256(),
                        plan.reservation().predecessor().epoch()+1, plan.reservation().successorToken());
                return RepositoryOperationLedger.lockLiveOwner(em, plan.next().key(), plan.next().predecessorGeneration()+1,
                        plan.next().seeds().ownerNonce(), Optional.of(claim));
            });
            require(admitted.attempts().size() == 1, "mixed owner admits exactly one upload");
            var upload = admitted.attempts().getFirst();
            require(upload.id().equals(plan.next().seeds().attempts().get("a"))
                    && !upload.id().equals(original.seeds().attempts().get("a")), "fresh attempt belongs to successor");
            var selected = new DocumentSelectedAttemptLedger.Selected("a", 1, upload.id(), upload.token());
            var selections = Map.of("a", selected);
            DocumentAssessmentCreation.Created created;
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                request.openExecution(coordinator, RepositoryReadControl.NONE);
                require(request.start(Duration.ofMinutes(1), RepositoryReadControl.NONE).equals(started), "START identity survives call boundary");
                try {
                    request.prepareAssessment(policy, Map.of("a", fragments), container, resolver, limits,
                            Instant.now(), RepositoryReadControl.NONE);
                    throw new AssertionError("Retained assessment was replaced");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.CONFLICT, "assessment replacement refused");
                }
                var physical = request.withAssessment(assessment -> assessment.preparePhysical(plan.next().placements(),
                        plan.next().seeds().attempts(), plan.next().lease(), plan.next().seeds().uploadTokens(),
                        RepositoryReadControl.NONE), RepositoryReadControl.NONE);
                var member = physical.plan().members().getFirst();
                var measured = member.attempt().orElseThrow().uploads().stream().map(part -> {
                    var object = part.object();
                    var actual = DocumentPartTransfer.upload(provider.store(), member.placement().drive().namespace(), object,
                            fragments.get(part.revisionOrdinal()).toByteArray(), Map.of(), () -> {}, () -> {});
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), actual.version(), actual.etag());
                }).toList();
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selected, measured);
                created = request.createAssessment(selections, observation, new RepositorySchemaArtifacts(tx), started,
                        RepositoryReadControl.NONE);
            }
            require(runtime.isIdle(), "CREATE request releases runtime barrier");
            var found = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
            require(found.stage().equals(created), "separate request CREATE has exact persisted identity");
            ai.protomolt.proto.repo.v1.DocumentPublicationResult result;
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                result = request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                        new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
            }
            require(runtime.isIdle(), "publication request releases runtime barrier");
            HistoricalClaimedMixedPublicationProbe.verify(tx, provider, caller, command, owner, selections, fragments, result);
            try (var call = runtime.enter(); var request = attempts.resume(caller, command).orElseThrow()) {
                try {
                    request.publishAssessment(selections, observation, new RepositorySchemaArtifacts(tx),
                            new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
                    throw new AssertionError("A later call repeated publication");
                } catch (RepositoryException refused) {
                    require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                            && refused.getMessage().contains("reconciliation"), "sticky publication flag survives call boundary");
                }
            }
            require(runtime.isIdle(), "repeat refusal releases runtime barrier");
            require(count(tx, "repository_publication_assessment_starts", command.operationId()) == 2,
                    "original and successor START only");
            require(count(tx, "document_assessment_owners", command.operationId()) == 1, "exactly one CREATE");
            require(count(tx, "document_revision_commits", command.operationId()) == 1, "exactly one publication");
        } finally {
            runtime.close(); attempts.close();
            require(runtime.awaitIdle(Duration.ofSeconds(1)), "client requests drained before owner shutdown");
            require(attempts.detachClosed(Duration.ofSeconds(1), ignored -> coordinator, RepositoryReadControl.NONE),
                    "retained owner closes assessment and execution then drains its exact capture");
            if (transferred) require(budget.reservedBytes() == before, "owner returns every retained byte reservation");
        }
        System.out.println("SCOPED_INSTALLED_HISTORICAL_MULTICALL_PUBLICATION_OK");
    }
    private static long count(Tx tx, String table, UUID operation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", operation).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
