package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Rejection authorization over an actual older reused source and real uploaded fragments. */
public final class RejectedAssessmentSourceProbe {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);
    record Candidate(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selected, DocumentAssessmentCreation.Created stage) {}

    static Candidate prepare(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentSchemaPolicies.Selection policy, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        return NativeAssessmentPreparationProbe.run(tx, provider, source, policy, observation);
    }

    static void verify(Tx tx, AssessmentProviderProbe provider, UUID source, Candidate candidate,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
        var command = candidate.command(); var stage = candidate.stage();
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "original mixed provider identity");
            return provider.store();
        }, 2, 16_000_000, payload);
        try (reader) {
            try (var capture = reads.captureAssessment(caller, candidate.owner(), command, candidate.selected(), stage.assessment(),
                    stage.manifestSha256(), stage.retainUntil(), budget, () -> {});
                    var verified = DocumentAssessmentReplay.verify(capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE)) {
                require(verified.result().firstFailure().isPresent(), "old mixed source reproduces invalid assessment");
                var gate = new DocumentAssessmentRejections(tx, Duration.ofSeconds(5));
                try {
                    policy(tx, source, "ACCESS_DENY");
                    refused(() -> new DocumentAssessmentDiscovery(tx).discover(caller, candidate.owner(), command, () -> {}));
                    refused(() -> gate.decide(caller, candidate.owner(), command, verified, RepositoryReadControl.NONE));
                    long outcomes = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(outcomes == 0, "revoked source blocks terminal decision");
                } finally { policy(tx, source, "ACCESS_READ"); }
                require(gate.decide(caller, candidate.owner(), command, verified, RepositoryReadControl.NONE)
                        .rejection().orElseThrow().getAssessment().getAssessmentId().equals(stage.assessment().toString()),
                        "restored source permits exact verified rejection");
            } finally { require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "live decision session drains"); }
            try {
                policy(tx, source, "ACCESS_DENY");
                refused(() -> reads.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE));
                refused(() -> new DocumentPublicationReplay(tx).observe(caller, command));
                require(reads.outstandingReads() == 0, "denied capture retains no capacity");
            } finally { policy(tx, source, "ACCESS_READ"); }
            try (var capture = reads.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE)) {
                var original = DocumentAssessmentReplay.replay(capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE);
                require(original.firstFailure().isPresent() && original.assessment().equals(stage.assessment()), "exact rejected mixed evidence");
                for (boolean privateFailure : List.of(false, true)) {
                    var fetched = new AtomicBoolean();
                    try {
                        refused(() -> DocumentAssessmentReplay.replay(capture, (captured, member, control) -> {
                            var batch = reader.readAssessment(captured, member, control);
                            boolean returned = false;
                            try {
                                fetched.set(true); policy(tx, source, "ACCESS_DENY");
                                if (privateFailure) throw new IllegalStateException("private-rejected-source-provider-detail");
                                returned = true; return batch;
                            } finally { if (!returned) batch.close(); }
                        }, budget, LIMITS, observation, RepositoryReadControl.NONE));
                        require(fetched.get(), "source revoked after actual complete provider batch");
                    } finally { policy(tx, source, "ACCESS_READ"); }
                }
                require(DocumentAssessmentReplay.replay(capture, reader, budget, LIMITS, observation, RepositoryReadControl.NONE)
                        .equals(original), "restored source reproduces exact retained result");
            } finally { require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "rejected mixed session drains"); }
        } finally {
            require(reader.awaitIdle(Duration.ofSeconds(5)), "mixed provider workers drain");
            require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "mixed rejection releases memory");
        }
        System.out.println("REJECTED_ASSESSMENT_SOURCE_AUTHORIZATION_OK");
    }

    private static void policy(Tx tx, UUID source, String access) {
        tx.inTransaction(em -> {
            require(em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("id", source).executeUpdate() == 1, "exact source policy changed");
        });
    }
    private static void refused(Runnable action) {
        try { action.run(); throw new AssertionError("Revoked source delivered a result"); }
        catch (RepositoryException failure) {
            require(failure.code() == RepositoryException.Code.NOT_FOUND && failure.getCause() == null
                    && failure.getSuppressed().length == 0, "source refusal conceals evidence and private errors");
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
