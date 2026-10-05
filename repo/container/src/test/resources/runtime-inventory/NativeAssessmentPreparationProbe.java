package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/** Actual native upload/input preparation with owned assessments and real retained-source reads. */
public final class NativeAssessmentPreparationProbe {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000);

    static RejectedAssessmentSourceProbe.Candidate run(Tx tx, AssessmentProviderProbe provider,
            AssessmentMixedReuseProbe.Source source, DocumentSchemaPolicies.Selection policy,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        RejectedAssessmentSourceProbe.Candidate invalidCandidate = null;
        var valid = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        var invalid = ObservedAssessmentProbe.invalidSchema();
        var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
        for (int mode = 0; mode < 4; mode++) {
            int scenario = mode;
            var command = AssessmentMixedReuseProbe.command(source.candidate());
            var owner = AssessmentMixedReuseProbe.owner(tx, command);
            var plan = DocumentOperationUploadAdmission.prepare(command, Map.of(source.placement().drive().id(), source.placement()),
                    Map.of("a", UUID.randomUUID()), Duration.ofMinutes(5));
            var bodies = new HashMap<DocumentUploadPayloads.Key,PartObject>();
            for (int ordinal = 0; ordinal < source.candidate().getPartsCount(); ordinal++) {
                var part = source.candidate().getParts(ordinal);
                if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key("a", ordinal),
                        new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                source.fragments().get(ordinal).toByteArray(), part.getUpload().getSha256()));
            }
            var caller = new RepositoryCaller("principal", true);
            var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var calls = new AtomicInteger(); var cancelled = new AtomicBoolean();
            var resolverFailure = new IllegalStateException("authorized schema resolver is unavailable");
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return cancelled.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var settings = new DocumentPublicationPreparation.Admission(policy,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED), container, (member, occurrence) -> {
                        calls.incrementAndGet();
                        if (scenario == 2) throw resolverFailure;
                        if (scenario == 3) cancelled.set(true);
                        return scenario == 1 ? invalid : valid;
                    }, LIMITS);
            var at = Instant.now();
            try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                    "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                    "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                    "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
                    var reader = new DocumentPartReader((generation, profile) -> {
                        require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact retained source provider");
                        return opened.store();
                    }, 2, 16_000_000, payload);
                    var uploads = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, profile) -> {
                        require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact upload provider");
                        return new DocumentUploadCoordinator.Backend(provider.profile().identity(), opened);
                    }, 2, Duration.ofMillis(25), new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)))) {
                var preparation = new DocumentPublicationPreparation(uploads, reader, budget);
                try (var pinned = reads.capture(new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)), caller, owner, plan)) {
                    try (var assessed = preparation.assess(caller, owner, plan, bodies, Map.of(), pinned, settings, at, control)) {
                        require(scenario < 2, "operational failure cannot return an assessment");
                        require(calls.get() == 1 && payload.reservedBytes() == 0, "one resolver pass and borrowed provider batches closed");
                        require(budget.reservedBytes() > 0, "returned assessment owns reserved copies");
                        bodies.values().forEach(body -> Arrays.fill(body.bytes(), (byte) 0));
                        var assessment = assessed.assessment();
                        require(assessment.evaluatedAt().equals(at) && assessment.failure().isPresent() == (scenario == 1),
                                "explicit evaluation time and real value verdict preserved");
                        assessment.verifySchemas(() -> {});
                        require(calls.get() == 1, "owned evidence verification makes no resolver calls");
                        var selected = assessed.selections();
                        require(selected.size() == 1 && selected.get("a").attempt().equals(
                                plan.plan().members().getFirst().attempt().orElseThrow().id()), "exact upload selection survives return");
                        if (scenario == 1) {
                            var retained = assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                                new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                                return new DocumentAssessmentCreation(tx, new DriveLedger(tx)).create(caller, owner, plan, selected,
                                        evidence, UUID.randomUUID(), Instant.now().plusSeconds(120)
                                                .truncatedTo(java.time.temporal.ChronoUnit.MICROS), budget, () -> {});
                            });
                            invalidCandidate = new RejectedAssessmentSourceProbe.Candidate(command, owner,
                                    DocumentAssessmentRetainedSlots.uploadSelections(selected), retained);
                        }
                    } catch (RuntimeException failure) {
                        if (scenario == 2) require(failure == resolverFailure, "resolver failure remains operational");
                        else if (scenario == 3) require(failure instanceof RepositoryException repository
                                && repository.code() == RepositoryException.Code.CANCELLED, "cancellation remains operational");
                        else throw failure;
                    }
                    require(calls.get() == 1, "each candidate resolves its single root once");
                    require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "assessment or failure releases copies");
                } finally {
                    reader.close();
                    require(reader.awaitIdle(Duration.ofSeconds(5)), "native reader drains");
                    require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "native source pin drains");
                }
            }
            long outcomes = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT (SELECT count(*) FROM repository_operation_success WHERE operation_id=:op)
                         + (SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op)
                    """).setParameter("op", command.operationId()).getSingleResult()).longValue());
            require(outcomes == 0, "preparation grants neither publication nor rejection");
        }
        require(invalidCandidate != null, "invalid assessment retained for later decision tests");
        System.out.println("NATIVE_ASSESSMENT_PREPARATION_OK");
        return invalidCandidate;
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
