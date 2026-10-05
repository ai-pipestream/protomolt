package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;

/** Real observed evidence and SQL transaction; physical observations are explicitly synthetic. */
public final class AssessmentCreationProbe {
    public static void run(Tx tx, DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setValidationProfile(DocumentSchemaAdmission.PROFILE).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(20).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        var active = new DocumentSchemaPolicies(tx).activate(policy, 0, () -> {});
        for (boolean invalid : new boolean[]{false, true}) {
            var a = ObservedAssessmentProbe.member("a"); var b = ObservedAssessmentProbe.member("b");
            var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                    .setOperationId(UUID.randomUUID().toString()).setAccountId("account").addMembers(a.member()).addMembers(b.member()).build());
            var caller = new RepositoryCaller("principal", true);
            var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1",
                    Map.of("endpoint", "synthetic-provider-observations")), "creation-probe");
            new ManagedBackendLedger(tx).bind("creation-probe", profile);
            var drives = new DriveLedger(tx);
            var placements = new HashMap<UUID,DocumentUploadPlan.Placement>();
            var attempts = new HashMap<String,UUID>();
            for (var member : command.intent().getMembersList()) {
                var drive = new DriveRecord(); drive.driveId = UUID.fromString(member.getDriveId()); drive.accountId = "account";
                drive.name = "creation-" + drive.driveId; drive.bucket = "namespace"; drive.prefix = "root";
                drive.provider = "test-location"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
                drives.insert(drive);
                placements.put(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "creation-probe", profile));
                attempts.put(member.getMemberId(), UUID.randomUUID());
            }
            var prepared = DocumentOperationUploadAdmission.prepare(command, placements, attempts, Duration.ofMinutes(5));
            var admitted = new DocumentOperationUploadAdmission(tx, drives).admit(caller, owner, prepared);
            var selected = new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
            for (var member : prepared.plan().members()) {
                var attempt = admitted.stream().filter(value -> value.id().equals(member.attempt().orElseThrow().id())).findFirst().orElseThrow();
                var selection = new DocumentSelectedAttemptLedger.Selected(member.intent().getMemberId(), 1, attempt.id(), attempt.token());
                selected.put(selection.member(), selection);
                // Exercise production verification SQL with labelled synthetic measurements,
                // never a fake successful object-store adapter.
                new DocumentSelectedAttemptLedger(tx).verifyBatch(owner, selection, member.attempt().orElseThrow().uploads().stream().map(upload -> {
                    var object = upload.object();
                    return new DocumentSelectedAttemptLedger.Observation(object.objectKey(), object.size(), object.sha256(),
                            object.contentType(), "fixture-version", "fixture-etag");
                }).toList());
            }
            var budget = new PayloadBudget(64_000_000);
            var payload = invalid ? ObservedAssessmentProbe.invalidSchema() : ObservedAssessmentProbe.asset(StringValue.getDescriptor());
            try (var assessment = DocumentPublicationAssessment.prepare(command, active,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED, "b", DocumentPublicationCandidate.Mode.OPAQUE),
                    Map.of("a", a.fragments(), "b", b.fragments()), Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor())),
                    (member, occurrence) -> payload, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), () -> {})) {
                require(assessment.failure().isPresent() == invalid, "real semantic result");
                UUID id = UUID.randomUUID();
                Instant deadline = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                long before = budget.reservedBytes();
                assessment.withRetentionEvidence(owner, observation, () -> {}, evidence -> {
                    var writer = new DocumentAssessmentCreation(tx, drives);
                    var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation() + 1, owner.token(), owner.leaseUntil());
                    try { writer.create(caller, wrong, prepared, selected, evidence, id, deadline, budget, () -> {}); throw new AssertionError("wrong owner accepted"); }
                    catch (IllegalArgumentException expected) { require(expected.getMessage().contains("differs from operation owner"), "owner identity refusal"); }
                    try { writer.create(caller, owner, prepared, selected, evidence, id, deadline.plusNanos(1), budget, () -> {}); throw new AssertionError("rounded deadline accepted"); }
                    catch (IllegalArgumentException expected) { require(expected.getMessage().contains("exact microsecond precision"), "deadline precision refusal"); }
                    // Missing claims fail after physical acquisition/sealing and must roll back all rows.
                    try { writer.create(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {}); throw new AssertionError("unstaged schemas accepted"); }
                    catch (RuntimeException expected) {
                        require(hasMessage(expected, "schema artifact is missing") || hasMessage(expected, "exact current-generation artifact claim"), "artifact failure");
                    }
                    require(count(tx, "document_assessment_owners", id) == 0, "owner rollback");
                    require(count(tx, "document_assessment_objects", id) == 0, "physical rollback");
                    new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                    var result = writer.create(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {});
                    require(result.assessment().equals(id) && result.retainUntil().equals(deadline), "exact create result");
                    require(count(tx, "document_assessment_roots", id) == evidence.roots(() -> {}).size(), "complete roots");
                    require(count(tx, "document_assessment_artifacts", id) == evidence.artifacts(() -> {}).size(), "complete schema assets");
                    byte[] stored = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(ByteString.copyFrom(stored).equals(evidence.manifestBytes(() -> {})), "exact observed manifest");
                    // An uncertain acknowledgement must use reconciliation, not a second create.
                    UUID duplicate = UUID.randomUUID();
                    try { writer.create(caller, owner, prepared, selected, evidence, duplicate, deadline, budget, () -> {}); throw new AssertionError("duplicate generation adopted"); }
                    catch (RuntimeException expected) { require(hasSqlState(expected, "23505"), "duplicate create conflicts"); }
                    require(count(tx, "document_assessment_owners", duplicate) == 0, "duplicate owner rollback");
                    require(count(tx, "document_assessment_owners", id) == 1, "original owner preserved");
                    byte[] afterRetry = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(Arrays.equals(stored, afterRetry), "duplicate create preserves original evidence");
                    return result;
                });
                require(budget.reservedBytes() == before, "writer and manifest reservations released");
                long published = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                long decisions = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_operation_rejection WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                require(published == 0 && decisions == 0, "staging is neither publication nor terminal rejection");
            }
            require(budget.reservedBytes() == 0, "assessment reservations released");
        }
        System.out.println("OBSERVED_ASSESSMENT_CREATION_OK");
    }
    private static long count(Tx tx, String table, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static void require(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
    private static boolean hasMessage(Throwable failure, String text) {
        for (var cause = failure; cause != null; cause = cause.getCause())
            if (cause.getMessage() != null && cause.getMessage().contains(text)) return true;
        return false;
    }
    private static boolean hasSqlState(Throwable failure, String state) {
        for (var cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof java.sql.SQLException sql && state.equals(sql.getSQLState())) return true;
        return false;
    }
}
