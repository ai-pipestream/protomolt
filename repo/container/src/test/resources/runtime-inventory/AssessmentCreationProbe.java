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
                    var reconciliation = new DocumentAssessmentReconciliation(tx);
                    require(reconciliation.observe(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {}).isEmpty(),
                            "absent stage is not observed");
                    new RepositorySchemaArtifacts(tx).stage(owner, command, List.copyOf(evidence.artifacts(() -> {}).values()), () -> {});
                    cancelledRootInsert(tx, writer, caller, owner, prepared, selected, evidence, deadline, budget);
                    var result = writer.create(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {});
                    require(reconciliation.observe(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {}).orElseThrow().equals(result),
                            "original committed stage acknowledged");
                    try { reconciliation.observe(new RepositoryCaller("other-principal", true), owner, prepared, selected,
                            evidence, id, deadline, budget, () -> {}); throw new AssertionError("other principal acknowledged stage"); }
                    catch (RepositoryException expected) { require(expected.getMessage().contains("principal differs"), "current caller refusal"); }
                    try { reconciliation.observe(caller, owner, prepared, selected, evidence, UUID.randomUUID(), deadline, budget, () -> {});
                        throw new AssertionError("different assessment identity adopted"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("requested original stage"), "assessment identity refusal"); }
                    require(result.assessment().equals(id) && result.retainUntil().equals(deadline), "exact create result");
                    require(count(tx, "document_assessment_roots", id) == evidence.roots(() -> {}).size(), "complete roots");
                    require(count(tx, "document_assessment_artifacts", id) == evidence.artifacts(() -> {}).size(), "complete schema assets");
                    byte[] stored = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(ByteString.copyFrom(stored).equals(evidence.manifestBytes(() -> {})), "exact observed manifest");
                    var retainedSlots = tx.readOnly(em -> em.createNativeQuery("""
                            SELECT member_id,revision_ordinal,selection_revision,object_id,declaration,source_revision,source_ordinal
                            FROM document_assessment_slots WHERE assessment_id=:id
                            """).setParameter("id", id).getResultList()).stream().map(value -> {
                        Object[] row = (Object[]) value;
                        return new DocumentAssessmentSlots.Slot((String) row[0], ((Number) row[1]).intValue(), ((Number) row[2]).longValue(),
                                (UUID) row[3], (String) row[4], (UUID) row[5], row[6] == null ? null : ((Number) row[6]).intValue());
                    }).toList();
                    try (var snapshot = DocumentAssessmentSlotSnapshot.encode(new DocumentAssessmentSlotSnapshot.Identity(
                            id, owner.key(), owner.generation(), command.sha256(), evidence.manifestSha256(() -> {}), deadline),
                            retainedSlots, budget, () -> {})) {
                        Object[] retained = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                                SELECT snapshot_codec,snapshot_version,snapshot_bytes,encode(snapshot_sha256,'hex')
                                FROM document_assessment_slot_snapshots WHERE assessment_id=:id
                                """).setParameter("id", id).getSingleResult());
                        require(retained[0].equals(DocumentAssessmentSlotSnapshot.CODEC)
                                && ((Number) retained[1]).intValue() == DocumentAssessmentSlotSnapshot.VERSION
                                && ByteString.copyFrom((byte[]) retained[2]).equals(snapshot.bytes())
                                && retained[3].equals(snapshot.sha256()), "exact retained slot snapshot");
                    }
                    var slotIdentity = new DocumentAssessmentSlotSnapshot.Identity(id, owner.key(), owner.generation(),
                            command.sha256(), evidence.manifestSha256(() -> {}), deadline);
                    verifyRetainedSlots(tx, caller, owner, command, prepared, selected, slotIdentity, budget);
                    var changedSelection = new HashMap<>(selected);
                    var originalSelection = selected.get("a");
                    changedSelection.put("a", new DocumentSelectedAttemptLedger.Selected("a", originalSelection.revision() + 1,
                            originalSelection.attempt(), originalSelection.token()));
                    try { verifyRetainedSlots(tx, caller, owner, command, prepared, changedSelection, slotIdentity, budget);
                        throw new AssertionError("changed selection reconciled"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("original staging identity"), "changed selection refusal"); }
                    try { verifyRetainedSlots(tx, caller, owner, command, prepared, selected,
                            new DocumentAssessmentSlotSnapshot.Identity(id, owner.key(), owner.generation(), command.sha256(),
                                    evidence.manifestSha256(() -> {}), deadline.plusSeconds(1)), budget);
                        throw new AssertionError("extended deadline reconciled"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("original staging identity"), "changed deadline refusal"); }
                    // An uncertain acknowledgement must use reconciliation, not a second create.
                    UUID duplicate = UUID.randomUUID();
                    try { writer.create(caller, owner, prepared, selected, evidence, duplicate, deadline, budget, () -> {}); throw new AssertionError("duplicate generation adopted"); }
                    catch (RuntimeException expected) { require(hasSqlState(expected, "23505"), "duplicate create conflicts"); }
                    require(count(tx, "document_assessment_owners", duplicate) == 0, "duplicate owner rollback");
                    require(count(tx, "document_assessment_owners", id) == 1, "original owner preserved");
                    byte[] afterRetry = tx.readOnly(em -> (byte[]) em.createNativeQuery("SELECT manifest_bytes FROM document_assessment_owners WHERE assessment_id=:id")
                            .setParameter("id", id).getSingleResult());
                    require(Arrays.equals(stored, afterRetry), "duplicate create preserves original evidence");
                    if (invalid) {
                        var revised = DocumentAdmissionPolicy.of(policy.definition().toBuilder()
                                .setLimits(policy.definition().getLimits().toBuilder().setMaxRoots(101)).build(), () -> {});
                        var changedPolicy = new DocumentSchemaPolicies(tx).activate(revised, active.revision(), () -> {});
                        require(changedPolicy.revision() > active.revision(), "policy actually advanced");
                        require(reconciliation.observe(caller, owner, prepared, selected, evidence, id, deadline, budget, () -> {})
                                .orElseThrow().equals(result), "policy advance does not erase original staging acknowledgement");
                    }
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
    private static void verifyRetainedSlots(Tx tx, RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentOperationUploadAdmission.Prepared prepared,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentSlotSnapshot.Identity identity, PayloadBudget budget) {
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            em.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE")
                    .setParameter("id", identity.assessment()).getSingleResult();
            DocumentAssessmentRetainedSlots.verify(em, identity, prepared.plan(), selected, budget, () -> {});
        });
    }
    /** Cancel real SQL after owner/physical/schema insertion; no successful backend is simulated. */
    private static void cancelledRootInsert(Tx tx, DocumentAssessmentCreation writer, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared prepared,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentEvidence evidence,
            Instant deadline, PayloadBudget budget) {
        UUID cancelled = UUID.randomUUID();
        long before = budget.reservedBytes();
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_cancel_assessment_root() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN RAISE EXCEPTION 'injected assessment root cancellation' USING ERRCODE='57014'; END $$
                    """).executeUpdate();
            em.createNativeQuery("CREATE TRIGGER test_cancel_assessment_root BEFORE INSERT ON document_assessment_roots "
                    + "FOR EACH ROW EXECUTE FUNCTION test_cancel_assessment_root()").executeUpdate();
        });
        try {
            try { writer.create(caller, owner, prepared, selected, evidence, cancelled, deadline, budget, () -> {});
                throw new AssertionError("cancelled SQL acknowledged creation"); }
            catch (RuntimeException expected) {
                require(hasSqlState(expected, "57014") && hasMessage(expected, "injected assessment root cancellation"), "exact SQL cancellation propagated");
            }
            for (String table : List.of("document_assessment_owners", "document_assessment_slots",
                    "document_assessment_objects", "document_assessment_artifacts", "document_assessment_roots", "document_assessment_slot_snapshots"))
                require(count(tx, table, cancelled) == 0, "cancelled transaction rolled back " + table);
            long references = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                    .setParameter("id", cancelled).getSingleResult()).longValue());
            require(references == 0, "cancelled physical references rolled back");
            require(budget.reservedBytes() == before, "cancelled writer scratch released");
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER test_cancel_assessment_root ON document_assessment_roots").executeUpdate();
                em.createNativeQuery("DROP FUNCTION test_cancel_assessment_root()").executeUpdate();
            });
        }
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
