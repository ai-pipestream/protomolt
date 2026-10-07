package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** SQL writes shared by assessment CREATE paths; the caller owns the transaction and authority locks. */
final class DocumentAssessmentCreationWrites {
    private DocumentAssessmentCreationWrites() {}

    /** Prepared outside SQL; byte arrays and evidence remain owned by the synchronous CREATE call. */
    record Prepared(DocumentPublicationCommand command, DocumentUploadPlan.Prepared plan,
            Map<String, DocumentSelectedAttemptLedger.Selected> selected, DocumentReuseAdmission.Prepared reuse,
            DocumentAssessmentSlots.Prepared slotPlan, boolean historical, byte[] manifestBytes, String manifestSha,
            int count, Map<String, ByteString> artifacts, List<DocumentAssessmentEvidence.Root> roots) {}

    /** No transaction opening, descriptor resolution, provider I/O or caller authorization occurs here. */
    static DocumentAssessmentCreation.Created write(EntityManager em, RepositoryOperationLedger.Owner owner,
            Prepared input, DocumentAssessmentEvidence evidence, UUID assessment, Instant retainUntil,
            PayloadBudget scratch, Runnable control) {
        var command = input.command();
        var plan = input.plan();
        var selected = input.selected();
        var reuse = input.reuse();
        var slotPlan = input.slotPlan();
        var manifestBytes = input.manifestBytes();
        var manifestSha = input.manifestSha();
        var count = input.count();
        var artifacts = input.artifacts();
        var roots = input.roots();
        insertOwner(em, owner, command, assessment, retainUntil, manifestBytes, manifestSha, count, artifacts.size(), roots.size());
        final java.util.List<DocumentAssessmentSlots.Slot> slots;
        if (!input.historical()) {
            var physical = DocumentCommitParts.bindAssessment(em, owner, plan, selected, reuse, control);
            slots = DocumentAssessmentSlots.bind(em, slotPlan, physical, control);
        } else {
            var physical = DocumentCommitParts.bindHistoricalAssessment(em, owner, plan, selected, reuse, control);
            slots = DocumentAssessmentSlots.bind(em, slotPlan, physical.physical(), physical.locks(), control);
        }
        em.unwrap(org.hibernate.Session.class).doWork(connection -> {
            try (var statement = connection.prepareStatement("""
                    INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,selection_revision,
                        object_id,declaration,source_revision,source_ordinal,source_node) VALUES(?,?,?,?,?,?,?,?,?)
                    """)) {
                int pending = 0;
                for (var slot : slots) {
                    evidence.check(control);
                    statement.setObject(1, assessment); statement.setString(2, slot.member()); statement.setInt(3, slot.ordinal());
                    statement.setLong(4, slot.selection()); statement.setObject(5, slot.object()); statement.setString(6, slot.declaration());
                    statement.setObject(7, slot.sourceRevision()); statement.setObject(8, slot.sourceOrdinal());
                    statement.setObject(9, slot.sourceNode()); statement.addBatch();
                    if (++pending == 256) { requireBatch(statement.executeBatch(), pending); statement.clearBatch(); pending = 0; }
                }
                if (pending > 0) requireBatch(statement.executeBatch(), pending);
            }
        });
        em.createNativeQuery("UPDATE document_assessment_owners SET sealed=true WHERE assessment_id=:id")
                .setParameter("id", assessment).executeUpdate();
        for (var artifact : new java.util.TreeMap<>(artifacts).entrySet()) {
            evidence.check(control);
            int inserted = em.createNativeQuery("""
                    INSERT INTO document_assessment_artifacts(assessment_id,account_id,artifact_sha256)
                    SELECT :id,account_id,artifact_sha256 FROM repository_schema_artifacts
                    WHERE account_id=:account AND artifact_sha256=:sha AND size_bytes=:size
                    """).setParameter("id", assessment).setParameter("account", owner.key().account())
                    .setParameter("sha", hex(artifact.getKey())).setParameter("size", artifact.getValue().size()).executeUpdate();
            if (inserted != 1) throw new IllegalStateException("Assessment schema artifact is missing or differs in size");
        }
        em.unwrap(org.hibernate.Session.class).doWork(connection -> {
            try (var statement = connection.prepareStatement("""
                    INSERT INTO document_assessment_roots(assessment_id,member_id,revision_ordinal,root_locator_sha256,
                        fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """)) {
                for (var root : roots) {
                    evidence.check(control);
                    statement.setObject(1, assessment); statement.setString(2, root.member()); statement.setInt(3, root.ordinal());
                    statement.setBytes(4, hex(root.locatorSha256())); statement.setBytes(5, hex(root.fragmentSha256()));
                    statement.setLong(6, root.fragmentSize()); statement.setString(7, root.codec()); statement.setInt(8, root.version());
                    statement.setBytes(9, root.bytes().toByteArray()); statement.setBytes(10, hex(root.sha256()));
                    if (statement.executeUpdate() != 1) throw new IllegalStateException("Assessment root insertion was incomplete");
                    statement.clearParameters();
                }
            }
        });
        var identity = new DocumentAssessmentSlotSnapshot.Identity(assessment, owner.key(), owner.generation(),
                command.sha256(), manifestSha, retainUntil);
        try (var snapshot = DocumentAssessmentSlotSnapshot.encode(identity, slots, scratch, control);
             var jdbc = scratch.reserve(2L * snapshot.bytes().size())) {
            int written = em.createNativeQuery("""
                    INSERT INTO document_assessment_slot_snapshots(assessment_id,snapshot_codec,snapshot_version,
                        snapshot_bytes,snapshot_sha256) VALUES(:id,:codec,:version,:bytes,:sha)
                    """).setParameter("id", assessment).setParameter("codec", DocumentAssessmentSlotSnapshot.CODEC)
                    .setParameter("version", DocumentAssessmentSlotSnapshot.VERSION).setParameter("bytes", snapshot.bytes().toByteArray())
                    .setParameter("sha", hex(snapshot.sha256())).executeUpdate();
            if (written != 1) throw new IllegalStateException("Assessment slot snapshot insertion was incomplete");
        }
        em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
        RepositoryOperationLedger.fenceLiveOwner(em, owner);
        evidence.check(control);
        return new DocumentAssessmentCreation.Created(assessment, manifestSha, retainUntil);
    }

    private static void insertOwner(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID assessment, Instant deadline, byte[] manifest, String sha, int slots, int artifacts, int roots) {
        em.createNativeQuery("""
                INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                    command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,expected_artifacts,
                    expected_roots,retain_until,creation_xid)
                VALUES(:id,:account,:principal,:op,:gen,:codec,:version,:command,:bytes,:sha,:slots,:artifacts,:roots,:deadline,'0'::xid8)
                """).setParameter("id", assessment).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("op", owner.key().operationId()).setParameter("gen", owner.generation())
                .setParameter("codec", DocumentPublicationCommand.CODEC).setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                .setParameter("command", hex(command.sha256())).setParameter("bytes", manifest).setParameter("sha", hex(sha))
                .setParameter("slots", slots).setParameter("artifacts", artifacts).setParameter("roots", roots)
                .setParameter("deadline", OffsetDateTime.ofInstant(deadline, ZoneOffset.UTC)).executeUpdate();
    }
    private static void requireBatch(int[] counts, int expected) {
        if (counts.length != expected) throw new IllegalStateException("Assessment slot batch was incomplete");
        for (int count : counts) if (count != 1) throw new IllegalStateException("Assessment slot batch did not report exact writes");
    }
    private static byte[] hex(String value) { return HexFormat.of().parseHex(value); }
}
