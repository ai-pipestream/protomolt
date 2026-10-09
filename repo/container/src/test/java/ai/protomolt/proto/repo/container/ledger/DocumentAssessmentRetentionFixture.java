package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** Synthetic SQL declarations, not canonical manifest admission or provider byte verification. */
final class DocumentAssessmentRetentionFixture {
    final Tx tx;
    final RepositoryOperationLedger operations;
    private final long fragmentSize;
    final String backend = "assessment-" + UUID.randomUUID();
    record Candidate(RepositoryOperationLedger.Owner owner, UUID attempt, UUID assessment) {}

    DocumentAssessmentRetentionFixture(Tx tx) {
        this(tx, 1);
    }

    DocumentAssessmentRetentionFixture(Tx tx, long fragmentSize) {
        this.tx = tx;
        this.fragmentSize = fragmentSize;
        operations = new RepositoryOperationLedger(tx);
        new ManagedBackendLedger(tx).bind(backend, new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "synthetic-sql-fixture")), "realm"));
    }

    Candidate candidate(int leaseSeconds) {
        return candidate(leaseSeconds, true);
    }

    Candidate reuseCandidate() {
        return candidate(120, false);
    }

    private Candidate candidate(int leaseSeconds, boolean uploads) {
        return candidate(admitOperation(Duration.ofMinutes(2)), leaseSeconds, uploads);
    }

    /** A real canonical envelope; candidate SQL declarations below remain deliberately synthetic. */
    RepositoryOperationLedger.Owner admitOperation(Duration lease) {
        var key = new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID());
        var member = ai.protomolt.proto.repo.v1.DocumentPublicationMember.newBuilder()
                .setMemberId("sql-fixture").setDriveId(UUID.randomUUID().toString())
                .setRowKind(ai.protomolt.proto.repo.v1.DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(ai.protomolt.proto.repo.v1.DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                        .setAddress(ai.protomolt.proto.repo.v1.NodeAddress.newBuilder().setAccountId("account")
                                .setDocId("sql-fixture").setGraphId("graph").setGraphAddressId("node")))
                .setOwnership(ai.protomolt.proto.repo.v1.OwnershipContext.newBuilder().setAccountId("account")
                        .setDatasourceId("fixture").setSecurity(ai.protomolt.proto.repo.v1.DocumentSecurity.getDefaultInstance()))
                .addParts(ai.protomolt.proto.repo.v1.DocumentPublicationPart.newBuilder()
                        .setSlot(ai.protomolt.proto.repo.v1.DocumentPublicationSlot.newBuilder()
                                .setPart(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE))
                        .setUpload(ai.protomolt.proto.repo.v1.PublicationUpload.newBuilder().setSizeBytes(fragmentSize)
                                .setSha256("a".repeat(64)).setContentType("application/protobuf")));
        var command = new ai.protomolt.proto.repo.spi.DocumentPublicationCommand(
                ai.protomolt.proto.repo.v1.DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                        .setAccountId("account").setOperationId(key.operationId().toString()).addMembers(member).build());
        return operations.admit(key, command, UUID.randomUUID(), lease).owner().orElseThrow();
    }

    Candidate candidate(RepositoryOperationLedger.Owner owner, int leaseSeconds) {
        return candidate(owner, leaseSeconds, true);
    }

    private Candidate candidate(RepositoryOperationLedger.Owner owner, int leaseSeconds, boolean uploads) {
        var key = owner.key();
        var candidate = new Candidate(owner, UUID.randomUUID(), UUID.randomUUID());
        UUID node = UUID.randomUUID();
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "assessment-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "test-location"; drive.bucket = "namespace";
        new DriveLedger(tx).insert(drive);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            if (!uploads) {
                em.createNativeQuery("""
                        INSERT INTO document_operation_selections(account_id,principal,operation_id,owner_generation,
                            member_id,node_id,sampled_revision,drive_id,drive_snapshot,drive_sha256,backend_generation,
                            storage_realm,storage_namespace,upload_count,attempt_id)
                        SELECT 'account','principal',:op,:gen,'member',:node,0,d.drive_id,
                            document_operation_drive_snapshot(d),document_operation_drive_digest_v1(d),:backend,
                            'realm','namespace',0,NULL FROM drives d WHERE d.drive_id=:drive
                        """).setParameter("op", key.operationId()).setParameter("gen", owner.generation())
                        .setParameter("node", node).setParameter("backend", backend).setParameter("drive", drive.driveId).executeUpdate();
                return;
            }
            em.createNativeQuery("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state,
                        plan_kind,operation_principal,operation_id,operation_generation,member_id,drive_id)
                    VALUES (:id,:node,'account',0,:backend,'realm','namespace',2,0,gen_random_uuid(),
                        clock_timestamp()+(:seconds * interval '1 second'),'PLANNING','NEW_CONTENT','principal',:op,:gen,'member',:drive)
                    """).setParameter("id", candidate.attempt).setParameter("node", node).setParameter("backend", backend)
                    .setParameter("seconds", leaseSeconds).setParameter("op", key.operationId()).setParameter("gen", owner.generation())
                    .setParameter("drive", drive.driveId).executeUpdate();
            for (int dense = 0; dense < 2; dense++) {
                int ordinal = dense == 0 ? 2 : 5;
                em.createNativeQuery("""
                        INSERT INTO document_part_attempt_objects(attempt_id,ordinal,revision_ordinal,part,sub_key,
                            storage_realm,storage_namespace,object_key,expected_size,expected_sha256,content_type)
                        VALUES (:id,:dense,:ordinal,3,:sub,'realm','namespace',:key,:size,:sha,'application/protobuf')
                        """).setParameter("id", candidate.attempt).setParameter("dense", dense).setParameter("ordinal", ordinal)
                        .setParameter("sub", "chunk-" + ordinal)
                        .setParameter("key", "documents/account/" + node + "/attempts/" + candidate.attempt + "/part-" + ordinal)
                        .setParameter("size", fragmentSize).setParameter("sha", "a".repeat(64)).executeUpdate();
            }
            em.createNativeQuery("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=:id")
                    .setParameter("id", candidate.attempt).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_operation_selections(account_id,principal,operation_id,owner_generation,
                        member_id,node_id,sampled_revision,drive_id,drive_snapshot,drive_sha256,backend_generation,
                        storage_realm,storage_namespace,upload_count,attempt_id)
                    SELECT a.account_id,a.operation_principal,a.operation_id,a.operation_generation,a.member_id,
                        a.node_id,a.sampled_revision,a.drive_id,document_operation_drive_snapshot(d),document_operation_drive_digest_v1(d),
                        a.backend_generation,a.storage_realm,a.storage_namespace,a.planned_count,a.attempt_id
                    FROM document_part_attempts a JOIN drives d ON d.drive_id=a.drive_id WHERE a.attempt_id=:id
                    """).setParameter("id", candidate.attempt).executeUpdate();
            em.createNativeQuery("UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id")
                    .setParameter("id", candidate.attempt).executeUpdate();
            em.createNativeQuery("UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id")
                    .setParameter("id", candidate.attempt).executeUpdate();
        });
        return candidate;
    }

    void insertOwner(EntityManager em, Candidate c, int seconds, int expected) {
        insertOwner(em, c, seconds, expected, 0);
    }

    void insertOwner(EntityManager em, Candidate c, int seconds, int expected, int artifacts) {
        em.createNativeQuery("""
                INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                    command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid,expected_artifacts)
                SELECT :id,account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                    decode('01','hex'),sha256(decode('01','hex')),:expected,clock_timestamp()+(:seconds * interval '1 second'),'0'::xid8,:artifacts
                FROM repository_operations WHERE account_id='account' AND principal='principal' AND operation_id=:op
                """).setParameter("id", c.assessment).setParameter("generation", c.owner.generation())
                .setParameter("expected", expected).setParameter("artifacts", artifacts).setParameter("seconds", seconds)
                .setParameter("op", c.owner.key().operationId()).executeUpdate();
    }

    void insertSlots(EntityManager em, Candidate c, String ordinal, long selection) {
        em.createNativeQuery("""
                INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,selection_revision,object_id,declaration)
                SELECT :id,'member',%s,:selection,physical_object_id,'NEW_CONTENT'
                FROM document_part_attempt_objects WHERE attempt_id=:attempt ORDER BY ordinal
                """.formatted(ordinal)).setParameter("id", c.assessment).setParameter("attempt", c.attempt)
                .setParameter("selection", selection).executeUpdate();
    }

    void seal(EntityManager em, Candidate c) {
        em.createNativeQuery("UPDATE document_assessment_owners SET sealed=true WHERE assessment_id=:id")
                .setParameter("id", c.assessment).executeUpdate();
    }

    void stage(Candidate c, int seconds) {
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner);
            insertOwner(em, c, seconds, 2); insertSlots(em, c, "revision_ordinal", 1); seal(em, c);
        });
    }

    boolean release(Candidate c) {
        return tx.inTransaction(em -> (Boolean) em.createNativeQuery(
                "SELECT release_expired_document_assessment('account','principal',:op,:id)")
                .setParameter("op", c.owner.key().operationId()).setParameter("id", c.assessment).getSingleResult());
    }

    void expire(String table, String idColumn, UUID id, String deadlineColumn) {
        tx.readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM " + deadlineColumn
                + "-clock_timestamp()))+0.03) FROM " + table + " WHERE " + idColumn + "=:id")
                .setParameter("id", id).getSingleResult());
    }
}
