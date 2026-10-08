package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveReadRecovery;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

/** SQL-only archive pin reclamation after verified managed-child termination; no provider read is claimed. */
@Testcontainers
class ArchiveExternalQuiescenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void verifiedTerminationQuiescesOnlyTheExactReaderBeforeArchivePinRecovery() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild()) {
            var identity = child.identity;
            ReaderHostExecutions.register(c.tx(), identity.execution(), identity.host(), identity.boot());
            UUID reader = UUID.randomUUID();
            ReaderRegistration.register(c.tx(), reader, identity.execution());
            UUID registrationNonce = c.tx().readOnly(em -> (UUID) em.createNativeQuery(
                    "SELECT registration_nonce FROM repository_reader_incarnations WHERE incarnation=:reader")
                    .setParameter("reader", reader).getSingleResult());

            var generation = "external-archive-" + UUID.randomUUID();
            new ManagedBackendLedger(c.tx()).bind(generation, new ManagedBackendLedger.Profile(
                    S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
            UUID entry = UUID.randomUUID();
            var binding = new ArchiveObjectLedger(c.tx()).register(new ArchiveObjectLedger.Location(
                    entry, "account", "external-quiescence", generation, "bucket", "key-" + UUID.randomUUID()));
            createArchiveVersion(c.tx(), entry, binding.objectId());

            // Direct native SQL exercises the database pin guard and mirror trigger. These
            // synthetic rows establish SQL retention only, not provider bytes or reads.
            UUID targetPin = UUID.randomUUID(), foreignReader = UUID.randomUUID(), foreignPin = UUID.randomUUID();
            UUID secondTargetPin = UUID.randomUUID();
            ReaderRegistration.register(c.tx(), foreignReader);
            insertArchivePin(c.tx(), targetPin, reader, entry, binding.objectId());
            insertArchivePin(c.tx(), secondTargetPin, reader, entry, binding.objectId());
            insertArchivePin(c.tx(), foreignPin, foreignReader, entry, binding.objectId());
            assertPinAndMirror(c.tx(), targetPin, 1);
            assertPinAndMirror(c.tx(), foreignPin, 1);

            ReaderHostExecutions.fence(c.tx(), identity.execution());
            assertThat(new ArchiveReadRecovery(c.tx()).recover(10)).isZero();
            assertPinAndMirror(c.tx(), targetPin, 1);
            assertPinAndMirror(c.tx(), foreignPin, 1);

            var termination = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify));
            var proof = child.proof();
            child.stop();
            var hostReceipt = termination.record(identity, proof);
            assertThat(hostState(c.tx(), identity.execution())).isEqualTo("TERMINATED");
            assertThat(readerState(c.tx(), reader)).isEqualTo("ACTIVE");
            assertPinAndMirror(c.tx(), targetPin, 1);
            assertPinAndMirror(c.tx(), foreignPin, 1);
            assertThat(new ArchiveReadRecovery(c.tx()).recover(10)).isZero();

            assertThatThrownBy(() -> quiesce(c.tx(), reader, UUID.randomUUID(), identity.execution(), hostReceipt.id()))
                    .hasStackTraceContaining("registration mismatch");
            assertThat(readerState(c.tx(), reader)).isEqualTo("ACTIVE");
            assertThat(externalReceiptCount(c.tx(), reader)).isZero();

            UUID externalReceipt = quiesce(c.tx(), reader, registrationNonce, identity.execution(), hostReceipt.id());
            assertThat(externalReceipt).isNotNull();
            assertThat(quiesce(c.tx(), reader, registrationNonce, identity.execution(), hostReceipt.id()))
                    .isEqualTo(externalReceipt);
            assertThat(readerState(c.tx(), reader)).isEqualTo("QUIESCED");
            assertThat(quiescenceSource(c.tx(), reader)).isEqualTo("HOST_TERMINATION");
            assertThat(readerState(c.tx(), foreignReader)).isEqualTo("ACTIVE");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:reader)")
                        .setParameter("reader", reader).getSingleResult();
            })).hasStackTraceContaining("cannot be claimed as local drain");

            var recovery = new ArchiveReadRecovery(c.tx());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION reject_target_archive_release() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                          IF OLD.pin_id='%s'::uuid THEN RAISE EXCEPTION 'injected archive release failure'; END IF;
                          RETURN OLD;
                        END $$
                        """.formatted(targetPin)).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER reject_target_archive_release BEFORE DELETE ON archive_read_pins FOR EACH ROW EXECUTE FUNCTION reject_target_archive_release()")
                        .executeUpdate();
            });
            try {
                assertThatThrownBy(() -> recovery.recoverReaderBatch(reader, 2))
                        .hasStackTraceContaining("injected archive release failure");
                assertPinAndMirror(c.tx(), targetPin, 1);
                assertPinAndMirror(c.tx(), secondTargetPin, 0);
                assertPinAndMirror(c.tx(), foreignPin, 1);
            } finally {
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER reject_target_archive_release ON archive_read_pins").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION reject_target_archive_release()").executeUpdate();
                });
            }
            assertThat(recovery.recoverReaderBatch(reader, 1)).isEqualTo(1);
            assertThat(recovery.recoverReaderBatch(reader, 1)).isZero();
            assertPinAndMirror(c.tx(), targetPin, 0);
            assertPinAndMirror(c.tx(), foreignPin, 1);
            assertThat(readerState(c.tx(), foreignReader)).isEqualTo("ACTIVE");
            c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_version_object_refs WHERE object_id=:object")
                        .setParameter("object", binding.objectId()).executeUpdate();
            });
            var cleanup = new ai.protomolt.proto.repo.container.archive.ArchiveCleanupLedger(c.tx());
            assertThat(cleanup.claim(binding.objectId(), java.time.Instant.now().plusSeconds(1))).isEmpty();
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT fence_repository_reader(:reader)").setParameter("reader", foreignReader).getSingleResult();
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:reader)").setParameter("reader", foreignReader).getSingleResult();
            });
            assertThat(recovery.recoverReaderBatch(foreignReader, 1)).isEqualTo(1);
            assertThat(quiescenceSource(c.tx(), foreignReader)).isEqualTo("LOCAL_DRAIN");
            assertThat(cleanup.claim(binding.objectId(), java.time.Instant.now().plusSeconds(1))).isPresent();
        }
    }

    @Test void emptyRecoveryStillRequiresQuiescenceAndValidBounds() throws Exception {
        try (var c = context(POSTGRES)) {
            var reader = UUID.randomUUID(); ReaderRegistration.register(c.tx(), reader);
            var recovery = new ArchiveReadRecovery(c.tx());
            for (var id : java.util.List.of(reader, UUID.randomUUID()))
                assertThatThrownBy(() -> recovery.recoverReaderBatch(id, 1)).hasMessageContaining("proven reader quiescence");
            for (int limit : new int[]{0, -1, 1001})
                assertThatThrownBy(() -> recovery.recoverReaderBatch(reader, limit)).isInstanceOf(IllegalArgumentException.class);
            c.tx().inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult(); });
            assertThatThrownBy(() -> recovery.recoverReaderBatch(reader, 1)).hasMessageContaining("proven reader quiescence");
            c.tx().inTransaction(em -> { em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", reader).getSingleResult(); });
            assertThat(recovery.recoverReaderBatch(reader, 1)).isZero();
        }
    }

    private static void createArchiveVersion(Tx tx, UUID entry, UUID object) {
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state)
                    VALUES(:object,0,'text/plain',gen_random_uuid(),clock_timestamp()+interval '1 hour','STAGING')
                    """).setParameter("object", object).executeUpdate();
            em.createNativeQuery("UPDATE archive_object_uploads SET state='VERIFIED',sha256=:sha WHERE object_id=:object")
                    .setParameter("sha", "a".repeat(64)).setParameter("object", object).executeUpdate();
            em.createNativeQuery("UPDATE archive_object_uploads SET state='LIVE' WHERE object_id=:object")
                    .setParameter("object", object).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) VALUES(:entry,'account','external-quiescence',:entry,1)")
                    .setParameter("entry", entry).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes) VALUES(:entry,1,'{}','fixture-root',0)")
                    .setParameter("entry", entry).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES(:entry,1,:object)")
                    .setParameter("entry", entry).setParameter("object", object).executeUpdate();
        });
    }

    private static void insertArchivePin(Tx tx, UUID pin, UUID reader, UUID entry, UUID object) {
        tx.inTransaction(em -> {
            em.createNativeQuery("INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version) VALUES(:pin,:reader,:object,:entry,1)")
                    .setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object)
                    .setParameter("entry", entry).executeUpdate();
        });
    }

    private static UUID quiesce(Tx tx, UUID reader, UUID nonce, UUID host, UUID terminationReceipt) {
        return tx.inTransaction(em -> (UUID) em.createNativeQuery(
                "SELECT quiesce_repository_reader_from_host(:reader,:nonce,:host,:termination)")
                .setParameter("reader", reader).setParameter("nonce", nonce).setParameter("host", host)
                .setParameter("termination", terminationReceipt).getSingleResult());
    }

    private static void assertPinAndMirror(Tx tx, UUID pin, long expected) {
        long pins = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM archive_read_pins WHERE pin_id=:pin").setParameter("pin", pin).getSingleResult()).longValue());
        long mirrors = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id=:pin")
                .setParameter("pin", pin).getSingleResult()).longValue());
        assertThat(pins).isEqualTo(expected);
        assertThat(mirrors).isEqualTo(expected);
    }

    private static long externalReceiptCount(Tx tx, UUID reader) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_external_quiescence WHERE incarnation=:reader")
                .setParameter("reader", reader).getSingleResult()).longValue());
    }

    private static String readerState(Tx tx, UUID reader) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT state FROM repository_reader_incarnations WHERE incarnation=:reader")
                .setParameter("reader", reader).getSingleResult().toString());
    }

    private static String quiescenceSource(Tx tx, UUID reader) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT quiescence_source FROM repository_reader_incarnations WHERE incarnation=:reader")
                .setParameter("reader", reader).getSingleResult().toString());
    }

    private static String hostState(Tx tx, UUID execution) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT state FROM repository_reader_host_executions WHERE execution=:host")
                .setParameter("host", execution).getSingleResult().toString());
    }
}
