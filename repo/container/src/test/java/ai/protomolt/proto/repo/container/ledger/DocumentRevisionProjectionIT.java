package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real populated SQL migrations; fixture verification observations are explicitly synthetic. */
@Testcontainers
class DocumentRevisionProjectionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans={false,true})
    void revisionRetentionCutoverPreservesPinsAndSameRevisionUpserts(boolean revisionCascadeFirst) {
        try (var context=context("38")) {
            var first=publish(context,true,"before");
            var deleted=publish(context,false,"deleted");
            new DocumentLedger(context.tx).deleteByNodeId(deleted.row().nodeId);
            var before=references(context);
            context.migrate();
            assertThat(references(context)).containsExactlyElementsOf(before);
            execute(context,"UPDATE document_part_publications SET attempt_id=attempt_id");
            assertThat(references(context)).containsExactlyElementsOf(before);
            var next=publish(context,true,"after",first.row().readManifest().getAddress());
            assertThat(count(context,"repository_object_references")).isEqualTo(before.size()+2);
            long invalid=context.tx.readOnly(em -> ((Number)em.createNativeQuery("""
                    SELECT count(*) FROM repository_object_references
                    WHERE NOT repository_native_reference_exists(object_id,owner_kind,owner_id,owner_revision)
                    """).getSingleResult()).longValue());
            assertThat(invalid).isZero();
            assertThatThrownBy(() -> execute(context,"DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_HISTORY'"))
                    .hasStackTraceContaining("cannot release a retained native owner");
            if (revisionCascadeFirst) {
                // Deliberately reverse the native FK cascade order. The revision
                // mirror must not depend on the old pin acquiring origin locks first.
                execute(context,"""
                        DO $$ DECLARE trigger_name text; BEGIN
                         SELECT tgname INTO STRICT trigger_name FROM pg_trigger
                         WHERE tgrelid='documents'::regclass AND tgconstrrelid='document_revision_current'::regclass
                          AND tgfoid='"RI_FKey_cascade_del"()'::regprocedure;
                         EXECUTE format('ALTER TRIGGER %I ON documents RENAME TO "A0_revision_cascade_first"',trigger_name);
                        END $$
                        """);
            }
            new DocumentLedger(context.tx).deleteByNodeId(next.row().nodeId);
            assertThat(count(context,"repository_object_references")).isEqualTo(before.size());
            assertThat(count(context,"document_revision_current")).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void retentionCutoverRejectsMissingOrExtraReferencesAtomically(boolean extra) {
        try (var context=context("38")) {
            publish(context,true,"before");
            context.tx.inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_object_references DISABLE TRIGGER repository_reference_guard").executeUpdate();
                em.createNativeQuery(extra ? """
                        INSERT INTO repository_object_references
                        SELECT object_id,owner_kind,gen_random_uuid(),owner_revision
                        FROM repository_object_references LIMIT 1
                        """ : "DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_CURRENT'").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_object_references ENABLE TRIGGER repository_reference_guard").executeUpdate();
            });
            var damaged=references(context);
            assertThatThrownBy(context::migrate).hasStackTraceContaining("retention differs from existing references");
            assertThat(references(context)).containsExactlyElementsOf(damaged);
            long oldMirrors=context.tx.readOnly(em -> ((Number)em.createNativeQuery("""
                    SELECT count(*) FROM pg_trigger WHERE tgrelid='document_part_publications'::regclass
                    AND tgname='document_current_reference_mirror'
                    """).getSingleResult()).longValue());
            assertThat(oldMirrors).isEqualTo(1);
        }
    }

    private static java.util.List<String> references(Context context) {
        return context.tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT object_id::text || ':' || owner_kind || ':' || owner_id::text || ':' || owner_revision::text
                FROM repository_object_references ORDER BY object_id,owner_kind,owner_id,owner_revision
                """,String.class).getResultList());
    }

    @Test void backfillsSparseAndDeletedHistoryAndMirrorsLivePublicationWithoutNewRetentionPins() {
        try (var context = context()) {
            var retained = publish(context, true, "v1");
            var deleted = publish(context, false, null);
            new DocumentLedger(context.tx).deleteByNodeId(deleted.row().nodeId);
            long references = count(context, "repository_object_references");
            context.migrate();
            assertThat(count(context, "document_revision_publications")).isEqualTo(2);
            var read=new DocumentPublicationLedger(context.tx).findForRead(retained.row()).orElseThrow();
            assertThat(read.revisionId()).isEqualTo(retained.attempt());
            assertThat(read.parts()).extracting(DocumentPublicationLedger.Part::subKey)
                    .containsExactlyElementsOf(retained.slots().stream().map(DocumentPublicationSlot::getSubKey).toList());
            assertThat(read.boundParts()).allSatisfy(part -> assertThat(part.binding().namespace()).isEqualTo("namespace"));
            assertThat(count(context, "document_revision_parts")).isEqualTo(4);
            assertThat(count(context, "document_revision_current")).isEqualTo(1);
            assertThat(count(context, "repository_object_references")).isEqualTo(references);
            var positions = context.tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("SELECT revision_ordinal FROM document_revision_parts WHERE revision_id=:id ORDER BY revision_ordinal", Integer.class)
                    .setParameter("id", retained.attempt()).getResultList());
            assertThat(positions).containsExactly(1, 3);
            long matching = context.tx.readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM document_revision_publications r JOIN document_part_publication_history h
                     ON h.attempt_id=r.legacy_attempt_id WHERE r.body=h.body AND r.published_at=h.published_at
                    """).getSingleResult()).longValue());
            assertThat(matching).isEqualTo(2);
            var live = publish(context, true, "live-version");
            assertThat(count(context, "document_revision_publications")).isEqualTo(3);
            assertThat(count(context, "document_revision_current")).isEqualTo(2);
            var versions = context.tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                    SELECT o.provider_version FROM document_revision_parts r JOIN document_part_attempt_objects o
                     ON o.physical_object_id=r.object_id WHERE r.revision_id=:id ORDER BY r.revision_ordinal
                    """, String.class).setParameter("id", live.attempt()).getResultList());
            assertThat(versions).containsExactly("live-version", "live-version");
            new DocumentLedger(context.tx).deleteByNodeId(live.row().nodeId);
            assertThat(count(context, "document_revision_current")).isEqualTo(1);
            assertThat(count(context, "document_revision_publications")).isEqualTo(3);
        }
    }

    @Test void shadowCannotBeMutatedExtendedOrUnpinnedIndependently() {
        try (var context = context()) {
            var source = publish(context, false, "v1");
            context.migrate();
            assertThatThrownBy(() -> execute(context, "UPDATE document_revision_publications SET body='{}'"))
                    .hasStackTraceContaining("projection is immutable");
            assertThatThrownBy(() -> execute(context, "DELETE FROM document_revision_parts"))
                    .hasStackTraceContaining("parts are immutable");
            assertThatThrownBy(() -> execute(context, "DELETE FROM document_revision_current"))
                    .hasStackTraceContaining("cannot lose its current pin");
            assertThatThrownBy(() -> execute(context, """
                    INSERT INTO document_revision_parts SELECT revision_id,9999,part,sub_key,object_id FROM document_revision_parts LIMIT 1
                    """ )).hasStackTraceContaining("creation transaction");
            assertThatThrownBy(() -> execute(context, """
                    INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at)
                     VALUES(gen_random_uuid(),'%s',1,'{}',now())
                    """.formatted(source.row().nodeId))).hasStackTraceContaining("exact legacy history");
        }
    }

    @Test void missingManagedProjectionCannotFallBackToLegacyReadsOrSourceCapture() {
        try (var context=context()) {
            var source=publish(context,true,"v1"); context.migrate();
            context.tx.inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE document_revision_current DISABLE TRIGGER document_revision_current_guard").executeUpdate();
                em.createNativeQuery("DELETE FROM document_revision_current").executeUpdate();
                em.createNativeQuery("ALTER TABLE document_revision_current ENABLE TRIGGER document_revision_current_guard").executeUpdate();
            });
            assertThatThrownBy(() -> new DocumentPublicationLedger(context.tx).findForRead(source.row()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("projection disagrees");
            var drives=new DriveLedger(context.tx);
            var drive=drives.findByName(source.row().accountId,source.row().driveName).orElseThrow();
            assertThatThrownBy(() -> DocumentSourceSnapshot.legacy(context.tx,drives,source.row(),drive))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class).hasMessageContaining("projection differs");
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void flushedConstraintCannotPermitLatePartInsertionInCreationTransaction(boolean deferred) {
        try (var context = context()) {
            context.migrate();
            if (deferred) {
                execute(context,"""
                        CREATE FUNCTION defer_projection_proof() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN SET CONSTRAINTS document_revision_projection_complete DEFERRED; RETURN NEW; END;
                        $$
                        """);
                execute(context,"""
                        CREATE TRIGGER a00_defer_projection_proof BEFORE INSERT ON document_part_publication_history
                         FOR EACH ROW EXECUTE FUNCTION defer_projection_proof()
                        """);
            }
            execute(context,"""
                    CREATE FUNCTION inject_late_projection_part() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                     SET CONSTRAINTS document_revision_projection_complete IMMEDIATE;
                     INSERT INTO document_revision_parts
                      SELECT revision_id,9999,part,sub_key,object_id FROM document_revision_parts
                      WHERE revision_id=NEW.attempt_id LIMIT 1;
                     RETURN NEW;
                    END;
                    $$
                    """);
            execute(context,"""
                    CREATE TRIGGER zz_inject_late_projection_part AFTER INSERT ON document_part_publication_history
                     FOR EACH ROW EXECUTE FUNCTION inject_late_projection_part()
                    """);
            assertThatThrownBy(() -> publish(context,false,"v1")).hasStackTraceContaining("creation transaction");
            assertThat(count(context,"document_revision_publications")).isZero();
            assertThat(count(context,"document_part_publication_history")).isZero();
            assertThat(count(context,"document_revision_parts")).isZero();
        }
    }

    @Test void successiveRevisionsRetainOriginalHistoryAndSwitchOnlyCurrentPin() {
        try (var context = context()) {
            var first=publish(context,true,"first");
            var address=first.row().readManifest().getAddress();
            var second=publish(context,false,"second",address);
            context.migrate();
            var third=publish(context,true,"third",address);
            assertThat(count(context,"document_revision_publications")).isEqualTo(3);
            assertThat(count(context,"document_revision_current")).isEqualTo(1);
            Object current=context.tx.readOnly(em -> em.createNativeQuery("SELECT revision_id FROM document_revision_current").getSingleResult());
            assertThat(current).isEqualTo(third.attempt());
            long historical=context.tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_publications WHERE body->>'version_id' IN ('first','second')").getSingleResult()).longValue());
            assertThat(historical).isEqualTo(2);
            assertThat(second.row().readManifest().getDocVersion()).isEqualTo(2);
        }
    }

    @Test void largeSparseRevisionPreservesAllPositionsDuringBackfillAndLiveMirroring() {
        try (var context = context()) {
            var address=NodeAddress.newBuilder().setAccountId("account").setGraphId("graph")
                    .setGraphAddressId("node").setDocId(UUID.randomUUID().toString()).build();
            publish(context,true,"before",address,513);
            long migrationStarted=System.nanoTime();
            context.migrate();
            long migrationMillis=java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-migrationStarted);
            long publicationStarted=System.nanoTime();
            var current=publish(context,true,"after",address,513);
            long publicationMillis=java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-publicationStarted);
            long positions=context.tx.readOnly(em -> ((Number)em.createNativeQuery("""
                    SELECT count(*) FROM document_revision_parts WHERE revision_id=:id
                     AND (revision_ordinal=1 OR revision_ordinal BETWEEN 3 AND 514)
                    """).setParameter("id",current.attempt()).getSingleResult()).longValue());
            assertThat(positions).isEqualTo(513);
            assertThat(count(context,"document_revision_parts")).isEqualTo(1026);
            // Diagnostic only: includes fixture staging; not an isolated latency benchmark.
            System.out.printf("513-part sparse revision: migration=%dms, complete SQL fixture publication=%dms%n",migrationMillis,publicationMillis);
        }
    }

    @ParameterizedTest @ValueSource(strings={"part-hash","aggregate-size","core-version"})
    void inconsistentHistoricalManifestFailsMigrationAtomically(String kind) {
        try (var context = context()) {
            var source = publish(context, true, "v1");
            // Deliberate offline corruption: disable only the immutable history trigger,
            // restore it immediately, and prove migration refuses the damaged evidence.
            context.tx.inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE document_part_publication_history DISABLE TRIGGER document_publication_history_guard").executeUpdate();
                String path = switch(kind) {
                    case "aggregate-size" -> "{size_bytes}";
                    case "core-version" -> "{version_id}";
                    default -> "{part_manifest,parts,1,sha256}";
                };
                em.createNativeQuery("UPDATE document_part_publication_history SET body=jsonb_set(body,CAST(:path AS text[]),to_jsonb('99'::text)) WHERE attempt_id=:id")
                        .setParameter("path",path)
                        .setParameter("id", source.attempt()).executeUpdate();
                em.createNativeQuery("ALTER TABLE document_part_publication_history ENABLE TRIGGER document_publication_history_guard").executeUpdate();
            });
            assertThatThrownBy(context::migrate).hasStackTraceContaining("inconsistent managed evidence");
            Object relation = context.tx.readOnly(em -> em.createNativeQuery("SELECT to_regclass('document_revision_publications')::text").getSingleResult());
            assertThat(relation).isNull();
            assertThat(count(context, "document_part_publication_history")).isEqualTo(1);
        }
    }

    private static ManagedDocumentFixture publish(Context c, boolean sparse, String version) {
        var address=NodeAddress.newBuilder().setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(UUID.randomUUID().toString()).build();
        return publish(c,sparse,version,address);
    }
    private static ManagedDocumentFixture publish(Context c, boolean sparse, String version, NodeAddress address) {
        return publish(c,sparse,version,address,2);
    }
    private static ManagedDocumentFixture publish(Context c, boolean sparse, String version, NodeAddress address, int parts) {
        var drive = new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account";
        drive.name="drive-"+drive.driveId; drive.driveType="CUSTOM"; drive.provider="test-location"; drive.bucket="namespace"; drive.prefix="prefix";
        new DriveLedger(c.tx).insert(drive);
        var profile=new ManagedBackendLedger.Profile(new BackendIdentity("test-location","test-location/v1",Map.of("endpoint","synthetic")),"realm");
        String generation="generation-"+UUID.randomUUID(); new ManagedBackendLedger(c.tx).bind(generation,profile);
        return ManagedDocumentFixture.publish(c.tx,drive,generation,profile,address,DocumentSecurity.getDefaultInstance(),parts,1,version,sparse);
    }
    private static long count(Context c, String table) {
        return c.tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table).getSingleResult()).longValue());
    }
    private static void execute(Context c, String sql) { c.tx.inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }); }
    private record Context(HikariDataSource pool, EntityManagerFactory emf, Tx tx) implements AutoCloseable {
        void migrate() { Flyway.configure().dataSource(pool).locations("classpath:db/migration/repo").load().migrate(); }
        public void close() { try { emf.close(); } finally { pool.close(); } }
    }
    private static Context context() {
        return context("37");
    }
    private static Context context(String target) {
        String schema="revision_"+UUID.randomUUID().toString().replace("-","");
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target(target).load().migrate();
        var config=new HikariConfig(); config.setJdbcUrl(POSTGRES.getJdbcUrl()); config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword()); config.setSchema(schema); config.setMaximumPoolSize(3);
        var pool=new HikariDataSource(config);
        try {
            var emf=Persistence.createEntityManagerFactory("document-ledger",Map.of("hibernate.connection.datasource",pool,"hibernate.hbm2ddl.auto","validate"));
            return new Context(pool,emf,new Tx(emf));
        } catch(RuntimeException|Error failure) { pool.close(); throw failure; }
    }
}
