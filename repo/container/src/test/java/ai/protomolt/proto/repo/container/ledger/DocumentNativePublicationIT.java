package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationResultCodec;
import ai.protomolt.proto.repo.v1.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL publication proof using explicitly synthetic physical verification observations. No provider qualification. */
@Testcontainers
class DocumentNativePublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void zeroUploadMultiMemberPublicationRetainsExactResultsAndReads() throws Exception {
        try (var c = context()) {
            var prepared = prepare(c, 2);
            var result = publish(c, prepared, Fault.NONE, em -> {});
            assertThat(result.getMembersCount()).isEqualTo(2);
            assertThat(count(c, "document_part_publications")).isZero();
            assertThat(count(c, "document_revision_commits")).isEqualTo(2);
            assertThat(count(c, "document_revision_publications")).isEqualTo(4);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
            for (var member : result.getMembersList()) {
                var row = new DocumentLedger(c.tx).findByNodeId(
                        ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getAddress())).orElseThrow();
                var publication = new DocumentPublicationLedger(c.tx).findForRead(row).orElseThrow();
                assertThat(publication.revisionId().toString()).isEqualTo(member.getRevisionId());
                assertThat(publication.parts()).hasSize(2);
                assertThat(publication.parts().getFirst().providerVersion()).isEqualTo("fixture-version");
                assertThat(DocumentSourceSnapshot.bound(c.tx, row).publication()).isPresent();
            }
            var stored = c.tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT result_codec,result_version,result_bytes,encode(result_sha256,'hex'),owner_generation
                    FROM repository_operation_success
                    """).getSingleResult());
            assertThat(DocumentPublicationResultCodec.decode(prepared.command, "principal", ((Number) stored[4]).longValue(),
                    (String) stored[0], ((Number) stored[1]).intValue(), com.google.protobuf.ByteString.copyFrom((byte[]) stored[2]),
                    (String) stored[3])).isEqualTo(result);
        }
    }

    @Test void lateFailureAndMissingOutcomeRollBackEveryMember() {
        for (var fault : List.of(Fault.NONE, Fault.OMIT_OUTCOME)) try (var c = context()) {
            var prepared = prepare(c, 2);
            var failure=assertThatThrownBy(() -> publish(c, prepared, fault, em -> {
                if (fault==Fault.NONE) throw new IllegalStateException("Failure after outcome");
            }));
            if (fault==Fault.NONE) failure.hasMessage("Failure after outcome");
            else failure.hasStackTraceContaining("Independent document requires its exact committed revision");
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_events_outbox")).isZero();
            assertThat(count(c, "document_revision_publications")).isEqualTo(2);
            assertThat(count(c, "document_part_publications")).isEqualTo(2);
            for (var source : prepared.sources) {
                var restored = new DocumentLedger(c.tx).findByNodeId(source.row().nodeId).orElseThrow();
                assertThat(restored.mutationRevision).isEqualTo(source.row().mutationRevision);
            }
        }
    }

    @Test void oneOperationPublishesMixedAndZeroUploadMembers() {
        try (var c=context()) {
            var prepared=prepare(c,2,true);
            var result=publish(c,prepared,Fault.NONE,em -> {});
            var row=new DocumentLedger(c.tx).findByNodeId(prepared.sources.getFirst().row().nodeId).orElseThrow();
            var publication=new DocumentPublicationLedger(c.tx).findForRead(row).orElseThrow();
            assertThat(publication.parts()).hasSize(2);
            assertThat(publication.parts().get(0).providerVersion()).isEqualTo("fixture-version");
            assertThat(publication.parts().get(1).providerVersion()).isEqualTo("new-version");
            assertThat(publication.parts().get(1).key()).isEqualTo(prepared.uploads.get(0).key);
            assertThat(result.getMembersCount()).isEqualTo(2);
            assertThat(count(c,"document_revision_commits")).isEqualTo(2);
        }
    }

    @Test void terminalSuccessBlocksOwnerPathsButPreservesRecovery() {
        try (var c = context()) {
            var prepared = prepare(c, 1);
            publish(c, prepared, Fault.NONE, em -> {});
            var operations = new RepositoryOperationLedger(c.tx);
            assertThatThrownBy(() -> operations.admit(prepared.owner.key(), prepared.command,
                    prepared.owner.token(), Duration.ofMinutes(5))).isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.renew(prepared.owner, Duration.ofMinutes(5)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.takeOver(prepared.owner.key(), 1, UUID.randomUUID(), Duration.ofMinutes(5)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> c.tx.inTransaction(em -> { RepositoryOperationLedger.fenceLiveOwner(em, prepared.owner); }))
                    .hasStackTraceContaining("Repository operation is terminal");
            c.tx.inTransaction(em -> {
                assertThat(em.createNativeQuery("SELECT fence_repository_operation_recovery(:a,:p,:o)")
                        .setParameter("a", prepared.owner.key().account()).setParameter("p", "principal")
                        .setParameter("o", prepared.owner.key().operationId()).getSingleResult()).isEqualTo(1L);
            });
        }
    }

    @Test void nativeStorageKeyRequiresPublicationForTheSameNode() {
        try (var c=context()) {
            var prepared=prepare(c,2,true);
            var key=prepared.uploads.get(0).key;
            var owner=prepared.sources.getFirst().row().nodeId;
            // Verification alone does not make the selected upload a published object.
            assertThatThrownBy(() -> checkStorageKey(c,owner,key))
                    .hasStackTraceContaining("Unknown repository location conflicts with a managed key");
            publish(c,prepared,Fault.NONE,em -> {});
            checkStorageKey(c,owner,key);
            assertThatThrownBy(() -> checkStorageKey(c,prepared.sources.get(1).row().nodeId,key))
                    .hasStackTraceContaining("Unknown repository location conflicts with a managed key");
        }
    }

    @Test void linkedPendingEventCanBeDeliveredButCannotBeRemoved() {
        try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,Fault.NONE,true,em -> {});
            assertThatThrownBy(() -> c.tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM document_events_outbox").executeUpdate();
            })).hasStackTraceContaining("Document publication event is retained by its revision");
            c.tx.inTransaction(em -> {
                assertThat(em.createNativeQuery("""
                        UPDATE document_events_outbox SET status='PUBLISHED',attempts=1,published_at=clock_timestamp()
                        WHERE status='PENDING'
                        """).executeUpdate()).isEqualTo(1);
            });
            assertThat(count(c,"document_events_outbox")).isEqualTo(1);
            assertThat(count(c,"repository_operation_success")).isEqualTo(1);
        }
    }

    private static void checkStorageKey(Context c,UUID node,String key) {
        c.tx.inTransaction(em -> {
            em.createNativeQuery("SELECT quarantine_document_location_keys(:node,jsonb_build_array(CAST(:key AS text)))")
                    .setParameter("node",node).setParameter("key",key).getSingleResult();
        });
    }

    @Test void immediateChecksRejectIncompleteOutcomeAndEventSets() {
        for (var fault : List.of(Fault.OMIT_EVENT,Fault.WRONG_COUNT)) try (var c=context()) {
            var prepared=prepare(c,2);
            assertThatThrownBy(() -> publish(c,prepared,fault,em -> {}))
                    .hasStackTraceContaining(fault==Fault.WRONG_COUNT ? "Repository result member set is incomplete" : "event");
            assertThat(count(c,"document_revision_commits")).isZero();
            assertThat(count(c,"repository_operation_success")).isZero();
            assertThat(count(c,"document_events_outbox")).isZero();
            assertThat(count(c,"document_part_publications")).isEqualTo(2);
        }
    }

    @Test void terminalWritesCannotBypassAlreadyImmediateConstraints() {
        for (String statement : List.of(
                "UPDATE document_revision_current SET revision_id=revision_id",
                "UPDATE documents SET filename='changed after result'",
                "DELETE FROM documents",
                "UPDATE document_events_outbox SET attempts=1",
                "UPDATE repository_operation_owners SET lease_until=lease_until+interval '1 second'")) {
            try (var c=context()) {
                var prepared=prepare(c,1);
                assertThatThrownBy(() -> publish(c,prepared,Fault.NONE,em -> em.createNativeQuery(statement).executeUpdate()))
                        .hasStackTraceContaining("terminal");
                assertThat(count(c,"repository_operation_success")).isZero();
                assertThat(count(c,"document_revision_commits")).isZero();
                assertThat(count(c,"document_part_publications")).isEqualTo(1);
            }
        }
    }

    @Test void laterPolicyChangeDoesNotRewriteAdmittedMetadata() {
        try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,Fault.NONE,em -> {});
            Object original=c.tx.readOnly(em -> em.createNativeQuery("SELECT metadata_snapshot::text FROM document_revision_commits").getSingleResult());
            c.tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                    .setParameter("policy", "{\"inheritanceEnabled\":true}").executeUpdate(); });
            Object retained=c.tx.readOnly(em -> em.createNativeQuery("SELECT metadata_snapshot::text FROM document_revision_commits").getSingleResult());
            assertThat(retained).isEqualTo(original);
        }
    }

    @Test void metadataSnapshotDoesNotDependOnSessionTimezone() {
        try (var c=context()) {
            var prepared=prepare(c,1);
            c.tx.inTransaction(em -> {
                em.createNativeQuery("SET LOCAL TIME ZONE 'UTC'").executeUpdate();
                Object utc=em.createNativeQuery("SELECT document_revision_metadata_v1(d)::text FROM documents d")
                        .getSingleResult();
                em.createNativeQuery("SET LOCAL TIME ZONE 'America/New_York'").executeUpdate();
                Object local=em.createNativeQuery("SELECT document_revision_metadata_v1(d)::text FROM documents d")
                        .getSingleResult();
                assertThat(local).isEqualTo(utc);
            });
        }
    }

    private record NewPart(UUID attempt, UUID object, String key) {}
    private record Prepared(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
                            List<ManagedDocumentFixture> sources, Map<Integer,NewPart> uploads) {}

    private static Prepared prepare(Context c, int size) {
        return prepare(c,size,false);
    }

    private static Prepared prepare(Context c, int size, boolean mixed) {
        var profile = new ManagedBackendLedger.Profile(new ai.protomolt.proto.repo.blob.spi.BackendIdentity(
                "test-location", "test-location/v1", Map.of("endpoint", "synthetic")), "native-test");
        new ManagedBackendLedger(c.tx).bind("native-test", profile);
        var drive = new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account";
        drive.name="native"; drive.bucket="bucket"; drive.prefix="root"; drive.provider="test-location";
        drive.driveType="CUSTOM"; drive.status="ACTIVE";
        new DriveLedger(c.tx).insert(drive);
        var intent = DocumentPublicationIntent.newBuilder().setOperationId(UUID.randomUUID().toString())
                .setEncodingVersion(1).setAccountId("account");
        var sources = new ArrayList<ManagedDocumentFixture>();
        for (int i=0;i<size;i++) {
            var address=NodeAddress.newBuilder().setAccountId("account").setDocId("doc-"+i)
                    .setGraphId("graph").setGraphAddressId("node").build();
            var source=ManagedDocumentFixture.publish(c.tx, drive, "native-test", profile, address,
                    DocumentSecurity.getDefaultInstance(), 2, 5, "fixture-version");
            sources.add(source);
            var condition=DocumentRevisionCondition.newBuilder().setAddress(address)
                    .setExpectedMutationRevision(source.row().mutationRevision).build();
            var member=DocumentPublicationMember.newBuilder().setMemberId("member-"+i).setDriveId(drive.driveId.toString())
                    .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).setDestination(condition)
                    .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                            .setSecurity(DocumentSecurity.getDefaultInstance()));
            for (int j=0;j<source.slots().size();j++) {
                var part=DocumentPublicationPart.newBuilder().setSlot(source.slots().get(j));
                if (mixed && i==0 && j==1) part.setUpload(PublicationUpload.newBuilder().setSizeBytes(1)
                        .setSha256("a".repeat(64)).setContentType("application/protobuf"));
                else part.setReuse(PublicationReuse.newBuilder().setSource(condition)
                        .setSourceSlot(source.slots().get(j)).setObject(source.identities().get(j)));
                member.addParts(part);
            }
            intent.addMembers(member);
        }
        var command=new DocumentPublicationCommand(intent.build());
        var key=new RepositoryOperationLedger.Key("account","principal",command.operationId());
        var owner=new RepositoryOperationLedger(c.tx).admit(key, command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
        var placements=Map.of(drive.driveId,DocumentUploadPlan.Placement.sample(drive,"native-test",profile));
        Map<String,UUID> attempts=mixed ? Map.of("member-0",UUID.randomUUID()) : Map.of();
        var admitted=new DocumentOperationUploadAdmission(c.tx,new DriveLedger(c.tx)).admit(
                new ai.protomolt.proto.repo.spi.RepositoryCaller("principal",true),owner,
                DocumentOperationUploadAdmission.prepare(command,placements,attempts,Duration.ofMinutes(5)));
        var uploads=new java.util.HashMap<Integer,NewPart>();
        if (mixed) {
            var upload=DocumentUploadPlan.prepare(command,placements,attempts).members().getFirst().attempt().orElseThrow().uploads().getFirst().object();
            var attempt=admitted.getFirst();
            // Synthetic observations exercise the real selected-attempt SQL boundary.
            new DocumentSelectedAttemptLedger(c.tx).verifyBatch(owner,
                    new DocumentSelectedAttemptLedger.Selected("member-0",1,attempt.id(),attempt.token()),
                    List.of(new DocumentSelectedAttemptLedger.Observation(upload.objectKey(),upload.size(),upload.sha256(),
                            upload.contentType(),"new-version","new-etag")));
            UUID object=c.tx.readOnly(em -> (UUID)em.createNativeQuery(
                    "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id AND ordinal=0")
                    .setParameter("id",attempt.id()).getSingleResult());
            uploads.put(0,new NewPart(attempt.id(),object,upload.objectKey()));
        }
        return new Prepared(command, owner, List.copyOf(sources),Map.copyOf(uploads));
    }

    /** Exercises SQL directly, not a substitute for the forthcoming production publisher/content validation. */
    private enum Fault { NONE, OMIT_OUTCOME, OMIT_EVENT, WRONG_COUNT }

    private static DocumentPublicationResult publish(Context c, Prepared p, Fault fault, Consumer<EntityManager> beforeCommit) {
        return publish(c,p,fault,false,beforeCommit);
    }

    private static DocumentPublicationResult publish(Context c, Prepared p, Fault fault, boolean deliver,
                                                      Consumer<EntityManager> beforeCommit) {
        return c.tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,p.owner);
            var destinations=p.sources.stream().map(s -> s.row().nodeId).collect(java.util.stream.Collectors.toSet());
            DocumentRevisionLocks.lock(em,destinations,Set.of());
            var objects=new java.util.HashSet<UUID>();
            for (int i=0;i<p.sources.size();i++) for (int j=0;j<p.sources.get(i).identities().size();j++)
                objects.add(j==1 && p.uploads.containsKey(i) ? p.uploads.get(i).object : UUID.fromString(p.sources.get(i).identities().get(j).getObjectId()));
            var selected=p.uploads.values().stream().map(NewPart::attempt).collect(java.util.stream.Collectors.toSet());
            var locks=DocumentPublicationLocks.lockIndependentOrigins(em,destinations,objects,selected);
            DocumentPublicationLocks.lockIndependentRetention(em,locks);
            var result=DocumentPublicationResult.newBuilder().setOperationId(p.command.operationId().toString()).setAccountId("account")
                    .setCommandEncodingVersion(1).setCommandSha256(p.command.sha256()).setPrincipal("principal").setOwnerGeneration(p.owner.generation());
            for (int i=0;i<p.sources.size();i++) {
                var source=p.sources.get(i); UUID revision=UUID.randomUUID();
                var manifest=source.row().readManifest().toBuilder().setDocVersion(source.row().readManifest().getDocVersion()+1);
                if (p.uploads.containsKey(i)) manifest.setParts(1,manifest.getParts(1).toBuilder().setObjectKey(p.uploads.get(i).key));
                em.createNativeQuery("""
                        UPDATE documents SET filename='native revision',part_manifest=CAST(:manifest AS jsonb) WHERE node_id=:node
                        """).setParameter("manifest",ai.protomolt.proto.repo.codec.DocumentPartCodec.manifestToJson(manifest.build()))
                        .setParameter("node",source.row().nodeId).executeUpdate();
                var row=em.find(DocumentRecord.class,source.row().nodeId); em.refresh(row);
                var event=deliver ? DocumentEventFactory.saved(row,Instant.now())
                        : DocumentEventFactory.savedWithoutDelivery(row,Instant.now());
                em.createNativeQuery("""
                        INSERT INTO document_revision_commits(revision_id,node_id,publication_revision,account_id,principal,operation_id,
                         owner_generation,member_id,member_ordinal,selection_revision,event_id,metadata_version,admission_mode)
                        VALUES(:revision,:node,:mutation,'account','principal',:operation,:generation,:member,:ordinal,1,:event,1,'OPAQUE')
                        """).setParameter("revision",revision).setParameter("node",row.nodeId).setParameter("mutation",row.mutationRevision)
                        .setParameter("operation",p.owner.key().operationId()).setParameter("generation",p.owner.generation())
                        .setParameter("member","member-"+i).setParameter("ordinal",i).setParameter("event",event.eventId).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at,native_binding)
                        SELECT :revision,node_id,mutation_revision,document_publication_body(documents),clock_timestamp(),:revision
                        FROM documents WHERE node_id=:node
                        """).setParameter("revision",revision).setParameter("node",row.nodeId).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO document_revision_parts SELECT :revision,revision_ordinal,part,sub_key,object_id
                        FROM document_revision_parts WHERE revision_id=:prior AND revision_ordinal<>:replaced
                        """).setParameter("revision",revision).setParameter("prior",source.attempt())
                        .setParameter("replaced",p.uploads.containsKey(i) ? 1 : -1).executeUpdate();
                if (p.uploads.containsKey(i)) em.createNativeQuery("""
                        INSERT INTO document_revision_parts(revision_id,revision_ordinal,part,sub_key,object_id)
                        SELECT :revision,revision_ordinal,part,sub_key,physical_object_id FROM document_part_attempt_objects WHERE physical_object_id=:object
                        """).setParameter("revision",revision).setParameter("object",p.uploads.get(i).object).executeUpdate();
                em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:revision")
                        .setParameter("revision",revision).executeUpdate();
                em.createNativeQuery("UPDATE document_revision_current SET revision_id=:revision WHERE node_id=:node")
                        .setParameter("revision",revision).setParameter("node",row.nodeId).executeUpdate();
                em.createNativeQuery("DELETE FROM document_part_publications WHERE node_id=:node").setParameter("node",row.nodeId).executeUpdate();
                if (fault!=Fault.OMIT_EVENT || i!=p.sources.size()-1) em.persist(event);
                result.addMembers(DocumentPublishedRevision.newBuilder().setMemberId("member-"+i)
                        .setAddress(row.readManifest().getAddress()).setRevisionId(revision.toString()).setMutationRevision(row.mutationRevision));
            }
            em.flush();
            var success=result.build();
            var encoded=DocumentPublicationResultCodec.encode(p.command,success,"principal",p.owner.generation());
            if (fault!=Fault.OMIT_OUTCOME) em.createNativeQuery("""
                    INSERT INTO repository_operation_success(account_id,principal,operation_id,owner_generation,
                     command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,member_count)
                    SELECT account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                     'document-publication-result',1,:result,:digest,:count FROM repository_operations WHERE operation_id=:operation
                    """).setParameter("generation",p.owner.generation()).setParameter("result",encoded.bytes().toByteArray())
                    .setParameter("digest",java.util.HexFormat.of().parseHex(encoded.sha256()))
                    .setParameter("count",success.getMembersCount()+(fault==Fault.WRONG_COUNT ? 1 : 0))
                    .setParameter("operation",p.owner.key().operationId()).executeUpdate();
            em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
            beforeCommit.accept(em);
            return success;
        });
    }

    private static long count(Context c,String table) { return c.tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table).getSingleResult()).longValue()); }
    private record Context(HikariDataSource pool, EntityManagerFactory emf, Tx tx) implements AutoCloseable {
        public void close() { try { emf.close(); } finally { pool.close(); } }
    }
    private static Context context() {
        String schema="native_"+UUID.randomUUID().toString().replace("-","");
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
        var config=new HikariConfig(); config.setJdbcUrl(POSTGRES.getJdbcUrl()); config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword()); config.setSchema(schema); config.setMaximumPoolSize(3);
        var pool=new HikariDataSource(config);
        try {
            var emf=Persistence.createEntityManagerFactory("document-ledger",Map.of("hibernate.connection.datasource",pool,"hibernate.hbm2ddl.auto","validate"));
            return new Context(pool,emf,new Tx(emf));
        } catch(RuntimeException|Error failure) { pool.close(); throw failure; }
    }
}
