package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.member;
import static org.assertj.core.api.Assertions.*;

/** Real SQL/admission, with fixture-supplied source provider observations. */
@Testcontainers
class DocumentPreparationMultiRootReleaseIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final Duration LEASE=Duration.ofMinutes(5);

    @Test void partialDeletionRollsBackAndCompleteReleasePreservesOtherPreparation() throws Exception {
        try (var c=context(POSTGRES); var original=historicalInitial(c,LEASE)) {
            var document=Document.newBuilder().setDocId("second-source").setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                    .setStructuredData(Any.pack(StringValue.of("second payload"),"type.test")).build();
            var producer=DocumentSchemaRetentionFixture.prepare(c,true,false,document,"second-source","second-drive");
            var second=new DocumentHistoricalRestoreAssessmentIT.Fixture(producer,DocumentSchemaRetentionFixture.publishBound(c,producer,
                    (em,candidate) -> {},(em,id,manifest) -> producer.retention().write(em,producer.owner(),id,() -> {})));
            var history=original.reads().captureHistorical(CALLER,second.address(),second.revision());
            try {
                var command=new DocumentPublicationCommand(original.record().command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).addMembers(member(second,history).toBuilder().setMemberId("second")).build());
                var placements=new HashMap<>(original.record().placements());
                var secondPlacement=producer.prepared().members().getFirst().placement();
                placements.put(secondPlacement.drive().id(),secondPlacement);
                var key=new RepositoryOperationLedger.Key("account","principal",command.operationId());
                var record=new DocumentPublicationPreparationRecord(key,command,DocumentPublicationSeeds.mint(key,command),placements,LEASE,0);
                try (var sources=DocumentHistoricalAssessmentSources.open(command,CALLER,List.of(original.history(),history),NONE)) {
                    var pins=DocumentPreparationSourcePins.prepare(command,sources.references(command,() -> {}),() -> {});
                    var coordinator=UUID.randomUUID();
                    var claim=c.tx().inTransaction(em -> {
                        var acquisition=RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(em,key,command,UUID.randomUUID(),LEASE,sources);
                        RepositoryCoordinatorBinding.bindInitial(em,acquisition,coordinator);
                        var bytes=DocumentPublicationPreparationCodec.encode(record);
                        DocumentPublicationPreparationJournal.insert(em,acquisition.claim(),record,bytes,
                                DocumentPublicationPreparationJournal.digest(bytes),sources.references(command,() -> {}));
                        var objects=pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
                        var nodes=pins.pins().stream().map(DocumentHistoricalSourcePin::node).collect(java.util.stream.Collectors.toSet());
                        var locked=DocumentPublicationLocks.lockIndependentOrigins(em,nodes,objects,Set.of());
                        DocumentPublicationLocks.lockIndependentRetention(em,locked);
                        DocumentPreparationSourcePins.insert(em,record,pins,acquisition.claim(),coordinator,() -> {});
                        return acquisition.claim();
                    });
                    assertThat(DocumentPreparationHistoryRoots.roots(command)).hasSize(2);
                    DocumentPublicationAbandonment.abandon(c.tx(),original.budget(),CALLER,claim,record,NONE);
                    sources.close(); original.sources().close(); history.close(); history.release();
                    original.history().close(); original.history().release(); original.reads().fence(); original.reads().attestLocalQuiescence();
                    var identity=new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(key,command.sha256(),
                            claim.epoch(),claim.token(),coordinator),0,HexFormat.of().formatHex(pins.digest()));
                    DocumentPreparationCaptureDrain.recover(c.tx(),CALLER,identity,NONE);
                    assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                        insert(em,record);
                        int deleted=em.createNativeQuery("""
                                DELETE FROM repository_preparation_history_roots WHERE operation_id=:o AND node_id=:node
                                """).setParameter("o",command.operationId()).setParameter("node",DocumentIds.nodeId(second.address())).executeUpdate();
                        assertThat(deleted).isEqualTo(1);
                    })).hasStackTraceContaining("remove every target root atomically");
                    assertThat(rows(c,"repository_preparation_history_roots",key)).isEqualTo(2);
                    assertThat(rows(c,"repository_preparation_root_releases",key)).isZero();
                    var receipt=DocumentPreparationRootReleases.release(c.tx(),original.budget(),CALLER,record,NONE);
                    assertThat(receipt.rootCount()).isEqualTo(2);
                    assertThat(receipt.captureCount()).isEqualTo(1);
                    assertThat(DocumentPreparationRootReleases.release(c.tx(),original.budget(),CALLER,record,NONE)).isEqualTo(receipt);
                    assertThat(rows(c,"repository_preparation_history_roots",key)).isZero();
                    assertThat(rows(c,"repository_preparation_root_releases",key)).isEqualTo(1);
                    assertThat(rows(c,"repository_preparation_history_roots",original.record().key())).isEqualTo(1);
                    assertThat(rows(c,"repository_preparation_root_releases",original.record().key())).isZero();
                    var live=c.tx().readOnly(em -> DocumentPreparationHistoryRoots.coverage(em,original.record(),
                            DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(original.record()))));
                    assertThat(live).isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
                }
            } finally { history.close(); history.release(); }
        }
    }

    private static void insert(jakarta.persistence.EntityManager em,DocumentPublicationPreparationRecord record) {
        assertThat(em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                 terminal_kind,abandonment_token,abandonment_nonce)
                SELECT h.account_id,h.principal,h.operation_id,h.predecessor_generation,h.preparation_sha256,h.command_sha256,
                 h.expected_count,h.roots_sha256,1,
                 repository_preparation_capture_fingerprint(h.account_id,h.principal,h.operation_id,h.predecessor_generation),
                 'ABANDONMENT',a.claim_token,a.owner_nonce
                FROM repository_preparation_history_sets h JOIN repository_publication_abandonments a USING(account_id,principal,operation_id)
                WHERE h.operation_id=:o AND h.predecessor_generation=0
                """).setParameter("o",record.command().operationId()).executeUpdate()).isEqualTo(1);
    }
    private static long rows(Context c,String table,RepositoryOperationLedger.Key key) {
        return c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:o")
                .setParameter("o",key.operationId()).getSingleResult()).longValue());
    }
}
