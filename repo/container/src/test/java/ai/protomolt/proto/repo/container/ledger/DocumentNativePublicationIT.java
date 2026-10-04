package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationResultCodec;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;

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
                var row = new DocumentLedger(c.tx()).findByNodeId(
                        ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getAddress())).orElseThrow();
                var publication = new DocumentPublicationLedger(c.tx()).findForRead(row).orElseThrow();
                assertThat(publication.revisionId().toString()).isEqualTo(member.getRevisionId());
                assertThat(publication.parts()).hasSize(2);
                assertThat(publication.parts().getFirst().providerVersion()).isEqualTo("fixture-version");
                assertThat(DocumentSourceSnapshot.bound(c.tx(), row).publication()).isPresent();
            }
            var stored = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT result_codec,result_version,result_bytes,encode(result_sha256,'hex'),owner_generation
                    FROM repository_operation_success
                    """).getSingleResult());
            assertThat(DocumentPublicationResultCodec.decode(prepared.command(), "principal", ((Number) stored[4]).longValue(),
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
            for (var source : prepared.sources()) {
                var restored = new DocumentLedger(c.tx()).findByNodeId(source.row().nodeId).orElseThrow();
                assertThat(restored.mutationRevision).isEqualTo(source.row().mutationRevision);
            }
        }
    }

    @Test void oneOperationPublishesMixedAndZeroUploadMembers() {
        try (var c=context()) {
            var prepared=prepare(c,2,true);
            var result=publish(c,prepared,Fault.NONE,em -> {});
            var row=new DocumentLedger(c.tx()).findByNodeId(prepared.sources().getFirst().row().nodeId).orElseThrow();
            var publication=new DocumentPublicationLedger(c.tx()).findForRead(row).orElseThrow();
            assertThat(publication.parts()).hasSize(2);
            assertThat(publication.parts().get(0).providerVersion()).isEqualTo("fixture-version");
            assertThat(publication.parts().get(1).providerVersion()).isEqualTo("new-version");
            assertThat(publication.parts().get(1).key()).isEqualTo(prepared.uploads().get(0).key());
            assertThat(result.getMembersCount()).isEqualTo(2);
            assertThat(count(c,"document_revision_commits")).isEqualTo(2);
        }
    }

    @Test void terminalSuccessBlocksOwnerPathsButPreservesRecovery() {
        try (var c = context()) {
            var prepared = prepare(c, 1);
            publish(c, prepared, Fault.NONE, em -> {});
            var operations = new RepositoryOperationLedger(c.tx());
            assertThatThrownBy(() -> operations.admit(prepared.owner().key(), prepared.command(),
                    prepared.owner().token(), Duration.ofMinutes(5))).isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.renew(prepared.owner(), Duration.ofMinutes(5)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.takeOver(prepared.owner().key(), 1, UUID.randomUUID(), Duration.ofMinutes(5)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { RepositoryOperationLedger.fenceLiveOwner(em, prepared.owner()); }))
                    .hasStackTraceContaining("Repository operation is terminal");
            c.tx().inTransaction(em -> {
                assertThat(em.createNativeQuery("SELECT fence_repository_operation_recovery(:a,:p,:o)")
                        .setParameter("a", prepared.owner().key().account()).setParameter("p", "principal")
                        .setParameter("o", prepared.owner().key().operationId()).getSingleResult()).isEqualTo(1L);
            });
        }
    }

    @Test void nativeStorageKeyRequiresPublicationForTheSameNode() {
        try (var c=context()) {
            var prepared=prepare(c,2,true);
            var key=prepared.uploads().get(0).key();
            var owner=prepared.sources().getFirst().row().nodeId;
            // Verification alone does not make the selected upload a published object.
            assertThatThrownBy(() -> checkStorageKey(c,owner,key))
                    .hasStackTraceContaining("Unknown repository location conflicts with a managed key");
            publish(c,prepared,Fault.NONE,em -> {});
            checkStorageKey(c,owner,key);
            assertThatThrownBy(() -> checkStorageKey(c,prepared.sources().get(1).row().nodeId,key))
                    .hasStackTraceContaining("Unknown repository location conflicts with a managed key");
        }
    }

    @Test void linkedPendingEventCanBeDeliveredButCannotBeRemoved() {
        try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,Fault.NONE,true,em -> {});
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM document_events_outbox").executeUpdate();
            })).hasStackTraceContaining("Document publication event is retained by its revision");
            c.tx().inTransaction(em -> {
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
        c.tx().inTransaction(em -> {
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
            Object original=c.tx().readOnly(em -> em.createNativeQuery("SELECT metadata_snapshot::text FROM document_revision_commits").getSingleResult());
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                    .setParameter("policy", "{\"inheritanceEnabled\":true}").executeUpdate(); });
            Object retained=c.tx().readOnly(em -> em.createNativeQuery("SELECT metadata_snapshot::text FROM document_revision_commits").getSingleResult());
            assertThat(retained).isEqualTo(original);
        }
    }

    @Test void metadataSnapshotDoesNotDependOnSessionTimezone() {
        try (var c=context()) {
            var prepared=prepare(c,1);
            c.tx().inTransaction(em -> {
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

    private static Context context() { return DocumentNativePublicationFixture.context(POSTGRES); }
}
