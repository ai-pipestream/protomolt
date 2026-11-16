package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.external.v1.ExternalAttemptTrigger;
import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * What a settled receipt does to the ledger's links: a deletion retires only the object it
 * deleted, and a publication that lands after the source was deleted is recorded as still live.
 */
class ExternalReceiptSettlementTest {

    private static final Timestamp T0 = Timestamp.newBuilder().setSeconds(1_770_000_000L).build();
    private static final Timestamp T1 = Timestamp.newBuilder().setSeconds(1_770_000_060L).build();
    private static final NodeAddress SOURCE = NodeAddress.newBuilder()
            .setAccountId("tenant-alpha").setGraphId("g").setGraphAddressId("ga").setDocId("doc-1")
            .build();

    private static ExternalSystemBinding binding(long revision) {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("rust-search").setAccountId("tenant-alpha")
                .setBindingRevision(revision).build();
    }

    private static ExternalLink link(String objectId, long bindingRevision, long sourceRevision,
            ExternalProjectionLifecycle lifecycle) {
        return ExternalLink.newBuilder()
                .setBindingId("rust-search").setBindingRevision(bindingRevision)
                .setBindingSnapshot(binding(bindingRevision))
                .setExternalObjectId(objectId).setSourceAddress(SOURCE)
                .setSourceRevision(sourceRevision)
                .setConfirmedByOperationId("op-publish-" + objectId)
                .setProjectionLifecycle(lifecycle).build();
    }

    private static ExternalPublicationOperationRecord operation(String operationId,
            ExternalPublicationAction action, String objectId, long bindingRevision,
            long sourceRevision) {
        return ExternalPublicationOperationRecord.newBuilder()
                .setOperationId(operationId)
                .setIntent(ExternalPublicationIntent.newBuilder()
                        .setAccountId("tenant-alpha").setSourceAddress(SOURCE)
                        .setSourceRevision(sourceRevision).setBindingId("rust-search")
                        .setBindingRevision(bindingRevision).setAction(action)
                        .setExternalObjectId(objectId))
                .setBindingSnapshot(binding(bindingRevision))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .build();
    }

    private static ExternalProviderEvidence evidence(ExternalProviderSettlement settlement,
            String objectId) {
        return ExternalProviderEvidence.newBuilder()
                .setSettlement(settlement).setExternalObjectId(objectId).build();
    }

    private static ExternalPublicationLedger deleted(ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord deleteOp) {
        return ExternalReceiptSettlement.settleConfirmedDeletion(
                ledger, deleteOp, 1, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL,
                T0, T1, true,
                evidence(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_DELETED,
                        deleteOp.getIntent().getExternalObjectId()));
    }

    private static ExternalPublicationLedger published(ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord publishOp) {
        return ExternalReceiptSettlement.settleConfirmedPublication(
                ledger, publishOp, 1, ExternalAttemptTrigger.EXTERNAL_ATTEMPT_TRIGGER_INITIAL,
                T0, T1, true, publishOp.getIntent(), publishOp.getBindingSnapshot(),
                evidence(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED,
                        publishOp.getIntent().getExternalObjectId()));
    }

    @Test
    void aDeletionRetiresTheCurrentLinkOnlyWhenItIsTheObjectThatWasDeleted() {
        ExternalPublicationOperationRecord deleteB = operation("op-del-b",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-b", 1L, 7L);
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-a", 1L, 5L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addOperations(deleteB)
                .build();

        ExternalPublicationLedger afterB = deleted(ledger, deleteB);
        // Object A was not deleted: it is still the current, externally live projection.
        assertThat(afterB.hasCurrentLink()).isTrue();
        assertThat(afterB.getCurrentLink().getExternalObjectId()).isEqualTo("object-a");
        assertThat(afterB.getCurrentLink().hasDeletionEvidence()).isFalse();
        assertThat(afterB.getHistoricalLinksList()).isEmpty();
        assertThat(ExternalLedgerStore.findOperation(afterB, "op-del-b").orElseThrow().getOutcome())
                .isEqualTo(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED);

        // A different binding revision is a different dataset: the same ID there is not A.
        ExternalPublicationOperationRecord deleteAUnderV2 = operation("op-del-a-v2",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 2L, 7L);
        ExternalPublicationLedger afterV2 = deleted(
                ledger.toBuilder().addOperations(deleteAUnderV2).build(), deleteAUnderV2);
        assertThat(afterV2.getCurrentLink().getExternalObjectId()).isEqualTo("object-a");
        assertThat(afterV2.getHistoricalLinksList()).isEmpty();

        // Deleting A under the binding revision that published it retires it, with evidence.
        ExternalPublicationOperationRecord deleteA = operation("op-del-a",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 7L);
        ExternalPublicationLedger afterA = deleted(
                ledger.toBuilder().addOperations(deleteA).build(), deleteA);
        assertThat(afterA.hasCurrentLink()).isFalse();
        assertThat(afterA.getHistoricalLinksList()).singleElement().satisfies(retired -> {
            assertThat(retired.getExternalObjectId()).isEqualTo("object-a");
            assertThat(retired.getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
            assertThat(retired.getRetiredByOperationId()).isEqualTo("op-del-a");
            assertThat(retired.getDeletionEvidence().getExternalObjectId()).isEqualTo("object-a");
        });
    }

    @Test
    void aPublicationThatLandsAfterTheSourceWasDeletedIsRecordedAsStillLive() {
        ExternalPublicationOperationRecord latePublish = operation("op-late",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-a", 1L, 5L);
        ExternalPublicationLedger tombstoned = ExternalPublicationLedger.newBuilder()
                .setTombstonedAtSourceRevision(6L).setHighestFencedSourceRevision(6L)
                .addOperations(latePublish)
                .build();

        ExternalPublicationLedger afterLate = published(tombstoned, latePublish);
        // The provider holds the object and nothing has deleted it: it is the current
        // projection, owed a deletion, and carries no deletion evidence.
        assertThat(afterLate.hasCurrentLink()).isTrue();
        assertThat(afterLate.getCurrentLink().getExternalObjectId()).isEqualTo("object-a");
        assertThat(afterLate.getCurrentLink().getProjectionLifecycle()).isEqualTo(
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
        assertThat(afterLate.getCurrentLink().hasDeletionEvidence()).isFalse();
        assertThat(afterLate.getHistoricalLinksList()).isEmpty();

        // Only a verified deletion of that object retires it.
        ExternalPublicationOperationRecord deleteA = operation("op-del-a",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 6L);
        ExternalPublicationLedger afterDelete = deleted(
                afterLate.toBuilder().addOperations(deleteA).build(), deleteA);
        assertThat(afterDelete.hasCurrentLink()).isFalse();
        assertThat(afterDelete.getHistoricalLinksList()).singleElement().satisfies(retired -> {
            assertThat(retired.getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
            assertThat(retired.getRetiredByOperationId()).isEqualTo("op-del-a");
        });
    }

    @Test
    void aLatePublicationBesideAnotherLiveObjectIsKeptPendingAndRetiredByItsOwnDeletion() {
        ExternalPublicationOperationRecord latePublish = operation("op-late-b",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-b", 1L, 4L);
        ExternalPublicationLedger purged = ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .setCurrentLink(link("object-a", 1L, 5L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addOperations(latePublish)
                .build();

        ExternalPublicationLedger afterLate = published(purged, latePublish);
        assertThat(afterLate.getCurrentLink().getExternalObjectId()).isEqualTo("object-a");
        assertThat(afterLate.getHistoricalLinksList()).singleElement().satisfies(pending -> {
            assertThat(pending.getExternalObjectId()).isEqualTo("object-b");
            assertThat(pending.getProjectionLifecycle()).isEqualTo(
                    ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
            assertThat(pending.hasDeletionEvidence()).isFalse();
            assertThat(pending.hasRetiredByOperationId()).isFalse();
        });

        ExternalPublicationOperationRecord deleteB = operation("op-del-b",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-b", 1L, 0L);
        ExternalPublicationLedger afterDeleteB = deleted(
                afterLate.toBuilder().addOperations(deleteB).build(), deleteB);
        assertThat(afterDeleteB.getCurrentLink().getExternalObjectId()).isEqualTo("object-a");
        assertThat(afterDeleteB.getHistoricalLinksList()).singleElement().satisfies(retired -> {
            assertThat(retired.getExternalObjectId()).isEqualTo("object-b");
            assertThat(retired.getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
            assertThat(retired.getRetiredByOperationId()).isEqualTo("op-del-b");
            assertThat(retired.getDeletionEvidence().getExternalObjectId()).isEqualTo("object-b");
        });
    }

    @Test
    void aPublicationOfAnOlderRevisionBehindALiveSourceIsStillOnlySuperseded() {
        ExternalPublicationOperationRecord oldPublish = operation("op-old",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-a", 1L, 3L);
        ExternalPublicationLedger live = ExternalPublicationLedger.newBuilder()
                .setHighestFencedSourceRevision(5L)
                .setCurrentLink(link("object-a", 1L, 5L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .addOperations(oldPublish)
                .build();

        ExternalPublicationLedger after = published(live, oldPublish);
        assertThat(after.getCurrentLink().getSourceRevision()).isEqualTo(5L);
        assertThat(after.getHistoricalLinksList()).singleElement().satisfies(old ->
                assertThat(old.getProjectionLifecycle()).isEqualTo(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED));
    }
}
