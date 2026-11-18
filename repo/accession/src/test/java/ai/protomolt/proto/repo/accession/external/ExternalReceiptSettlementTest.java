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

    /**
     * The ledger as the deletion's dispatch leaves it: every link on record that the deletion
     * covers carries the deletion's operation ID.
     */
    private static ExternalPublicationLedger claimedBy(ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord deleteOp) {
        ExternalPublicationLedger.Builder claimed = ledger.toBuilder();
        long bound = deleteOp.getIntent().getSourceRevision();
        java.util.function.Predicate<ExternalLink> covers = link ->
                link.getExternalObjectId().equals(deleteOp.getIntent().getExternalObjectId())
                        && ExternalDeletionLifecycle.sameExternalTarget(
                                link.getBindingSnapshot(), deleteOp.getBindingSnapshot())
                        && (bound == 0L || link.getSourceRevision() <= bound);
        if (ledger.hasCurrentLink() && covers.test(ledger.getCurrentLink())) {
            claimed.setCurrentLink(ledger.getCurrentLink().toBuilder()
                    .addDeletionClaimedByOperationIds(deleteOp.getOperationId()));
        }
        for (int i = 0; i < ledger.getHistoricalLinksCount(); i++) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (historical.getProjectionLifecycle()
                            != ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED
                    && covers.test(historical)) {
                claimed.setHistoricalLinks(i, historical.toBuilder()
                        .addDeletionClaimedByOperationIds(deleteOp.getOperationId()));
            }
        }
        return claimed.build();
    }

    /** Dispatches (claims) and then settles a deletion, as the lifecycle does. */
    private static ExternalPublicationLedger deleted(ExternalPublicationLedger ledger,
            ExternalPublicationOperationRecord deleteOp) {
        return settled(claimedBy(ledger, deleteOp), deleteOp);
    }

    /** Settles a deletion against the ledger exactly as given. */
    private static ExternalPublicationLedger settled(ExternalPublicationLedger ledger,
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

        // A binding revision that points at another dataset: the same ID there is not A.
        ExternalPublicationOperationRecord deleteAUnderV2 = operation("op-del-a-v2",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 2L, 7L)
                .toBuilder()
                .setBindingSnapshot(binding(2L).toBuilder().setDataset("another-dataset"))
                .build();
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

    @Test
    void aDeletionFromBeforeARepublicationDoesNotRetireTheNewerPublication() {
        // Deleted at tombstone revision 2; the reply was lost. The source was then restored
        // and published again at revision 3 under the same object ID and binding.
        ExternalPublicationOperationRecord deleteAtR2 = operation("op-del-r2",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 2L);
        ExternalPublicationLedger republished = ExternalPublicationLedger.newBuilder()
                .setTombstonedAtSourceRevision(2L).setHighestFencedSourceRevision(3L)
                .setCurrentLink(link("object-a", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .addHistoricalLinks(link("object-a", 1L, 1L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addOperations(deleteAtR2)
                .build();

        ExternalPublicationLedger after = deleted(republished, deleteAtR2);
        // Revision 3 was published after that deletion ran: it is still live and current.
        assertThat(after.getCurrentLink().getSourceRevision()).isEqualTo(3L);
        assertThat(after.getCurrentLink().getProjectionLifecycle())
                .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE);
        // The revision-1 projection is what the deletion removed.
        assertThat(after.getHistoricalLinksList()).singleElement().satisfies(old -> {
            assertThat(old.getSourceRevision()).isEqualTo(1L);
            assertThat(old.getProjectionLifecycle())
                    .isEqualTo(ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
        });
    }

    @Test
    void aPurgedLedgerIsKeptWhileAnyExternalObjectOrOperationIsOutstanding() {
        ExternalPublicationOperationRecord deleteB = operation("op-del-b",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-b", 1L, 0L)
                .toBuilder()
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                .build();
        ExternalLink deletedB = link("object-b", 1L, 2L,
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
        ExternalPublicationLedger.Builder purged = ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .addHistoricalLinks(deletedB).addOperations(deleteB);

        // Everything recorded has been deleted: nothing is owed.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.build())).isTrue();

        // Object A was published for an earlier revision and never deleted.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .addHistoricalLinks(link("object-a", 1L, 1L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build())).isFalse();
        // An earlier projection of the same object the deletion removed is not owed anything.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .addHistoricalLinks(link("object-b", 1L, 1L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build())).isTrue();
        // A projection still current, or an operation still pending, is outstanding.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .setCurrentLink(link("object-c", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .build())).isFalse();
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .addOperations(operation("op-pending",
                        ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE,
                        "object-d", 1L, 0L))
                .build())).isFalse();
    }

    @Test
    void oldDeletionEvidenceDoesNotCoverALaterPublicationOfTheSameObject() {
        // B was deleted, then a publication already in flight put B back: it is owed a
        // deletion again, and the evidence from the first deletion says nothing about it.
        ExternalPublicationLedger.Builder purged = ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .addHistoricalLinks(link("object-b", 1L, 2L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED));
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .addHistoricalLinks(link("object-b", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .build())).isFalse();
        // The same holds for a superseded projection newer than the deletion.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(purged.clone()
                .addHistoricalLinks(link("object-b", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build())).isFalse();
    }

    @Test
    void anOlderBindingsLatePublicationDoesNotDisplaceANewerBindingsLink() {
        // The same source revision was sent under binding v1 (reply lost), the binding rotated
        // to v2 and published it. v1's receipt is only now reconciled.
        ExternalPublicationOperationRecord lateV1 = operation("op-v1",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-a", 1L, 5L);
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setHighestFencedSourceRevision(5L)
                .setCurrentLink(link("object-a", 2L, 5L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .addOperations(lateV1)
                .build();

        ExternalPublicationLedger after = published(ledger, lateV1);
        assertThat(after.getCurrentLink().getBindingRevision()).isEqualTo(2L);
        assertThat(after.getHistoricalLinksList()).singleElement().satisfies(old -> {
            assertThat(old.getBindingRevision()).isEqualTo(1L);
            assertThat(old.getProjectionLifecycle()).isEqualTo(
                    ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED);
        });
    }

    @Test
    void aPurgeDeletionDoesNotCoverAPublicationRecordedAfterItWasSent() {
        // The purge deletion was sent, ran at the provider, and its reply was lost. A
        // publication already dispatched then completed and was recorded as owed a deletion.
        // The old deletion's receipt is only now reconciled.
        ExternalPublicationOperationRecord purgeDelete = operation("op-purge-del",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 0L);
        ExternalLink onRecordAtDispatch = link("object-a", 1L, 3L,
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
        ExternalPublicationLedger atDispatch = claimedBy(ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .setCurrentLink(onRecordAtDispatch)
                .addOperations(purgeDelete)
                .build(), purgeDelete);
        // Recorded afterwards, with the same timestamp as far as any clock could tell.
        ExternalLink recordedLater = link("object-a", 1L, 4L,
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
        ExternalPublicationLedger withLatePublication = atDispatch.toBuilder()
                .addHistoricalLinks(recordedLater).build();

        ExternalPublicationLedger after = settled(withLatePublication, purgeDelete);
        // The link that was on record when the deletion was sent is retired.
        assertThat(after.hasCurrentLink()).isFalse();
        // The later one is still owed a deletion, and the ledger is not ready to be purged.
        assertThat(after.getHistoricalLinksList())
                .filteredOn(link -> link.getSourceRevision() == 4L)
                .singleElement()
                .satisfies(late -> assertThat(late.getProjectionLifecycle()).isEqualTo(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING));
        assertThat(ExternalReceiptSettlement.nothingOutstanding(after)).isFalse();
    }

    @Test
    void aRetriedDeletionDoesNotClaimALinkRecordedAfterItsFirstDispatch() {
        // A purge deletion is registered while a publication is still pending, so there is no
        // link for it to claim. It runs at the provider and its reply is lost. The publication
        // then completes and is recorded as owed a deletion. The deletion is retried, and the
        // provider answers the retry with the original deletion's receipt.
        ExternalPublicationOperationRecord purgeDelete = operation("op-purge-del",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 0L);
        ExternalPublicationLedger beforeDispatch = ExternalPublicationLedger.newBuilder().build();
        ExternalPublicationLedger firstDispatch = register(beforeDispatch, purgeDelete).ledger();
        assertThat(firstDispatch.hasCurrentLink()).isFalse();

        ExternalPublicationLedger withLatePublication = firstDispatch.toBuilder()
                .setCurrentLink(link("object-a", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .build();
        ExternalDeletionLifecycle.DeletionRegistration retry =
                register(withLatePublication, purgeDelete);
        assertThat(retry.attemptNumber()).isEqualTo(2);
        assertThat(retry.ledger().getCurrentLink().getDeletionClaimedByOperationIdsList()).isEmpty();

        ExternalPublicationLedger after = settled(retry.ledger(),
                ExternalLedgerStore.findOperation(retry.ledger(), "op-purge-del").orElseThrow());
        assertThat(after.getCurrentLink().getProjectionLifecycle()).isEqualTo(
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
        assertThat(ExternalReceiptSettlement.nothingOutstanding(after)).isFalse();
    }

    @Test
    void theFirstRegistrationOfADeletionClaimsTheLinksOnRecordThatItCovers() {
        ExternalPublicationOperationRecord purgeDelete = operation("op-purge-del",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 0L);
        ExternalPublicationLedger onRecord = ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-a", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .addHistoricalLinks(link("object-b", 1L, 2L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build();
        ExternalDeletionLifecycle.DeletionRegistration first = register(onRecord, purgeDelete);
        assertThat(first.attemptNumber()).isEqualTo(1);
        assertThat(first.ledger().getCurrentLink().getDeletionClaimedByOperationIdsList()).containsExactly("op-purge-del");
        assertThat(first.ledger().getCurrentLink().getProjectionLifecycle()).isEqualTo(
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING);
        // A different object is not this deletion's.
        assertThat(first.ledger().getHistoricalLinks(0).getDeletionClaimedByOperationIdsList()).isEmpty();
    }

    @Test
    void aPublicationWhoseAnswerFailedVerificationKeepsTheLedgerUntilItsObjectIsDeleted() {
        // The provider was sent the publication and answered with evidence that did not
        // verify. The operation failed, but the object may be there.
        ExternalPublicationOperationRecord unverified = operation("op-unverified",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-a", 1L, 3L)
                .toBuilder()
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .setProviderAnswerUnverified(true)
                .build();
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .addOperations(unverified).build();
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ledger)).isFalse();

        // A verified deletion of a different object, or at a different place, does not cover it.
        ExternalPublicationOperationRecord otherObject = deletedOperation("op-del-b", "object-b", binding(1L));
        ExternalPublicationOperationRecord otherPlace = deletedOperation("op-del-elsewhere", "object-a",
                binding(1L).toBuilder().setDataset("another-dataset").build());
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ledger.toBuilder()
                .addOperations(otherObject).addOperations(otherPlace).build())).isFalse();

        // A verified deletion of the object at the same place, registered after it, does.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ledger.toBuilder()
                .addOperations(deletedOperation("op-del-a", "object-a", binding(2L))).build())).isTrue();

        // One registered before it does not.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ExternalPublicationLedger.newBuilder()
                .addOperations(deletedOperation("op-del-a", "object-a", binding(1L)))
                .addOperations(unverified).build())).isFalse();

        // A publication the provider refused, with verified evidence, left nothing.
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ExternalPublicationLedger.newBuilder()
                .addOperations(unverified.toBuilder().setProviderAnswerUnverified(false)).build())).isTrue();
    }

    @Test
    void aDeletionSentBeforeARepublicationStillRetiresTheProjectionItClaimed() {
        // Deletion D is sent for object-a at revision 3 and its reply is lost. The source is
        // restored and republished as revision 5, which supersedes the revision-3 link. D's
        // receipt is reconciled afterwards.
        ExternalPublicationOperationRecord tombstoneDelete = operation("op-del-d",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 4L);
        ExternalPublicationLedger atDispatch = claimedBy(ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-a", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addOperations(tombstoneDelete)
                .build(), tombstoneDelete);
        ExternalPublicationOperationRecord republish = operation("op-pub-r5",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH, "object-b", 1L, 5L);
        ExternalPublicationLedger republished = published(
                atDispatch.toBuilder().addOperations(republish).build(), republish);
        ExternalLink displaced = republished.getHistoricalLinksList().stream()
                .filter(link -> link.getSourceRevision() == 3L).findFirst().orElseThrow();
        assertThat(displaced.getRetiredByOperationId()).isEqualTo("op-pub-r5");
        assertThat(displaced.getDeletionClaimedByOperationIdsList()).containsExactly("op-del-d");

        ExternalPublicationLedger after = settled(republished, tombstoneDelete);
        assertThat(after.getHistoricalLinksList())
                .filteredOn(link -> link.getSourceRevision() == 3L)
                .singleElement()
                .satisfies(old -> assertThat(old.getProjectionLifecycle()).isEqualTo(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED));
        assertThat(after.getCurrentLink().getSourceRevision()).isEqualTo(5L);
    }

    @Test
    void deletionUnderARotatedBindingCoversTheSameObjectPublishedUnderTheOlderRevision() {
        // object-a was published under v1 and again under v2, which only rotated credentials,
        // so both are the same object at the same place. The purge deleted it under v2.
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .addHistoricalLinks(link("object-a", 1L, 2L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .addHistoricalLinks(link("object-a", 2L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED))
                .build();
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ledger)).isTrue();

        // Under a revision that moved to another dataset, it is a different object.
        ExternalLink elsewhere = link("object-a", 1L, 2L,
                ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED);
        elsewhere = elsewhere.toBuilder()
                .setBindingSnapshot(elsewhere.getBindingSnapshot().toBuilder().setDataset("another-dataset"))
                .build();
        assertThat(ExternalReceiptSettlement.nothingOutstanding(ledger.toBuilder()
                .setHistoricalLinks(0, elsewhere).build())).isFalse();
    }

    private static ExternalPublicationOperationRecord deletedOperation(
            String operationId, String objectId, ExternalSystemBinding target) {
        return operation(operationId, ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE,
                        objectId, target.getBindingRevision(), 0L).toBuilder()
                .setBindingSnapshot(target)
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_DELETED)
                .build();
    }

    @Test
    void aLaterDeletionDoesNotEraseTheClaimOfOneStillInFlight() {
        // D1 is registered and sent; its reply is lost. D2, another purge deletion of the same
        // object, is registered and claims the same link. D1's receipt is reconciled after.
        ExternalPublicationOperationRecord d1 = operation("op-del-1",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 0L);
        ExternalPublicationOperationRecord d2 = operation("op-del-2",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 1L, 0L);
        ExternalPublicationLedger onRecord = ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-a", 1L, 3L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .build();
        ExternalPublicationLedger afterD1 = register(onRecord, d1).ledger();
        ExternalPublicationLedger afterD2 = register(afterD1, d2).ledger();
        assertThat(afterD2.getCurrentLink().getDeletionClaimedByOperationIdsList())
                .containsExactly("op-del-1", "op-del-2");

        ExternalPublicationLedger after = settled(afterD2,
                ExternalLedgerStore.findOperation(afterD2, "op-del-1").orElseThrow());
        assertThat(after.hasCurrentLink()).isFalse();
        assertThat(after.getHistoricalLinksList()).singleElement().satisfies(link ->
                assertThat(link.getProjectionLifecycle()).isEqualTo(
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED));
    }

    /** Registers an attempt of a purge deletion, as the lifecycle does before sending it. */
    private static ExternalDeletionLifecycle.DeletionRegistration register(
            ExternalPublicationLedger ledger, ExternalPublicationOperationRecord deleteOp) {
        return ExternalDeletionLifecycle.registerDeletionAttempt(ledger,
                deleteOp.getOperationId(), "intent-sha-" + deleteOp.getOperationId(),
                deleteOp.getIntent(), deleteOp.getBindingSnapshot(),
                java.util.Optional.empty(), java.util.Optional.of("purge-1"), T0);
    }

    @Test
    void aDeletionUnderARevisedBindingThatDidNotMoveRetiresTheLink() {
        // Published under v1; v2 only rotated credentials; the deletion ran under v2.
        ExternalPublicationOperationRecord deleteUnderV2 = operation("op-del-v2",
                ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE, "object-a", 2L, 0L);
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setSourcePurged(true).setLastPurgeId("purge-1")
                .setCurrentLink(link("object-a", 1L, 5L,
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addOperations(deleteUnderV2)
                .build();
        ExternalPublicationLedger after = deleted(ledger, deleteUnderV2);
        assertThat(after.hasCurrentLink()).isFalse();
    }
}
