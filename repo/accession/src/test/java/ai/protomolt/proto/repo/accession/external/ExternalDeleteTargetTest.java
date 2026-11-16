package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOutcome;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A deletion's object and its binding snapshot come from the same recorded link, so an object
 * published under one binding revision is never deleted in another revision's dataset.
 */
class ExternalDeleteTargetTest {

    private static ExternalSystemBinding binding(long revision, String dataset) {
        return ExternalSystemBinding.newBuilder()
                .setBindingId("rust-search").setAccountId("tenant-alpha")
                .setBindingRevision(revision).setDataset(dataset).build();
    }

    private static ExternalLink link(String objectId, long bindingRevision, String dataset,
            ExternalProjectionLifecycle lifecycle) {
        return ExternalLink.newBuilder()
                .setBindingId("rust-search").setBindingRevision(bindingRevision)
                .setBindingSnapshot(binding(bindingRevision, dataset))
                .setExternalObjectId(objectId).setProjectionLifecycle(lifecycle).build();
    }

    private static Optional<ExternalDeletionLifecycle.ResolvedDeleteTarget> recorded(
            ExternalPublicationLedger ledger, long bindingRevision, String objectId) {
        return ExternalDeletionLifecycle.recordedTarget(
                Optional.of(ledger), bindingRevision, objectId);
    }

    @Test
    void anObjectFromHistoryIsDeletedUnderTheBindingRevisionThatPublishedIt() {
        // Published under binding v1 (dataset-one) and since deleted; the binding is now v2.
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .addHistoricalLinks(link("object-a", 1L, "dataset-one",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED))
                .build();

        ExternalDeletionLifecycle.ResolvedDeleteTarget target =
                recorded(ledger, 0L, "").orElseThrow();
        assertThat(target.externalObjectId()).isEqualTo("object-a");
        assertThat(target.binding().getBindingRevision()).isEqualTo(1L);
        assertThat(target.binding().getDataset()).isEqualTo("dataset-one");
    }

    @Test
    void theCurrentLinkIsTheDefaultTargetAndALiveHistoricalLinkComesBeforeADeletedOne() {
        ExternalPublicationLedger withCurrent = ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-live", 2L, "dataset-two",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addHistoricalLinks(link("object-old", 1L, "dataset-one",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build();
        assertThat(recorded(withCurrent, 0L, "").orElseThrow().externalObjectId())
                .isEqualTo("object-live");

        ExternalPublicationLedger historyOnly = ExternalPublicationLedger.newBuilder()
                .addHistoricalLinks(link("object-owed", 1L, "dataset-one",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETION_PENDING))
                .addHistoricalLinks(link("object-gone", 2L, "dataset-two",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED))
                .build();
        ExternalDeletionLifecycle.ResolvedDeleteTarget owed =
                recorded(historyOnly, 0L, "").orElseThrow();
        assertThat(owed.externalObjectId()).isEqualTo("object-owed");
        assertThat(owed.binding().getDataset()).isEqualTo("dataset-one");
    }

    @Test
    void anExplicitObjectIsMatchedToItsOwnLinkAndAnExplicitRevisionMustAgree() {
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .setCurrentLink(link("object-new", 2L, "dataset-two",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_ACTIVE))
                .addHistoricalLinks(link("object-old", 1L, "dataset-one",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build();

        ExternalDeletionLifecycle.ResolvedDeleteTarget old =
                recorded(ledger, 0L, "object-old").orElseThrow();
        assertThat(old.binding().getDataset()).isEqualTo("dataset-one");

        // An object the ledger never recorded has no snapshot here; the caller's store decides.
        assertThat(recorded(ledger, 0L, "object-unknown")).isEmpty();
        assertThat(recorded(ledger, 1L, "object-new")).isEmpty();

        // No object named, and the revision asked for is not the one the default link used.
        assertThatThrownBy(() -> recorded(ledger, 1L, ""))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription()).contains("object-new");
                });
    }

    @Test
    void aPublicationThatWasOnlyAcceptedIsStillADeletionTarget() {
        // The provider accepted the object (or its reply was lost), so it may hold it, but
        // no link was ever confirmed. A tombstone that names no object must still find it.
        ExternalPublicationOperationRecord pendingPublish = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-accepted")
                .setIntent(ExternalPublicationIntent.newBuilder()
                        .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                        .setExternalObjectId("object-pending").setBindingRevision(2L))
                .setBindingSnapshot(binding(2L, "dataset-two"))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .build();
        ExternalPublicationOperationRecord failedPublish = pendingPublish.toBuilder()
                .setOperationId("op-failed")
                .setIntent(pendingPublish.getIntent().toBuilder().setExternalObjectId("object-refused"))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_FAILED)
                .build();
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .addOperations(pendingPublish).addOperations(failedPublish).build();

        ExternalDeletionLifecycle.ResolvedDeleteTarget target =
                recorded(ledger, 0L, "").orElseThrow();
        assertThat(target.externalObjectId()).isEqualTo("object-pending");
        assertThat(target.binding().getDataset()).isEqualTo("dataset-two");

        // A publication the provider refused left nothing to delete.
        assertThat(recorded(ExternalPublicationLedger.newBuilder()
                .addOperations(failedPublish).build(), 0L, "")).isEmpty();
    }

    @Test
    void anExplicitObjectStillPendingIsDeletedUnderTheBindingRevisionItWasSentWith() {
        // Object X was sent under binding v1 (dataset-one) and the reply was lost. The binding
        // has since rotated to v2, which points at another dataset.
        ExternalPublicationOperationRecord pendingUnderV1 = ExternalPublicationOperationRecord.newBuilder()
                .setOperationId("op-x-v1")
                .setIntent(ExternalPublicationIntent.newBuilder()
                        .setAction(ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_PUBLISH)
                        .setExternalObjectId("object-x").setBindingRevision(1L))
                .setBindingSnapshot(binding(1L, "dataset-one"))
                .setOutcome(ExternalPublicationOutcome.EXTERNAL_PUBLICATION_OUTCOME_PENDING)
                .build();
        ExternalPublicationLedger ledger = ExternalPublicationLedger.newBuilder()
                .addOperations(pendingUnderV1).build();

        ExternalDeletionLifecycle.ResolvedDeleteTarget target =
                recorded(ledger, 0L, "object-x").orElseThrow();
        assertThat(target.binding().getBindingRevision()).isEqualTo(1L);
        assertThat(target.binding().getDataset()).isEqualTo("dataset-one");

        // The same ID recorded under two binding revisions names two different objects:
        // without a revision the caller must say which.
        ExternalPublicationLedger both = ledger.toBuilder()
                .addHistoricalLinks(link("object-x", 2L, "dataset-two",
                        ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_SUPERSEDED))
                .build();
        assertThatThrownBy(() -> recorded(both, 0L, "object-x"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription()).contains("binding revisions");
                });
        assertThat(recorded(both, 2L, "object-x").orElseThrow().binding().getDataset())
                .isEqualTo("dataset-two");
        assertThat(recorded(both, 1L, "object-x").orElseThrow().binding().getDataset())
                .isEqualTo("dataset-one");
    }
}
