package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
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
}
