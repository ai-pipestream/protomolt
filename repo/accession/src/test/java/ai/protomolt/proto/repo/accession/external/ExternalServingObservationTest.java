package ai.protomolt.proto.repo.accession.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.external.v1.ExternalProviderEvidence;
import ai.protomolt.proto.repo.external.v1.ExternalProviderSettlement;
import ai.protomolt.proto.repo.external.v1.ExternalServingVisibility;
import ai.protomolt.proto.repo.external.v1.ManagedSearchIdentity;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * A serving refresh changes only what was observed about serving. The identity a publication
 * was confirmed with (history, index, version, sequence, epoch, manifest) is never replaced.
 */
class ExternalServingObservationTest {

    private static ManagedSearchIdentity confirmed() {
        return ManagedSearchIdentity.newBuilder()
                .setHistoryId(ByteString.copyFrom(new byte[16]))
                .setDocumentKey(ByteString.copyFromUtf8("doc-key"))
                .setIndexKey(ByteString.copyFromUtf8("idx-main"))
                .setAcceptedVersion(5L).setAcceptedSequence(41L)
                .setPublishedCatalogEpoch(12L).setAfterManifestSha256("2".repeat(64))
                .setServingVisibility(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN)
                .build();
    }

    private static ExternalProviderEvidence refreshed(
            ExternalProviderSettlement settlement, ManagedSearchIdentity identity) {
        return ExternalProviderEvidence.newBuilder()
                .setSettlement(settlement).setManagedSearch(identity).build();
    }

    @Test
    void aRefreshUpdatesTheServingObservationAndNothingElse() {
        ManagedSearchIdentity observed = confirmed().toBuilder()
                .setServingVisibility(
                        ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS)
                .build();
        ManagedSearchIdentity merged = ExternalServingObservation.withServingObservation(
                confirmed(),
                refreshed(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED, observed));
        assertThat(merged.getServingVisibility())
                .isEqualTo(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_CURRENT_ROWS);
        assertThat(merged.toBuilder()
                .setServingVisibility(ExternalServingVisibility.EXTERNAL_SERVING_VISIBILITY_UNKNOWN)
                .build()).isEqualTo(confirmed());
    }

    @Test
    void anAcceptedOnlyReceiptCannotEraseThePublishedEpochAndManifest() {
        ManagedSearchIdentity acceptedOnly = confirmed().toBuilder()
                .clearPublishedCatalogEpoch().clearAfterManifestSha256().build();
        assertThatThrownBy(() -> ExternalServingObservation.withServingObservation(
                confirmed(),
                refreshed(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_ACCEPTED_ONLY,
                        acceptedOnly)))
                .isInstanceOfSatisfying(StatusRuntimeException.class, refused ->
                        assertThat(refused.getStatus().getCode())
                                .isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    @Test
    void aReceiptWithADifferentDurableIdentityIsRefused() {
        for (ManagedSearchIdentity altered : new ManagedSearchIdentity[] {
                confirmed().toBuilder().setAcceptedSequence(42L).build(),
                confirmed().toBuilder().setAcceptedVersion(6L).build(),
                confirmed().toBuilder().setPublishedCatalogEpoch(13L).build(),
                confirmed().toBuilder().setIndexKey(ByteString.copyFromUtf8("idx-other")).build(),
                confirmed().toBuilder().setAfterManifestSha256("3".repeat(64)).build()}) {
            assertThatThrownBy(() -> ExternalServingObservation.withServingObservation(
                    confirmed(),
                    refreshed(ExternalProviderSettlement.EXTERNAL_PROVIDER_SETTLEMENT_PUBLISHED,
                            altered)))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, refused -> {
                        assertThat(refused.getStatus().getCode())
                                .isEqualTo(Status.Code.FAILED_PRECONDITION);
                        assertThat(refused.getStatus().getDescription())
                                .contains("durable identity");
                    });
        }
    }
}
