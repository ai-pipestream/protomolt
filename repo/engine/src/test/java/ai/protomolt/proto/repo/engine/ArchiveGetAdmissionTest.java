package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArchiveGetAdmissionTest {
    private static RenditionManifestEntry item(long size) {
        return RenditionManifestEntry.newBuilder().setState(RenditionState.RENDITION_STATE_PRESENT)
                .setRendition(RenditionDescriptor.newBuilder().setName("data")) .setSizeBytes(size).build();
    }

    @Test void computesExactProtobufFramingAcrossVarintBoundaries() {
        for (int size : new int[] {0, 1, 127, 128, 16383, 16384}) {
            var item = item(size);
            var envelope = GetEntryResponse.newBuilder().setManifest(VersionManifest.newBuilder().addRenditions(item)).build();
            var result = envelope.toBuilder().addRenditions(RenditionContent.newBuilder()
                    .setRendition(item.getRendition()).setData(ByteString.copyFrom(new byte[size]))).build();
            var budget = new PayloadBudget(100_000);
            try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(Math.max(size, 1), result.getSerializedSize(), 2), budget, 1);
                    var scope = gate.admit(envelope, List.of(item))) {
                scope.verify(result);
                assertThat(budget.reservedBytes()).isEqualTo(result.getSerializedSize() + 2L * size);
                assertThatThrownBy(() -> scope.verify(result.toBuilder().clearRenditions().build()))
                        .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void rejectsAggregateMetadataAndInvalidStoredSizesWithoutReservations() {
        var budget = new PayloadBudget(4096);
        try (var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(64, 128, 2), budget, 1)) {
            var envelope = GetEntryResponse.getDefaultInstance();
            assertThatThrownBy(() -> gate.admit(envelope, List.of(item(64), item(64))))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
            assertThatThrownBy(() -> gate.admit(GetEntryResponse.newBuilder().setInfo(EntryInfo.newBuilder().setTitle("x".repeat(129))).build(), List.of()))
                    .isInstanceOf(RepositoryException.class);
            assertThatThrownBy(() -> gate.admit(envelope, List.of(item(-1))))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThatThrownBy(() -> gate.admit(envelope, List.of(item(Long.MAX_VALUE))))
                    .isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void closeAndSharedBudgetDoNotReleaseLiveConstruction() throws Exception {
        var budget = new PayloadBudget(64);
        var gate = new ArchiveGetAdmission(new ArchiveGetAdmission.Limits(32, 64, 2), budget, 2);
        var scope = gate.admit(GetEntryResponse.getDefaultInstance(), List.of(item(16)));
        assertThatThrownBy(() -> gate.admit(GetEntryResponse.getDefaultInstance(), List.of(item(16))))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
        gate.close();
        assertThat(gate.awaitIdle(Duration.ZERO)).isFalse();
        assertThat(budget.reservedBytes()).isPositive();
        scope.close(); scope.close();
        assertThat(gate.awaitIdle(Duration.ZERO)).isTrue();
        assertThat(budget.reservedBytes()).isZero();
        assertThatThrownBy(() -> gate.admit(GetEntryResponse.getDefaultInstance(), List.of()))
                .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
    }
}
