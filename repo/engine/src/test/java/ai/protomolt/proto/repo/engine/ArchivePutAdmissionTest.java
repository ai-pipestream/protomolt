package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArchivePutAdmissionTest {
    @Test void boundsMetadataPayloadAndEmptyRenditionCountBeforeReserving() {
        var budget = new PayloadBudget(4096);
        try (var admission = new ArchivePutAdmission(new ArchivePutAdmission.Limits(4, 128, 2), budget, 1)) {
            var request = PutEntryRequest.newBuilder().setTitle("x".repeat(129)).build();
            assertThatThrownBy(() -> admission.admit(request)).isInstanceOf(RepositoryException.class);
            var oversized = PutEntryRequest.newBuilder().addRenditions(RenditionContent.newBuilder()
                    .setData(ByteString.copyFromUtf8("12345"))).build();
            assertThatThrownBy(() -> admission.admit(oversized)).isInstanceOf(RepositoryException.class);
            var empty = PutEntryRequest.newBuilder().addRenditions(RenditionContent.getDefaultInstance())
                    .addRenditions(RenditionContent.getDefaultInstance()).addRenditions(RenditionContent.getDefaultInstance()).build();
            assertThatThrownBy(() -> admission.admit(empty)).isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void closePreventsNewCallsButDoesNotReleaseAnActiveScope() throws Exception {
        var budget = new PayloadBudget(1024);
        var admission = new ArchivePutAdmission(new ArchivePutAdmission.Limits(4, 128, 2), budget, 1);
        var request = PutEntryRequest.newBuilder().setTitle("metadata only").build();
        var scope = admission.admit(request);
        admission.close();
        assertThat(admission.awaitIdle(Duration.ZERO)).isFalse();
        assertThat(budget.reservedBytes()).isPositive();
        assertThatThrownBy(() -> admission.admit(request)).isInstanceOfSatisfying(RepositoryException.class,
                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
        scope.close(); scope.close();
        assertThat(admission.awaitIdle(Duration.ZERO)).isTrue();
        assertThat(budget.reservedBytes()).isZero();
    }
}
