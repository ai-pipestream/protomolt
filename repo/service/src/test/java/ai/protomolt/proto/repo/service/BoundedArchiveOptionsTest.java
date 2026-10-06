package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BoundedArchiveOptionsTest {
    @Test void explicitResponseLimitRequiresConstructionAndTransportBudget() {
        assertThat(new BoundedArchiveOptions(16, 1024, 4, 8192, 2).maxResponseBytes()).isEqualTo(1024);
        assertThatThrownBy(() -> new BoundedArchiveOptions(16, 1024, 4, 10239, 2, 2048))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transport response allowances");
        assertThat(new BoundedArchiveOptions(16, 1024, 4, 10240, 2, 2048).maxResponseBytes()).isEqualTo(2048);
    }
    @Test void budgetMustAdmitAtLeastOneMaximumTransportPutWithoutIntegerOverflow() {
        int maximum = 256 * 1024 * 1024;
        long minimum = 7L * maximum;
        assertThatThrownBy(() -> new BoundedArchiveOptions(1, maximum, 1, minimum - 1, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seven maximum request allowances");
        assertThat(new BoundedArchiveOptions(1, maximum, 1, minimum, 1024).payloadBudgetBytes()).isEqualTo(minimum);
    }

    @Test void boundsRejectInvalidConcurrencyAndPayloadShapes() {
        assertThatThrownBy(() -> new BoundedArchiveOptions(1, 1, 1, 7, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedArchiveOptions(1, 1, 1, 7, 1025)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedArchiveOptions(2, 1, 1, 7, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedArchiveOptions(1, 1, 257, 7, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void publicFactoryRejectsUnqualifiedStorageBeforeConnecting() {
        var limits = new BoundedArchiveOptions(16, 1024, 4, 8192, 2);
        var config = new RepoServiceConfig(0, new LedgerConfig("jdbc:postgresql://127.0.0.1:1/missing", "unused", "unused"),
                null, null, null, null, "bounded", 0, "redis", null, null, "redis://127.0.0.1:1", 0, 1024);
        assertThatThrownBy(() -> RepoServices.buildBoundedArchive(config, limits)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires managed Redis");
        var qualified = config.withManagedStorage(new ManagedStoragePolicy("bounded", "realm", true));
        var excessive = new BoundedArchiveOptions(1025, 2048, 4, 16384, 2);
        assertThatThrownBy(() -> RepoServices.buildBoundedArchive(qualified, excessive)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object limit");
    }
}
