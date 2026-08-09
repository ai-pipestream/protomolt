package ai.pipestream.proto.repo.container.ledger;

import ai.pipestream.proto.repo.v1.DriveProviderConfig;
import ai.pipestream.proto.repo.v1.S3DriveConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DriveRecord}'s provider-config JSON column (round-trip, null
 * clearing, parse-failure type), its storage-adapter defaults, and the
 * {@code @PrePersist} creation-timestamp defaulting.
 */
class DriveRecordTest {

    @Test
    void providerConfigRoundTripsThroughTheJsonColumn() {
        DriveRecord record = new DriveRecord();
        DriveProviderConfig config = DriveProviderConfig.newBuilder()
                .setS3(S3DriveConfig.newBuilder()
                        .setEndpointOverride("http://localhost:9000")
                        .setForcePathStyle(true))
                .putOptions("partFanout", "8")
                .build();

        record.writeProviderConfig(config);

        assertThat(record.providerConfig).isNotBlank();
        assertThat(record.readProviderConfig()).isEqualTo(config);
    }

    @Test
    void aNullProviderConfigWritesNullAndReadsNull() {
        DriveRecord record = new DriveRecord();

        assertThat(record.readProviderConfig()).isNull();

        record.writeProviderConfig(DriveProviderConfig.newBuilder()
                .putOptions("k", "v").build());
        assertThat(record.readProviderConfig()).isNotNull();
        record.writeProviderConfig(null);
        assertThat(record.providerConfig).isNull();
        assertThat(record.readProviderConfig()).isNull();
    }

    @Test
    void anUnparseableProviderConfigSurfacesAsLedgerExceptionNamingTheDrive() {
        DriveRecord record = new DriveRecord();
        record.driveId = UUID.randomUUID();
        record.providerConfig = "{ broken json";

        assertThatThrownBy(record::readProviderConfig)
                .isInstanceOf(LedgerException.class)
                .hasMessageContaining(record.driveId.toString());
    }

    @Test
    void newDrivesDefaultToS3AtTheBucketRootAndActive() {
        DriveRecord record = new DriveRecord();

        assertThat(record.provider).isEqualTo("s3");
        assertThat(record.prefix).isEmpty();
        assertThat(record.status).isEqualTo("ACTIVE");
    }

    @Test
    void prePersistDefaultsOnlyAnUnsetCreatedAt() {
        DriveRecord fresh = new DriveRecord();
        fresh.onPrePersist();
        assertThat(fresh.createdAt).isNotNull();

        DriveRecord stamped = new DriveRecord();
        Instant when = Instant.parse("2026-03-03T00:00:00Z");
        stamped.createdAt = when;
        stamped.onPrePersist();
        assertThat(stamped.createdAt).isEqualTo(when);
    }
}
