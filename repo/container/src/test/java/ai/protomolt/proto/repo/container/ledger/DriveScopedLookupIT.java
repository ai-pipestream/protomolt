package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DriveScopedLookupIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");

    @Test void foreignDriveIsFilteredBeforeBackendGateAndBoundedLookupTimesOut() throws Exception {
        try (var c=context(POSTGRES)) {
            var record=new DriveRecord(); record.driveId=UUID.randomUUID(); record.accountId="owner";
            record.name="private"; record.driveType="CUSTOM"; record.bucket="namespace";
            new DriveLedger(c.tx()).insert(record);
            var checks=new AtomicInteger();
            var bounded=c.tx().withTimeouts(new SqlTimeouts(Duration.ofMillis(100),Duration.ofSeconds(1)));
            var ledger=new DriveLedger(bounded,drive -> checks.incrementAndGet());
            assertThat(ledger.findById("foreign",record.driveId)).isEmpty();
            assertThat(ledger.findById("owner",UUID.randomUUID())).isEmpty();
            assertThat(checks.get()).isZero();
            assertThat(ledger.findById("owner",record.driveId)).isPresent();
            assertThat(checks.get()).isEqualTo(1);
            var other=new DriveRecord(); other.driveId=UUID.randomUUID(); other.accountId="owner";
            other.name="second"; other.driveType="CUSTOM"; other.bucket="namespace";
            new DriveLedger(c.tx()).insert(other);
            var foreign=new DriveRecord(); foreign.driveId=UUID.randomUUID(); foreign.accountId="foreign";
            foreign.name="private"; foreign.driveType="CUSTOM"; foreign.bucket="private";
            new DriveLedger(c.tx()).insert(foreign);
            assertThat(ledger.findByIds("owner",Set.of(record.driveId,foreign.driveId))).isEmpty();
            assertThat(ledger.findByIds("owner",Set.of(record.driveId,UUID.randomUUID()))).isEmpty();
            assertThat(checks.get()).as("partial batches do not validate any backend").isEqualTo(1);
            var both=Set.of(record.driveId,other.driveId);
            assertThat(ledger.findByIds("owner",both).orElseThrow()).containsOnlyKeys(record.driveId,other.driveId);
            assertThat(checks.get()).isEqualTo(3);
            try (var blocker=c.pool().getConnection()) {
                blocker.setAutoCommit(false);
                try (var statement=blocker.createStatement()) { statement.execute("LOCK TABLE drives IN ACCESS EXCLUSIVE MODE"); }
                assertThatThrownBy(() -> ledger.findByIds("owner",both))
                        .hasStackTraceContaining("lock timeout");
                assertThat(checks.get()).isEqualTo(3);
                blocker.rollback();
            }
            assertThat(ledger.findByIds("owner",both)).isPresent();
        }
    }
}
