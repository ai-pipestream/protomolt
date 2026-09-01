package ai.pipestream.proto.repo.container.ledger;

import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drive-ledger behavior against a real PostgreSQL: insert, the
 * account-scoped name lookup, keyset pagination, and the
 * {@code (account_id, name)} unique constraint.
 */
@Testcontainers
class DriveLedgerIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final String ACCOUNT = "acct-drives";

    private static DriveLedger ledger;

    @BeforeAll
    static void boot() {
        LedgerConfig config = new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        LedgerDatabase database = new LedgerDatabase(config);
        ledger = new DriveLedger(new Tx(database.entityManagerFactory()));
        // Deliberately never closed: shared across the class; the container
        // dies with the JVM.
    }

    private static DriveRecord drive(String account, String name) {
        DriveRecord record = new DriveRecord();
        record.driveId = UUID.randomUUID();
        record.accountId = account;
        record.name = name;
        record.driveType = "PIPELINE";
        record.bucket = "bucket-" + name;
        return record;
    }

    @Test
    void insertFindByIdAndFindByName() {
        DriveRecord record = drive(ACCOUNT, "main");
        record.region = "us-east-1";
        record.metadata = "{\"tier\":\"hot\"}";

        DriveRecord inserted = ledger.insert(record);
        assertThat(inserted.createdAt).isNotNull();
        assertThat(inserted.provider).isEqualTo("s3");
        assertThat(inserted.prefix).isEmpty();
        assertThat(inserted.status).isEqualTo("ACTIVE");

        Optional<DriveRecord> byId = ledger.findById(record.driveId);
        assertThat(byId).isPresent();
        assertThat(byId.get().bucket).isEqualTo("bucket-main");
        assertThat(byId.get().region).isEqualTo("us-east-1");
        assertThat(byId.get().metadata).contains("hot");

        Optional<DriveRecord> byName = ledger.findByName(ACCOUNT, "main");
        assertThat(byName).isPresent();
        assertThat(byName.get().driveId).isEqualTo(record.driveId);

        assertThat(ledger.findByName(ACCOUNT, "nope")).isEmpty();
        assertThat(ledger.findByName("other-account", "main")).isEmpty();
    }

    @Test
    void duplicateAccountNameIsRejected() {
        ledger.insert(drive(ACCOUNT, "dupe"));
        // Same name in the SAME account: rejected. Same name in ANOTHER
        // account: fine — names are only account-scoped.
        assertThatThrownBy(() -> ledger.insert(drive(ACCOUNT, "dupe")))
                .isInstanceOf(PersistenceException.class);
        assertThat(ledger.insert(drive("acct-other", "dupe")).name).isEqualTo("dupe");
    }

    @Test
    void listByAccountPaginatesByNameKeyset() {
        String account = "acct-paging";
        for (String name : List.of("alpha", "bravo", "charlie", "delta", "echo")) {
            ledger.insert(drive(account, name));
        }
        ledger.insert(drive("acct-unrelated", "zulu")); // must not leak in

        List<DriveRecord> page1 = ledger.listByAccount(account, 2, null);
        assertThat(page1).extracting(d -> d.name).containsExactly("alpha", "bravo");

        List<DriveRecord> page2 = ledger.listByAccount(account, 2, "bravo");
        assertThat(page2).extracting(d -> d.name).containsExactly("charlie", "delta");

        List<DriveRecord> page3 = ledger.listByAccount(account, 2, "delta");
        assertThat(page3).extracting(d -> d.name).containsExactly("echo");

        List<DriveRecord> all = ledger.listByAccount(account, 100, null);
        assertThat(all).hasSize(5);
    }
}
