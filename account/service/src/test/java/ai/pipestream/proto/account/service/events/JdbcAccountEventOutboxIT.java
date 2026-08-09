package ai.pipestream.proto.account.service.events;

import ai.pipestream.proto.account.service.store.AccountDatabase;
import ai.pipestream.proto.account.service.store.AccountRecord;
import ai.pipestream.proto.account.service.store.AccountStoreConfig;
import ai.pipestream.proto.account.service.store.AccountStoreException;
import ai.pipestream.proto.account.v1.AccountStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link JdbcAccountEventOutbox} against a real testcontainers PostgreSQL 17:
 * the row-level contract the relay and the gRPC commit points rely on.
 * {@code AccountEventRelayIT} covers the drain loop with a mock producer;
 * this pins the outbox's own semantics — the caller-transaction enqueue, the
 * conditional terminal transitions, and the claim window's limit.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcAccountEventOutboxIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static AccountDatabase database;
    static JdbcAccountEventOutbox outbox;

    @BeforeAll
    static void boot() {
        database = new AccountDatabase(new AccountStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        outbox = new JdbcAccountEventOutbox(database);
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try {
                c.createStatement().execute("DELETE FROM account_events_outbox");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private static AccountEventRecord newEvent(String accountId) {
        AccountRecord row = new AccountRecord();
        row.accountId = accountId;
        row.displayName = accountId;
        row.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
        row.metadata = Map.of();
        return AccountEventFactory.created(row, Instant.now());
    }

    private AccountEventRecord enqueued(String accountId) {
        AccountEventRecord record = newEvent(accountId);
        database.inTransaction(c -> {
            outbox.enqueue(c, record);
            return null;
        });
        return record;
    }

    @Test
    void enqueueRidesTheCallersTransaction() {
        // The outbox pattern's whole point: the event row commits or rolls
        // back WITH the caller's unit of work.
        RuntimeException marker = new RuntimeException("force rollback");
        AccountEventRecord record = newEvent("acct-outbox-tx");

        assertThatThrownBy(() -> database.inTransaction(c -> {
            outbox.enqueue(c, record);
            throw marker;
        })).isSameAs(marker);

        assertThat(outbox.findById(record.eventId)).isEmpty();
        assertThat(outbox.countByStatus()).isEmpty();
    }

    @Test
    void enqueueDuplicateEventIdFails() {
        AccountEventRecord record = enqueued("acct-outbox-dup");
        assertThatThrownBy(() -> database.inTransaction(c -> {
            outbox.enqueue(c, record);
            return null;
        })).isInstanceOfSatisfying(AccountStoreException.class, e ->
                assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NONE));
        // Still exactly one row.
        assertThat(outbox.countByStatus())
                .containsExactly(Map.entry(AccountEventRecord.STATUS_PENDING, 1L));
    }

    @Test
    void claimBatchHonorsTheLimitAndSkipsSettledRows() {
        AccountEventRecord first = enqueued("acct-claim-1");
        AccountEventRecord second = enqueued("acct-claim-2");
        AccountEventRecord third = enqueued("acct-claim-3");

        List<AccountEventRecord> two = outbox.claimBatch(2);
        assertThat(two).hasSize(2);

        // Settle the oldest; the next claim sees only the remaining PENDING.
        assertThat(outbox.markPublished(first.eventId, Instant.now())).isTrue();
        List<AccountEventRecord> rest = outbox.claimBatch(10);
        assertThat(rest.stream().map(r -> r.eventId).toList())
                .containsExactlyInAnyOrder(second.eventId, third.eventId);

        // Claimed rows carry everything the relay needs: the payload, the
        // Kafka key, and the attempts so far.
        assertThat(rest).allSatisfy(r -> {
            assertThat(r.payload).isNotEmpty();
            assertThat(r.kafkaKey).startsWith("acct-claim-");
            assertThat(r.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
            assertThat(r.attempts).isZero();
            assertThat(r.createdAt).isNotNull();
            assertThat(r.publishedAt).isNull();
        });
    }

    @Test
    void markPublishedIsConditionalAndStampsTheAckTime() {
        AccountEventRecord record = enqueued("acct-publish");
        Instant ackedAt = Instant.now();

        assertThat(outbox.markPublished(record.eventId, ackedAt)).isTrue();
        AccountEventRecord published = outbox.findById(record.eventId).orElseThrow();
        assertThat(published.status).isEqualTo(AccountEventRecord.STATUS_PUBLISHED);
        assertThat(published.publishedAt).isNotNull();
        assertThat(published.publishedAt.getEpochSecond()).isEqualTo(ackedAt.getEpochSecond());

        // Conditional on PENDING: a second settle (a competing relay's late
        // ack) does not move the row and reports it.
        assertThat(outbox.markPublished(record.eventId, Instant.now())).isFalse();
        // An unknown id simply settles nothing.
        assertThat(outbox.markPublished(UUID.randomUUID(), Instant.now())).isFalse();
    }

    @Test
    void markFailedOnASettledRowIsEmptyAndLeavesItAlone() {
        AccountEventRecord record = enqueued("acct-settled");
        assertThat(outbox.markPublished(record.eventId, Instant.now())).isTrue();

        // A competing relay already settled the row: the failure record is a
        // no-op, not a resurrection.
        assertThat(outbox.markFailed(record, "late failure")).isEmpty();
        AccountEventRecord after = outbox.findById(record.eventId).orElseThrow();
        assertThat(after.status).isEqualTo(AccountEventRecord.STATUS_PUBLISHED);
        assertThat(after.attempts).isZero();
        assertThat(after.lastError).isNull();
    }

    @Test
    void markFailedWithNullErrorClearsTheDetail() {
        AccountEventRecord record = enqueued("acct-null-err");

        Optional<AccountEventRecord> failed = outbox.markFailed(record, null);
        assertThat(failed).isPresent();
        assertThat(failed.orElseThrow().attempts).isEqualTo(1);
        assertThat(failed.orElseThrow().lastError).isNull();
        // And it persisted as NULL, not the string "null".
        assertThat(outbox.findById(record.eventId).orElseThrow().lastError).isNull();

        // A later real error overwrites the cleared detail.
        Optional<AccountEventRecord> failedAgain =
                outbox.markFailed(outbox.findById(record.eventId).orElseThrow(), "boom");
        assertThat(failedAgain.orElseThrow().attempts).isEqualTo(2);
        assertThat(failedAgain.orElseThrow().lastError).isEqualTo("boom");
    }

    @Test
    void findByIdOfAnUnknownEventIsEmpty() {
        assertThat(outbox.findById(UUID.randomUUID())).isEmpty();
    }

    @Test
    void countByStatusGroupsEverySettledLane() {
        AccountEventRecord published = enqueued("acct-count-1");
        AccountEventRecord failed = enqueued("acct-count-2");
        enqueued("acct-count-3"); // stays PENDING
        outbox.markPublished(published.eventId, Instant.now());
        for (int i = 0; i < AccountEventRecord.MAX_ATTEMPTS; i++) {
            outbox.markFailed(outbox.findById(failed.eventId).orElseThrow(), "down");
        }

        assertThat(outbox.countByStatus()).containsExactlyInAnyOrderEntriesOf(Map.of(
                AccountEventRecord.STATUS_PENDING, 1L,
                AccountEventRecord.STATUS_PUBLISHED, 1L,
                AccountEventRecord.STATUS_FAILED, 1L));
    }
}
