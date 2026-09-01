package ai.pipestream.proto.account.service.events;

import ai.pipestream.proto.account.service.store.AccountDatabase;
import ai.pipestream.proto.account.service.store.AccountRecord;
import ai.pipestream.proto.account.service.store.AccountStoreConfig;
import ai.pipestream.proto.account.v1.AccountEvent;
import ai.pipestream.proto.account.v1.AccountStatus;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The outbox relay against a real testcontainers PostgreSQL 17, with the
 * Kafka producer replaced by a {@link MockProducer}: a claimed PENDING row is
 * "published" and lands PUBLISHED with the account id as its record key; a
 * failing send increments attempts and leaves the row PENDING for the next
 * drain (at MAX_ATTEMPTS it would land FAILED — the DLQ). No Kafka container:
 * what the relay owes Kafka is exercised by the serde lane elsewhere; what
 * the relay owes the OUTBOX is all here.
 */
@Testcontainers(disabledWithoutDocker = true)
class AccountEventRelayIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static AccountDatabase database;
    static JdbcAccountEventOutbox outbox;
    static AccountEventRelay relay;

    @BeforeAll
    static void boot() {
        database = new AccountDatabase(new AccountStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        outbox = new JdbcAccountEventOutbox(database);
        relay = new AccountEventRelay(outbox);
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

    private static MockProducer<String, Message> mockProducer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, null, new StringSerializer(),
                (topic, data) -> data == null ? null : data.toByteArray());
    }

    private static AccountEventRecord enqueuedCreated(String accountId) {
        return enqueuedCreated(accountId, Instant.now());
    }

    private static AccountEventRecord enqueuedCreated(String accountId, Instant when) {
        AccountRecord row = new AccountRecord();
        row.accountId = accountId;
        row.displayName = "Relay " + accountId;
        row.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
        row.metadata = Map.of();
        AccountEventRecord record = AccountEventFactory.created(row, when);
        database.inTransaction(c -> {
            outbox.enqueue(c, record);
            return null;
        });
        return record;
    }

    /** A producer whose every send fails with the given message. */
    private static MockProducer<String, Message> alwaysFailingProducer(String message) {
        return new MockProducer<>(true, null, new StringSerializer(),
                (org.apache.kafka.common.serialization.Serializer<Message>)
                        (topic, data) -> data.toByteArray()) {
            @Override
            public synchronized java.util.concurrent.Future<RecordMetadata> send(
                    ProducerRecord<String, Message> record) {
                java.util.concurrent.CompletableFuture<RecordMetadata> failed =
                        new java.util.concurrent.CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException(message));
                return failed;
            }
        };
    }

    @Test
    void relayPublishesPendingRowsKeyedByAccountId() throws Exception {
        AccountEventRecord first = enqueuedCreated("acct-relay-1");
        AccountEventRecord second = enqueuedCreated("acct-relay-2");

        MockProducer<String, Message> producer = mockProducer(true);
        int published = relay.relayOnce(producer, "account-events", 100);
        assertThat(published).isEqualTo(2);

        List<ProducerRecord<String, Message>> history = producer.history();
        assertThat(history).hasSize(2);
        assertThat(history).allSatisfy(r -> assertThat(r.topic()).isEqualTo("account-events"));
        assertThat(history.stream().map(ProducerRecord::key))
                .containsExactlyInAnyOrder("acct-relay-1", "acct-relay-2");
        // The relay sends the parsed AccountEvent: the round-trip through the
        // outbox payload is lossless.
        ProducerRecord<String, Message> firstSent = history.stream()
                .filter(r -> r.key().equals("acct-relay-1")).findFirst().orElseThrow();
        AccountEvent event = (AccountEvent) firstSent.value();
        assertThat(event.getEventId()).isEqualTo(first.eventId.toString());
        assertThat(event.getCreated().getAccountId()).isEqualTo("acct-relay-1");

        // Both rows settled PUBLISHED with a published_at stamp.
        assertThat(outbox.countByStatus())
                .containsExactly(Map.entry(AccountEventRecord.STATUS_PUBLISHED, 2L));
        assertThat(outbox.findById(second.eventId).orElseThrow().publishedAt).isNotNull();

        // An empty outbox drains zero and is the loop's backoff signal.
        assertThat(relay.relayOnce(producer, "account-events", 100)).isZero();
    }

    @Test
    void failedSendIncrementsAttemptsAndStaysPending() {
        AccountEventRecord record = enqueuedCreated("acct-relay-fail");

        // A producer whose first (and only first) send fails: the relay must
        // record the failure and leave the row PENDING for the next drain.
        MockProducer<String, Message> producer = new MockProducer<>(true, null, new StringSerializer(),
                (org.apache.kafka.common.serialization.Serializer<Message>)
                        (topic, data) -> data.toByteArray()) {
            private boolean failNext = true;

            @Override
            public synchronized java.util.concurrent.Future<RecordMetadata> send(
                    ProducerRecord<String, Message> record) {
                if (failNext) {
                    failNext = false;
                    java.util.concurrent.CompletableFuture<RecordMetadata> failed =
                            new java.util.concurrent.CompletableFuture<>();
                    failed.completeExceptionally(new RuntimeException("broker exploded"));
                    return failed;
                }
                return super.send(record);
            }
        };
        int published = relay.relayOnce(producer, "account-events", 100);
        assertThat(published).isZero();

        AccountEventRecord after = outbox.findById(record.eventId).orElseThrow();
        assertThat(after.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
        assertThat(after.attempts).isEqualTo(1);
        assertThat(after.lastError).contains("broker exploded");

        // The next drain retries the same row — and now it succeeds.
        int retried = relay.relayOnce(producer, "account-events", 100);
        assertThat(retried).isEqualTo(1);
        assertThat(outbox.findById(record.eventId).orElseThrow().status)
                .isEqualTo(AccountEventRecord.STATUS_PUBLISHED);
    }

    @Test
    void attemptsExhaustionLandsFailedAndIsNeverReclaimed() {
        AccountEventRecord record = enqueuedCreated("acct-relay-dlq");

        // Every send fails: attempts climb to MAX_ATTEMPTS and the row lands
        // FAILED — the DLQ. Each drain claims the row while it is PENDING.
        MockProducer<String, Message> producer = alwaysFailingProducer("broker down");
        for (int attempt = 1; attempt <= AccountEventRecord.MAX_ATTEMPTS; attempt++) {
            assertThat(relay.relayOnce(producer, "account-events", 100)).isZero();
        }
        AccountEventRecord after = outbox.findById(record.eventId).orElseThrow();
        assertThat(after.status).isEqualTo(AccountEventRecord.STATUS_FAILED);
        assertThat(after.attempts).isEqualTo(AccountEventRecord.MAX_ATTEMPTS);
        assertThat(after.lastError).contains("broker down");

        // FAILED is terminal: even a healthy producer never sees the row
        // again — the relay deliberately does not re-enqueue it.
        MockProducer<String, Message> healed = mockProducer(true);
        assertThat(relay.relayOnce(healed, "account-events", 100)).isZero();
        assertThat(healed.history()).isEmpty();
        assertThat(outbox.countByStatus())
                .containsExactly(Map.entry(AccountEventRecord.STATUS_FAILED, 1L));
    }

    @Test
    void claimOrderIsOldestFirst() {
        // Insertion order scrambled against created_at: the drain order is
        // the commit time, so relay lag is bounded by the outbox, not by
        // chance.
        Instant now = Instant.now();
        AccountEventRecord newest = enqueuedCreated("acct-relay-new", now);
        AccountEventRecord oldest = enqueuedCreated("acct-relay-old", now.minusSeconds(120));
        AccountEventRecord middle = enqueuedCreated("acct-relay-mid", now.minusSeconds(60));

        assertThat(outbox.claimBatch(10).stream().map(r -> r.eventId).toList())
                .containsExactly(oldest.eventId, middle.eventId, newest.eventId);
    }

    @Test
    void lastErrorIsTruncatedToTheColumnCap() {
        AccountEventRecord record = enqueuedCreated("acct-relay-long-err");

        AccountEventRecord updated = outbox.markFailed(record, "x".repeat(9000)).orElseThrow();
        assertThat(updated.attempts).isEqualTo(1);
        assertThat(updated.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
        assertThat(updated.lastError).hasSize(4000);
        // The truncation is what actually persisted, not just the return value.
        assertThat(outbox.findById(record.eventId).orElseThrow().lastError).hasSize(4000);
    }
}
