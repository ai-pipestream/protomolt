package ai.pipestream.proto.account.service.events;

import ai.pipestream.proto.account.service.store.AccountRecord;
import ai.pipestream.proto.account.v1.AccountEvent;
import ai.pipestream.proto.account.v1.AccountStatus;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The event factory's contract: what each commit point's outbox row carries
 * (the oneof arm, the account snapshot, the dedupe-key echo) and the
 * descriptor set the relay's serde frames against. No containers — the ITs
 * prove the rows land; this proves the rows are RIGHT.
 */
class AccountEventFactoryTest {

    private static final Instant WHEN = Instant.parse("2026-07-01T12:00:00.123456789Z");

    private static AccountRecord row(String accountId) {
        AccountRecord row = new AccountRecord();
        row.accountId = accountId;
        row.displayName = "Acme";
        row.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
        row.metadata = Map.of("tier", "gold");
        return row;
    }

    @Test
    void createdCarriesTheAccountSnapshot() throws Exception {
        AccountEventRecord record = AccountEventFactory.created(row("acct-1"), WHEN);

        assertThat(record.eventType).isEqualTo(AccountEventRecord.TYPE_CREATED);
        assertThat(record.kafkaKey).isEqualTo("acct-1");
        assertThat(record.createdAt).isEqualTo(WHEN);
        assertThat(record.status).isEqualTo(AccountEventRecord.STATUS_PENDING);

        AccountEvent event = AccountEvent.parseFrom(record.payload);
        // The envelope's event_id echoes the row id — the consumer dedupe key.
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
        assertThat(event.getEventCase()).isEqualTo(AccountEvent.EventCase.CREATED);
        assertThat(event.getCreated().getAccountId()).isEqualTo("acct-1");
        assertThat(event.getCreated().getDisplayName()).isEqualTo("Acme");
        assertThat(event.getCreated().getOccurredAt().getSeconds())
                .isEqualTo(WHEN.getEpochSecond());
        assertThat(event.getCreated().getOccurredAt().getNanos()).isEqualTo(WHEN.getNano());
        assertThat(event.getCreated().getMetadataMap()).containsEntry("tier", "gold");
    }

    @Test
    void activatedAndDeactivatedCarryTheTransition() throws Exception {
        AccountEventRecord activated = AccountEventFactory.activated(row("acct-2"), WHEN);
        AccountEvent activatedEvent = AccountEvent.parseFrom(activated.payload);
        assertThat(activated.eventType).isEqualTo(AccountEventRecord.TYPE_ACTIVATED);
        assertThat(activatedEvent.getEventCase()).isEqualTo(AccountEvent.EventCase.ACTIVATED);
        assertThat(activatedEvent.getActivated().getAccountId()).isEqualTo("acct-2");
        assertThat(activatedEvent.getActivated().getOccurredAt().getSeconds())
                .isEqualTo(WHEN.getEpochSecond());
        assertThat(activatedEvent.getActivated().getMetadataMap()).containsEntry("tier", "gold");

        AccountEventRecord deactivated = AccountEventFactory.deactivated(row("acct-2"), WHEN);
        AccountEvent deactivatedEvent = AccountEvent.parseFrom(deactivated.payload);
        assertThat(deactivated.eventType).isEqualTo(AccountEventRecord.TYPE_DEACTIVATED);
        assertThat(deactivatedEvent.getEventCase()).isEqualTo(AccountEvent.EventCase.DEACTIVATED);
        assertThat(deactivatedEvent.getDeactivated().getAccountId()).isEqualTo("acct-2");
        assertThat(deactivatedEvent.getDeactivated().getOccurredAt().getSeconds())
                .isEqualTo(WHEN.getEpochSecond());
    }

    @Test
    void nullDisplayNameSerializesAsEmpty() throws Exception {
        AccountRecord bare = row("acct-3");
        bare.displayName = null;
        AccountEvent event = AccountEvent.parseFrom(
                AccountEventFactory.created(bare, WHEN).payload);
        assertThat(event.getCreated().getDisplayName()).isEmpty();
    }

    @Test
    void everyEventMintsItsOwnDedupeId() {
        // The dedupe key is per EVENT, not per account: two events for the
        // same account must never collide.
        AccountEventRecord first = AccountEventFactory.created(row("acct-dedupe"), WHEN);
        AccountEventRecord second = AccountEventFactory.created(row("acct-dedupe"), WHEN);
        assertThat(first.eventId).isNotEqualTo(second.eventId);
        assertThat(first.payload).isNotEqualTo(second.payload);
    }

    @Test
    void transitionRecordsAreKeyedStampedAndPending() {
        // The row-level contract for the transition events: kafka key, commit
        // instant, initial status, attempts.
        AccountEventRecord activated = AccountEventFactory.activated(row("acct-4"), WHEN);
        assertThat(activated.kafkaKey).isEqualTo("acct-4");
        assertThat(activated.createdAt).isEqualTo(WHEN);
        assertThat(activated.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
        assertThat(activated.attempts).isZero();
        assertThat(activated.publishedAt).isNull();
        assertThat(activated.lastError).isNull();

        AccountEventRecord deactivated = AccountEventFactory.deactivated(row("acct-4"), WHEN);
        assertThat(deactivated.kafkaKey).isEqualTo("acct-4");
        assertThat(deactivated.createdAt).isEqualTo(WHEN);
        assertThat(deactivated.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
    }

    @Test
    void descriptorSetParsesAndContainsTheContract() throws Exception {
        // The serde frames every record against this set: it must decode, and
        // it must carry account_events.proto plus its transitive imports.
        FileDescriptorSet set = FileDescriptorSet.parseFrom(
                Base64.getDecoder().decode(AccountEventFactory.descriptorSetBase64()));
        assertThat(set.getFileList().stream().map(f -> f.getName()).toList())
                .contains("ai/pipestream/proto/account/v1/account_events.proto",
                        "google/protobuf/timestamp.proto");
        var accountEvents = set.getFileList().stream()
                .filter(f -> f.getName().endsWith("account_events.proto"))
                .findFirst().orElseThrow();
        assertThat(accountEvents.getMessageTypeList().stream().map(m -> m.getName()).toList())
                .contains("AccountEvent", "AccountCreated", "AccountActivated",
                        "AccountDeactivated");
    }
}
