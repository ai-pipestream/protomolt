package ai.pipestream.proto.account.service.store;

import ai.pipestream.proto.account.v1.Account;
import ai.pipestream.proto.account.v1.AccountStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AccountRecord#toProto()}: the store row's wire conversion. The ITs
 * see the proto through the RPC responses; this pins the conversion's own
 * edges — null display name, absent timestamps, and the nanos on the
 * timestamp fields. No containers.
 */
class AccountRecordTest {

    private static final Instant CREATED = Instant.parse("2026-07-01T10:00:00.123456789Z");
    private static final Instant UPDATED = Instant.parse("2026-07-02T11:30:00.987654321Z");

    @Test
    void fullRecordConvertsEveryField() {
        AccountRecord record = new AccountRecord();
        record.accountId = "acct-1";
        record.displayName = "Acme Corp";
        record.status = AccountStatus.ACCOUNT_STATUS_SUSPENDED;
        record.metadata = Map.of("tier", "gold", "region", "eu");
        record.createdAt = CREATED;
        record.updatedAt = UPDATED;

        Account proto = record.toProto();
        assertThat(proto.getAccountId()).isEqualTo("acct-1");
        assertThat(proto.getDisplayName()).isEqualTo("Acme Corp");
        assertThat(proto.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_SUSPENDED);
        assertThat(proto.getMetadataMap())
                .containsExactlyInAnyOrderEntriesOf(Map.of("tier", "gold", "region", "eu"));
        // Sub-second precision survives the conversion (Instant.nanos ↔
        // Timestamp.nanos), not just the epoch seconds.
        assertThat(proto.getCreatedAt().getSeconds()).isEqualTo(CREATED.getEpochSecond());
        assertThat(proto.getCreatedAt().getNanos()).isEqualTo(CREATED.getNano());
        assertThat(proto.getUpdatedAt().getSeconds()).isEqualTo(UPDATED.getEpochSecond());
        assertThat(proto.getUpdatedAt().getNanos()).isEqualTo(UPDATED.getNano());
    }

    @Test
    void nullDisplayNameAndTimestampsStayAbsentOnTheWire() {
        AccountRecord record = new AccountRecord();
        record.accountId = "acct-bare";
        // displayName, createdAt, updatedAt left null; metadata defaults to empty.
        record.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;

        Account proto = record.toProto();
        // Proto3 strings cannot be null: the conversion maps a null display
        // name to the empty string rather than throwing.
        assertThat(proto.getDisplayName()).isEmpty();
        // Absent instants are genuinely absent, not epoch-zero stamps.
        assertThat(proto.hasCreatedAt()).isFalse();
        assertThat(proto.hasUpdatedAt()).isFalse();
        assertThat(proto.getMetadataMap()).isEmpty();
    }

    @Test
    void protoRoundTripThroughTheRecord() throws Exception {
        AccountRecord record = new AccountRecord();
        record.accountId = "acct-rt";
        record.displayName = "Round Trip";
        record.status = AccountStatus.ACCOUNT_STATUS_DEACTIVATED;
        record.metadata = Map.of("k", "v");
        record.createdAt = CREATED;
        record.updatedAt = UPDATED;

        // Serialize and reparse: the wire form is stable.
        Account proto = record.toProto();
        assertThat(Account.parseFrom(proto.toByteArray())).isEqualTo(proto);
    }
}
