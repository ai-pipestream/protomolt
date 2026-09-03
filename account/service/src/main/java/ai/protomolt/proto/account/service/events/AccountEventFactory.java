package ai.protomolt.proto.account.service.events;

import ai.protomolt.proto.account.v1.AccountActivated;
import ai.protomolt.proto.account.v1.AccountCreated;
import ai.protomolt.proto.account.v1.AccountDeactivated;
import ai.protomolt.proto.account.v1.AccountEvent;
import ai.protomolt.proto.account.service.store.AccountRecord;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the outbox rows for the three account-event commit points: one
 * {@link AccountEvent} protobuf per event, serialized into an
 * {@link AccountEventRecord} whose {@code kafka_key} is the account_id (one
 * account's events are partition-ordered) and whose {@code event_id} the
 * protobuf echoes (the consumer dedupe key under at-least-once delivery).
 * <p>
 * Also packages the producer's contract: {@link #descriptorSetBase64()} is
 * the serialized {@link FileDescriptorSet} of account_events.proto and its
 * transitive imports, built from the generated classes' own descriptors, so
 * the protomolt serde validates and frames every record against exactly the
 * schema this service was compiled with — no registry, no drift.
 */
public final class AccountEventFactory {

    private AccountEventFactory() {
    }

    /**
     * The create commit point's event.
     *
     * @param row the account row being committed ACTIVE
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static AccountEventRecord created(AccountRecord row, Instant when) {
        UUID eventId = UUID.randomUUID();
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId(eventId.toString())
                .setCreated(AccountCreated.newBuilder()
                        .setAccountId(row.accountId)
                        .setDisplayName(row.displayName == null ? "" : row.displayName)
                        .setOccurredAt(timestamp(when))
                        .putAllMetadata(row.metadata))
                .build();
        return record(eventId, AccountEventRecord.TYPE_CREATED, row.accountId, event, when);
    }

    /**
     * The activation commit point's event (a real SUSPENDED/DEACTIVATED →
     * ACTIVE transition; no-op activations fire nothing).
     *
     * @param row the account row being committed ACTIVE
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static AccountEventRecord activated(AccountRecord row, Instant when) {
        UUID eventId = UUID.randomUUID();
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId(eventId.toString())
                .setActivated(AccountActivated.newBuilder()
                        .setAccountId(row.accountId)
                        .setOccurredAt(timestamp(when))
                        .putAllMetadata(row.metadata))
                .build();
        return record(eventId, AccountEventRecord.TYPE_ACTIVATED, row.accountId, event, when);
    }

    /**
     * The deactivation commit point's event (a real ACTIVE/SUSPENDED →
     * DEACTIVATED transition; no-op deactivations fire nothing).
     *
     * @param row the account row being committed DEACTIVATED
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static AccountEventRecord deactivated(AccountRecord row, Instant when) {
        UUID eventId = UUID.randomUUID();
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId(eventId.toString())
                .setDeactivated(AccountDeactivated.newBuilder()
                        .setAccountId(row.accountId)
                        .setOccurredAt(timestamp(when))
                        .putAllMetadata(row.metadata))
                .build();
        return record(eventId, AccountEventRecord.TYPE_DEACTIVATED, row.accountId, event, when);
    }

    /**
     * The descriptor set the protomolt serde publishes against, base64:
     * account_events.proto plus its transitive imports (the well-known
     * timestamp.proto), taken from the generated classes' runtime
     * descriptors.
     *
     * @return the serialized FileDescriptorSet, base64-encoded
     */
    public static String descriptorSetBase64() {
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> files =
                new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(java.util.List.of(AccountEvent.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                .addAllFile(files.values())
                .build().toByteArray());
    }

    private static AccountEventRecord record(UUID eventId, String type, String accountId,
            AccountEvent event, Instant when) {
        AccountEventRecord record = new AccountEventRecord();
        record.eventId = eventId;
        record.eventType = type;
        record.payload = event.toByteArray();
        record.kafkaKey = accountId;
        record.createdAt = when;
        return record;
    }

    private static Timestamp timestamp(Instant when) {
        return Timestamp.newBuilder().setSeconds(when.getEpochSecond()).setNanos(when.getNano())
                .build();
    }
}
