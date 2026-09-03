package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentEventRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.v1.DocumentDeleted;
import ai.protomolt.proto.repo.v1.DocumentEvent;
import ai.protomolt.proto.repo.v1.DocumentPurged;
import ai.protomolt.proto.repo.v1.DocumentSaved;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PurgeRequested;
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
 * Builds the outbox rows for the four document-event commit points: one
 * {@link DocumentEvent} protobuf per event, serialized into a
 * {@link DocumentEventRecord} whose {@code kafka_key} is the doc_id (one
 * document's events are partition-ordered) and whose {@code event_id} the
 * protobuf echoes (the consumer dedupe key under at-least-once delivery).
 * <p>
 * Also packages the producer's contract: {@link #descriptorSetBase64()} is
 * the serialized {@link FileDescriptorSet} of document_events.proto and its
 * transitive imports, built from the generated classes' own descriptors, so
 * the protomolt serde validates and frames every record against exactly the
 * schema this service was compiled with - no registry, no drift.
 */
public final class DocumentEventFactory {

    private DocumentEventFactory() {
    }

    /**
     * The save commit point's event (full/partial save, revive included).
     *
     * @param row the ledger row being committed AVAILABLE
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static DocumentEventRecord saved(DocumentRecord row, Instant when) {
        UUID eventId = UUID.randomUUID();
        DocumentEvent event = envelope(eventId, DocumentSaved.newBuilder()
                .setAddress(addressOf(row))
                .setChecksum(row.checksum)
                .setDocVersion(docVersion(row))
                .setSizeBytes(row.sizeBytes == null ? 0 : row.sizeBytes)
                .setSavedAt(timestamp(when))
                .build());
        return record(eventId, DocumentEventRecord.TYPE_SAVED, row.docId, event, when);
    }

    /**
     * The hard-delete commit point's event (purge_storage=true: the row is
     * removed in this transaction).
     *
     * @param row the ledger row being removed
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static DocumentEventRecord deleted(DocumentRecord row, Instant when) {
        UUID eventId = UUID.randomUUID();
        DocumentEvent event = envelope(eventId, DocumentDeleted.newBuilder()
                .setAddress(addressOf(row))
                .setChecksum(row.checksum == null ? "" : row.checksum)
                .setDeletedAt(timestamp(when))
                .build());
        return record(eventId, DocumentEventRecord.TYPE_DELETED, row.docId, event, when);
    }

    /**
     * Phase A of the two-phase delete: tombstone + purge enqueue commit.
     *
     * @param purge the purge record being enqueued
     * @param checksum the root checksum of the body the purge targets
     * @param when the commit instant (the purge record's requested_at)
     * @return the outbox row to persist in the same transaction
     */
    public static DocumentEventRecord purgeRequested(DocumentPurgeRecord purge, String checksum,
            Instant when) {
        UUID eventId = UUID.randomUUID();
        DocumentEvent event = envelope(eventId, PurgeRequested.newBuilder()
                .setAddress(addressOf(purge))
                .setPurgeId(purge.purgeId.toString())
                .setChecksum(checksum == null ? "" : checksum)
                .setRequestedAt(timestamp(when))
                .build());
        return record(eventId, DocumentEventRecord.TYPE_PURGE_REQUESTED, purge.docId, event, when);
    }

    /**
     * Phase B of the two-phase delete: the purge finalized (objects deleted,
     * row removed) in this transaction.
     *
     * @param purge the purge record that landed
     * @param checksum the purged body's root checksum, or null when the row
     *        was already gone
     * @param when the commit instant
     * @return the outbox row to persist in the same transaction
     */
    public static DocumentEventRecord purged(DocumentPurgeRecord purge, String checksum,
            Instant when) {
        UUID eventId = UUID.randomUUID();
        DocumentEvent event = envelope(eventId, DocumentPurged.newBuilder()
                .setAddress(addressOf(purge))
                .setPurgeId(purge.purgeId.toString())
                .setChecksum(checksum == null ? "" : checksum)
                .setPurgedAt(timestamp(when))
                .build());
        return record(eventId, DocumentEventRecord.TYPE_PURGED, purge.docId, event, when);
    }

    /**
     * The descriptor set the protomolt serde publishes against, base64:
     * document_events.proto plus its transitive imports (address.proto, the
     * well-known timestamp.proto), taken from the generated classes' runtime
     * descriptors.
     *
     * @return the serialized FileDescriptorSet, base64-encoded
     */
    public static String descriptorSetBase64() {
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> files =
                new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(java.util.List.of(DocumentEvent.getDescriptor().getFile()));
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

    private static DocumentEvent envelope(UUID eventId, DocumentSaved saved) {
        return DocumentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setSaved(saved)
                .build();
    }

    private static DocumentEvent envelope(UUID eventId, DocumentDeleted deleted) {
        return DocumentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setDeleted(deleted)
                .build();
    }

    private static DocumentEvent envelope(UUID eventId, PurgeRequested requested) {
        return DocumentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPurgeRequested(requested)
                .build();
    }

    private static DocumentEvent envelope(UUID eventId, DocumentPurged purged) {
        return DocumentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPurged(purged)
                .build();
    }

    private static DocumentEventRecord record(UUID eventId, String type, String docId,
            DocumentEvent event, Instant when) {
        DocumentEventRecord record = new DocumentEventRecord();
        record.eventId = eventId;
        record.eventType = type;
        record.payload = event.toByteArray();
        record.kafkaKey = docId;
        record.createdAt = when;
        return record;
    }

    private static NodeAddress addressOf(DocumentRecord row) {
        return NodeAddress.newBuilder()
                .setDocId(row.docId)
                .setGraphAddressId(row.graphAddressId)
                .setAccountId(row.accountId)
                .setGraphId(row.graphId)
                .build();
    }

    private static NodeAddress addressOf(DocumentPurgeRecord purge) {
        return NodeAddress.newBuilder()
                .setDocId(purge.docId)
                .setGraphAddressId(purge.graphAddressId)
                .setAccountId(purge.accountId)
                .setGraphId(purge.graphId)
                .build();
    }

    private static long docVersion(DocumentRecord row) {
        try {
            var manifest = row.readManifest();
            return manifest == null ? 0 : manifest.getDocVersion();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private static Timestamp timestamp(Instant when) {
        return Timestamp.newBuilder().setSeconds(when.getEpochSecond()).setNanos(when.getNano())
                .build();
    }
}
