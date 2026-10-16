package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/** Purge transactions. Provider I/O is deliberately outside these transactions. */
final class PostgresPurges {
    private PostgresPurges() {}

    static Optional<CatalogRecords.PurgeState> mark(Connection connection, PurgeDocumentRequest request, Timestamp now) throws SQLException {
        // Serialize only equal idempotency keys (hash collisions merely serialize unrelated purges).
        // The document and object row locks below, not this key lock, protect references.
        lockKey(connection, request.getAddress().getAccountId(), request.getPurgeId());
        var existing = lookup(connection, request.getAddress().getAccountId(), request.getPurgeId());
        if (existing.isPresent()) {
            if (!existing.get().record().getRequest().getAddress().equals(request.getAddress()))
                throw new CatalogRecords.PurgeConflict();
            return existing;
        }
        final long documentId;
        try (var select = connection.prepareStatement("""
                SELECT document_id FROM catalog_documents
                WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ? FOR UPDATE""")) {
            var a = request.getAddress();
            select.setString(1, a.getAccountId()); select.setString(2, a.getGraphId());
            select.setString(3, a.getGraphAddressId()); select.setString(4, a.getDocId());
            try (var rows = select.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                documentId = rows.getLong(1);
            }
        }
        var candidates = new ArrayList<StackRef>();
        // Lock in the same unsigned UTF-8 identity order as commit.checkCarried.
        try (var select = connection.prepareStatement("""
                SELECT o.stack_id, o.object_key, o.sha256, o.length FROM catalog_objects o
                WHERE EXISTS (SELECT 1 FROM catalog_part_refs r WHERE r.document_id = ? AND NOT r.purged
                    AND r.stack_id = o.stack_id AND r.object_key = o.object_key)
                   OR EXISTS (SELECT 1 FROM catalog_blob_refs b WHERE b.document_id = ? AND NOT b.purged
                    AND b.stack_id = o.stack_id AND b.object_key = o.object_key)
                ORDER BY o.stack_id COLLATE "C", o.object_key COLLATE "C" FOR UPDATE OF o""")) {
            select.setLong(1, documentId); select.setLong(2, documentId);
            try (var rows = select.executeQuery()) {
                while (rows.next()) candidates.add(StackRef.newBuilder().setStackId(rows.getString(1))
                        .setKey(rows.getString(2)).setSha256(rows.getString(3)).setLength(rows.getLong(4)).build());
            }
        }
        long references = 0;
        for (String table : new String[]{"catalog_part_refs", "catalog_blob_refs"}) {
            try (var update = connection.prepareStatement("UPDATE " + table + " SET purged = true WHERE document_id = ? AND NOT purged")) {
                update.setLong(1, documentId); references += update.executeLargeUpdate();
            }
        }
        try (var update = connection.prepareStatement("UPDATE catalog_documents SET purged = true WHERE document_id = ?")) {
            update.setLong(1, documentId); update.executeUpdate();
        }
        var record = CatalogPurge.newBuilder().setRequest(request).setMarkedAt(now).setReferencesPurged(references);
        long shared = 0;
        // Fresh READ COMMITTED statement after the object locks sees a save that won the lock first.
        try (var select = connection.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM catalog_part_refs WHERE stack_id = ? AND object_key = ? AND NOT purged)
                    OR EXISTS (SELECT 1 FROM catalog_blob_refs WHERE stack_id = ? AND object_key = ? AND NOT purged)""");
             var update = connection.prepareStatement("UPDATE catalog_objects SET purged = true WHERE stack_id = ? AND object_key = ?")) {
            for (var ref : candidates) {
                select.setString(1, ref.getStackId()); select.setString(2, ref.getKey());
                select.setString(3, ref.getStackId()); select.setString(4, ref.getKey());
                boolean retained;
                try (var rows = select.executeQuery()) { rows.next(); retained = rows.getBoolean(1); }
                if (retained) shared++;
                else {
                    update.setString(1, ref.getStackId()); update.setString(2, ref.getKey()); update.executeUpdate();
                    record.addObjectsToDelete(ref);
                }
            }
        }
        record.setObjectsRetainedShared(shared);
        var built = record.build();
        byte[] bytes = built.toByteArray();
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_purges(account_id, purge_id, document_id, record, record_sha256)
                VALUES (?, ?, ?, ?, ?)""")) {
            insert.setString(1, request.getAddress().getAccountId()); insert.setString(2, request.getPurgeId());
            insert.setLong(3, documentId); insert.setBytes(4, bytes); insert.setString(5, digest(bytes)); insert.executeUpdate();
        }
        var event = baseEvent(request, "marked", now).setPurgeMarked(PurgeMarked.newBuilder()
                .setPurgeId(request.getPurgeId()).setReferencesPurged(references)).build();
        event(connection, documentId, event);
        return Optional.of(new CatalogRecords.PurgeState(built, false));
    }

    static void complete(Connection connection, String accountId, String purgeId, Timestamp now) throws SQLException {
        lockKey(connection, accountId, purgeId);
        var state = lookup(connection, accountId, purgeId).orElseThrow(() -> new IllegalStateException("Purge was not marked"));
        if (state.completed()) return;
        long documentId;
        try (var update = connection.prepareStatement("""
                UPDATE catalog_purges SET completed = true WHERE account_id = ? AND purge_id = ? RETURNING document_id""")) {
            update.setString(1, accountId); update.setString(2, purgeId);
            try (var rows = update.executeQuery()) { rows.next(); documentId = rows.getLong(1); }
        }
        var record = state.record();
        event(connection, documentId, baseEvent(record.getRequest(), "completed", now).setPurgeCompleted(
                PurgeCompleted.newBuilder().setPurgeId(purgeId).setObjectsDeleted(record.getObjectsToDeleteCount())
                        .setObjectsRetainedShared(record.getObjectsRetainedShared())).build());
    }

    private static Optional<CatalogRecords.PurgeState> lookup(Connection connection, String accountId, String purgeId) throws SQLException {
        try (var select = connection.prepareStatement("SELECT record, record_sha256, completed FROM catalog_purges WHERE account_id = ? AND purge_id = ?")) {
            select.setString(1, accountId); select.setString(2, purgeId);
            try (var rows = select.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                byte[] bytes = rows.getBytes(1);
                if (!digest(bytes).equals(rows.getString(2))) throw new CatalogDataLoss("Purge record digest mismatch");
                try {
                    var record = CatalogPurge.parseFrom(bytes);
                    if (!record.getRequest().getAddress().getAccountId().equals(accountId) || !record.getRequest().getPurgeId().equals(purgeId))
                        throw new CatalogDataLoss("Purge record identity mismatch");
                    return Optional.of(new CatalogRecords.PurgeState(record, rows.getBoolean(3)));
                } catch (InvalidProtocolBufferException corrupt) { throw new CatalogDataLoss("Purge record does not decode", corrupt); }
            }
        }
    }

    private static void lockKey(Connection connection, String accountId, String purgeId) throws SQLException {
        try (var lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
            lock.setString(1, accountId.length() + ":" + accountId + purgeId);
            lock.execute();
        }
    }

    private static DocumentEvent.Builder baseEvent(PurgeDocumentRequest request, String phase, Timestamp time) {
        // Length-delimited protobuf identity; reason and timestamp cannot change event identity on retry.
        var identity = request.toBuilder().clearReason().build().toByteArray();
        var prefix = ("protomolt/purge/" + phase + "/v1\0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var name = new byte[prefix.length + identity.length];
        System.arraycopy(prefix, 0, name, 0, prefix.length); System.arraycopy(identity, 0, name, prefix.length, identity.length);
        return DocumentEvent.newBuilder().setAddress(request.getAddress()).setOccurredAt(time)
                .setEventId(UUID.nameUUIDFromBytes(name).toString());
    }

    private static void event(Connection connection, long documentId, DocumentEvent event) throws SQLException {
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_outbox(event_id, document_id, revision, event_key, payload, created_at)
                VALUES (?, ?, 0, ?, ?, ?)""")) {
            insert.setString(1, event.getEventId()); insert.setLong(2, documentId);
            insert.setString(3, event.getAddress().getDocId()); insert.setBytes(4, event.toByteArray());
            insert.setTimestamp(5, java.sql.Timestamp.from(Instant.ofEpochSecond(event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos())));
            insert.executeUpdate();
        }
    }

    private static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable", impossible); }
    }
}
