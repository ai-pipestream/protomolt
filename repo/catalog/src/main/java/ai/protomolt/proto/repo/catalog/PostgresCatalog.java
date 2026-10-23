package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.catalog.CatalogRecords.Commit;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CommitResult;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Committed;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Conflict;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Credential;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CredentialCheck;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Current;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Grant;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Outcome;
import ai.protomolt.proto.repo.catalog.CatalogRecords.OutboxEvent;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Recorded;
import ai.protomolt.proto.repo.catalog.CatalogRecords.RejectionResult;
import ai.protomolt.proto.repo.catalog.CatalogRecords.SourcePurged;
import ai.protomolt.proto.repo.catalog.CatalogRecords.WriteDenied;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DocumentSortField;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.types.ConceptRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import javax.sql.DataSource;

/**
 * The PostgreSQL catalog (requirements §7). A save commits in one READ COMMITTED
 * transaction: outcome lookup, writer-authority recheck under share locks,
 * compare-and-set of the current-revision pointer, purge check of carried
 * objects, then the revision, part references, outcome and outbox event. The
 * compare-and-set is the only coordination between hosts; there are no leases.
 */
public final class PostgresCatalog implements Catalog {
    private static final String OUTCOME_KEY = "catalog_operation_outcomes_pkey";
    /** Bound on the digests remembered as persisted; past it the set starts again from empty. */
    private static final int KNOWN_ARTIFACTS = 4096;
    private final DataSource dataSource;
    /**
     * Schema artifact digests this process has seen committed. Rows are immutable by
     * trigger, so a remembered digest stays persisted; the database stays the truth,
     * and a new process starts empty and sends what it has not seen committed.
     */
    private final Set<String> knownArtifacts = ConcurrentHashMap.newKeySet();
    private final LongAdder artifactsSent = new LongAdder();
    private final HikariDataSource owned;

    /** Opens a pool and applies the baseline. */
    public PostgresCatalog(CatalogConfig config) {
        this(config, null);
    }

    /**
     * Opens a pool whose connections live in {@code schema} and applies the baseline
     * there. A catalog co-hosted with a repository ledger database needs its own
     * schema: the ledger's Flyway history occupies the default schema, and the
     * baseline refuses any schema that already holds objects it did not create.
     *
     * @param config the pool configuration
     * @param schema the catalog's dedicated schema name ({@code [a-z_][a-z0-9_]*}),
     *        or null or blank for the connection default
     */
    public PostgresCatalog(CatalogConfig config, String schema) {
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.username());
        hikari.setPassword(config.password());
        hikari.setMaximumPoolSize(config.maxPoolSize());
        hikari.setAutoCommit(false);
        hikari.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        hikari.setPoolName("repo-catalog");
        // An unreachable catalog surfaces as UNAVAILABLE quickly rather than as a hang.
        hikari.setConnectionTimeout(10_000);
        String dedicated = schema == null || schema.isBlank() ? null : schema;
        if (dedicated != null) {
            if (!isSchemaName(dedicated))
                throw new IllegalArgumentException("Catalog schema must match [a-z_][a-z0-9_]*: " + dedicated);
            hikari.setSchema(dedicated);
        }
        this.owned = new HikariDataSource(hikari);
        this.dataSource = owned;
        try {
            if (dedicated != null) {
                // The name is validated above; DDL takes no bind parameters.
                try (var connection = owned.getConnection(); var statement = connection.createStatement()) {
                    statement.execute("CREATE SCHEMA IF NOT EXISTS " + dedicated);
                    connection.commit();
                } catch (java.sql.SQLException failure) {
                    throw CatalogException.from("Catalog schema creation", failure);
                }
            }
            CatalogBaseline.apply(dataSource);
        } catch (RuntimeException failure) {
            owned.close();
            throw failure;
        }
    }

    /** A dedicated catalog schema is a plain PostgreSQL identifier ({@code [a-z_][a-z0-9_]*}); nothing else is accepted. */
    private static boolean isSchemaName(String name) {
        if (name == null || name.isEmpty()) return false;
        char first = name.charAt(0);
        if (!((first >= 'a' && first <= 'z') || first == '_')) return false;
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
        }
        return true;
    }

    @Override public void close() { if (owned != null) owned.close(); }

    // ------------------------------------------------------------------ stacks

    /** Returns the stack ID recorded for this namespace, recording it on first use. */
    @Override public String registerStack(StackRecord record) {
        return inTransaction("Stack registration", connection -> {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_stacks (stack_id, provider, endpoint, namespace, record, created_at)
                    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""")) {
                insert.setString(1, record.getStackId());
                insert.setString(2, record.getProvider());
                insert.setString(3, record.getEndpoint());
                insert.setString(4, record.getNamespace());
                insert.setBytes(5, record.toByteArray());
                insert.setTimestamp(6, Timestamp.from(Instant.now()));
                insert.executeUpdate();
            }
            try (var select = connection.prepareStatement(
                    "SELECT stack_id FROM catalog_stacks WHERE provider = ? AND endpoint = ? AND namespace = ?")) {
                select.setString(1, record.getProvider());
                select.setString(2, record.getEndpoint());
                select.setString(3, record.getNamespace());
                try (var rows = select.executeQuery()) {
                    if (!rows.next())
                        throw new IllegalStateException("Stack ID " + record.getStackId() + " already names another namespace");
                    return rows.getString(1);
                }
            }
        });
    }

    // ------------------------------------------------------------------ reads

    /** The recorded outcome for an operation ID, read in a fresh transaction. */
    @Override public Optional<Outcome> outcome(String accountId, String operationId) {
        return inTransaction("Outcome lookup", connection -> selectOutcome(connection, accountId, operationId));
    }

    /** The document's current-revision pointer. */
    @Override public Optional<Current> current(NodeAddress address) {
        return inTransaction("Current revision lookup", connection -> selectCurrent(connection, address, false));
    }

    /** One accession's stored manifest; its stored digest is verified. */
    @Override public Optional<StoredManifest> manifest(NodeAddress address, long revision) {
        return inTransaction("Manifest lookup", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT r.manifest, r.manifest_sha256 FROM catalog_revisions r
                    JOIN catalog_documents d ON d.document_id = r.document_id
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                      AND NOT d.purged AND r.revision = ?""")) {
                bindAddress(select, 1, address);
                select.setLong(5, revision);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    return Optional.of(parseManifest(rows.getBytes(1), rows.getString(2), address, revision));
                }
            }
        });
    }

    /** The document's current-revision manifest, read in a single transaction. */
    @Override public Optional<StoredManifest> currentManifest(NodeAddress address) {
        return inTransaction("Current manifest lookup", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT d.current_revision, r.manifest, r.manifest_sha256
                    FROM catalog_documents d
                    LEFT JOIN catalog_revisions r ON r.document_id = d.document_id AND r.revision = d.current_revision
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                      AND NOT d.purged""")) {
                bindAddress(select, 1, address);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    long revision = rows.getLong(1);
                    byte[] bytes = rows.getBytes(2);
                    if (bytes == null) throw new CatalogDataLoss("Current revision has no manifest");
                    return Optional.of(parseManifest(bytes, rows.getString(3), address, revision));
                }
            }
        });
    }

    /** Stored manifests below {@code beforeRevision}, newest first. */
    @Override public List<StoredManifest> revisions(NodeAddress address, long beforeRevision, int limit) {
        return inTransaction("Revision history", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT r.manifest, r.manifest_sha256, r.revision FROM catalog_revisions r
                    JOIN catalog_documents d ON d.document_id = r.document_id
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                      AND NOT d.purged AND r.revision < ? ORDER BY r.revision DESC LIMIT ?""")) {
                bindAddress(select, 1, address);
                select.setLong(5, beforeRevision);
                select.setInt(6, limit);
                var result = new ArrayList<StoredManifest>();
                try (var rows = select.executeQuery()) {
                    while (rows.next()) result.add(parseManifest(rows.getBytes(1), rows.getString(2), address, rows.getLong(3)));
                }
                return result;
            }
        });
    }

    /** Retained schema bytes by SHA-256. */
    @Override public Optional<ByteString> artifact(String sha256) {
        return inTransaction("Schema artifact lookup", connection -> {
            try (var select = connection.prepareStatement("SELECT content FROM catalog_schema_artifacts WHERE sha256 = ?")) {
                select.setString(1, sha256);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    var raw = rows.getBytes(1);
                    if (!sha256Hex(raw).equals(sha256))
                        throw new CatalogDataLoss("Retained schema artifact differs from its digest");
                    return Optional.of(UnsafeByteOperations.unsafeWrap(raw));
                }
            }
        });
    }

    /** Retained schema artifact length by SHA-256, without reading its content. */
    @Override public OptionalLong artifactLength(String sha256) {
        return inTransaction("Schema artifact length lookup", connection -> {
            try (var select = connection.prepareStatement(
                    "SELECT octet_length(content) FROM catalog_schema_artifacts WHERE sha256 = ?")) {
                select.setString(1, sha256);
                try (var rows = select.executeQuery()) {
                    return rows.next() ? OptionalLong.of(rows.getLong(1)) : OptionalLong.empty();
                }
            }
        });
    }

    // ------------------------------------------------------------------ commit

    /** The entry bytes of a revision, with the lookup of its CORE part. */
    private static byte[] entryBytes(StoredManifest manifest, Optional<DocumentLookup> lookup) {
        return CatalogEntries.entry(manifest, lookup).toByteArray();
    }

    private static final String LISTED_COLUMNS = "graph_id, graph_address_id, doc_id, current_revision, tombstone, "
            + "catalog_entry, catalog_entry_sha256, committed_nanos, COALESCE(lookup_title, '')";

    @Override public List<CatalogRecords.ListedDocument> documents(CatalogRecords.Listing listing,
            Optional<CatalogRecords.Cursor> after, int limit) {
        if (limit < 1 || limit > 1001) throw new IllegalArgumentException("Invalid catalog page size");
        if (after.isPresent() && !after.get().after().getAccountId().equals(listing.accountId()))
            throw new IllegalArgumentException("Cursor belongs to another account");
        if (listing.sort().getField() == DocumentSortField.UNRECOGNIZED)
            throw new IllegalArgumentException("Unknown sort field");
        var filter = listing.filter();
        var where = new StringBuilder("NOT purged AND account_id = ?");
        var binds = new ArrayList<Object>();
        binds.add(listing.accountId());
        if (!listing.graphId().isEmpty()) { where.append(" AND graph_id = ?"); binds.add(listing.graphId()); }
        if (!listing.graphAddressId().isEmpty()) { where.append(" AND graph_address_id = ?"); binds.add(listing.graphAddressId()); }
        if (!listing.includeTombstoned()) where.append(" AND NOT tombstone");
        if (filter.hasDocId()) { where.append(" AND doc_id = ?"); binds.add(filter.getDocId()); }
        if (filter.hasTitle()) { where.append(" AND lookup_title = ?"); binds.add(filter.getTitle()); }
        if (filter.hasDocumentType()) { where.append(" AND lookup_document_type = ?"); binds.add(filter.getDocumentType()); }
        if (filter.hasSourceUri()) { where.append(" AND lookup_source_uri = ?"); binds.add(filter.getSourceUri()); }
        if (filter.hasSourceMimeType()) { where.append(" AND lookup_source_mime_type = ?"); binds.add(filter.getSourceMimeType()); }
        if (filter.hasLanguage()) { where.append(" AND lookup_language = ?"); binds.add(filter.getLanguage()); }
        if (filter.hasAuthor()) { where.append(" AND lookup_author = ?"); binds.add(filter.getAuthor()); }
        if (filter.hasCategory()) { where.append(" AND lookup_category = ?"); binds.add(filter.getCategory()); }
        if (filter.hasDatasourceId()) { where.append(" AND lookup_datasource_id = ?"); binds.add(filter.getDatasourceId()); }
        if (filter.hasConnectorId()) { where.append(" AND lookup_connector_id = ?"); binds.add(filter.getConnectorId()); }
        if (filter.hasCommittedAfter()) { where.append(" AND committed_nanos >= ?"); binds.add(CatalogEntries.nanos(filter.getCommittedAfter())); }
        if (filter.hasCommittedBefore()) { where.append(" AND committed_nanos < ?"); binds.add(CatalogEntries.nanos(filter.getCommittedBefore())); }
        for (var match : filter.getMetadataList()) {
            where.append(" AND EXISTS (SELECT 1 FROM catalog_document_metadata m WHERE m.document_id = d.document_id"
                    + " AND m.key = ? AND m.value = ?)");
            binds.add(match.getKey()); binds.add(match.getValue());
        }
        if (filter.hasConcept()) {
            // Any pair of the expanded set; the pairs travel as two parallel text arrays.
            where.append(" AND EXISTS (SELECT 1 FROM catalog_document_concepts c"
                    + " JOIN unnest(?::text[], ?::text[]) AS q(scheme_id, concept_id)"
                    + " ON c.scheme_id = q.scheme_id AND c.concept_id = q.concept_id WHERE c.document_id = d.document_id)");
            binds.add(listing.concepts().stream().map(ConceptRef::getSchemeId).toArray(String[]::new));
            binds.add(listing.concepts().stream().map(ConceptRef::getConceptId).toArray(String[]::new));
        }
        switch (filter.getOwnerCase()) {
            case OWNER_DATASOURCE_ID -> { where.append(" AND owner_datasource_id = ?"); binds.add(filter.getOwnerDatasourceId()); }
            case OWNER_MODULE_ID -> { where.append(" AND owner_module_id = ?"); binds.add(filter.getOwnerModuleId()); }
            case OWNER_NOT_SET -> {}
        }
        // The sort key, then the address: the same columns in the same order as the index.
        var address = "(graph_id COLLATE \"C\", graph_address_id COLLATE \"C\", doc_id COLLATE \"C\")";
        var key = switch (listing.sort().getField()) {
            case DOCUMENT_SORT_FIELD_COMMITTED_AT -> "committed_nanos";
            case DOCUMENT_SORT_FIELD_TITLE -> "(COALESCE(lookup_title, '') COLLATE \"C\")";
            default -> null;
        };
        boolean descending = key != null && listing.sort().getDescending();
        if (after.isPresent()) {
            var cursor = after.get();
            if (key == null) {
                where.append(" AND ").append(address).append(" > (?, ?, ?)");
            } else {
                where.append(" AND (").append(key).append(descending ? " < ?" : " > ?").append(" OR (")
                        .append(key).append(" = ? AND ").append(address).append(" > (?, ?, ?)))");
                Object value = key.equals("committed_nanos") ? CatalogEntries.nanos(cursor.committedAt()) : cursor.title();
                binds.add(value); binds.add(value);
            }
            binds.add(cursor.after().getGraphId()); binds.add(cursor.after().getGraphAddressId()); binds.add(cursor.after().getDocId());
        }
        var order = (key == null ? "" : key + (descending ? " DESC, " : ", "))
                + "graph_id COLLATE \"C\", graph_address_id COLLATE \"C\", doc_id COLLATE \"C\"";
        var sql = "SELECT " + LISTED_COLUMNS + " FROM catalog_documents d WHERE " + where + " ORDER BY " + order + " LIMIT ?";
        binds.add(limit);
        return inTransaction("Document listing", connection -> {
            try (var select = connection.prepareStatement(sql)) {
                for (int i = 0; i < binds.size(); i++) {
                    if (binds.get(i) instanceof String[] values) select.setArray(i + 1, connection.createArrayOf("text", values));
                    else select.setObject(i + 1, binds.get(i));
                }
                var result = new ArrayList<CatalogRecords.ListedDocument>();
                try (var rows = select.executeQuery()) {
                    while (rows.next()) {
                        var found = NodeAddress.newBuilder().setAccountId(listing.accountId()).setGraphId(rows.getString(1))
                                .setGraphAddressId(rows.getString(2)).setDocId(rows.getString(3)).build();
                        var entry = parseEntry(rows.getBytes(6), rows.getString(7), found);
                        var cursor = new CatalogRecords.Cursor(found, CatalogEntries.timestamp(rows.getLong(8)), rows.getString(9));
                        result.add(new CatalogRecords.ListedDocument(found, rows.getLong(4), rows.getBoolean(5),
                                entry.getCommittedAt(), entry, cursor));
                    }
                }
                return List.copyOf(result);
            }
        });
    }

    static CatalogEntry parseEntry(byte[] bytes, String sha256, NodeAddress address) {
        if (bytes == null || !sha256Hex(bytes).equals(sha256)) throw new CatalogDataLoss("Catalog entry digest mismatch");
        final CatalogEntry entry;
        try { entry = CatalogEntry.parseFrom(bytes); }
        catch (InvalidProtocolBufferException bad) { throw new CatalogDataLoss("Catalog entry does not decode", bad); }
        if (!entry.getDocId().equals(address.getDocId()) || !entry.hasOwnership()
                || !entry.getOwnership().getAccountId().equals(address.getAccountId()) || !entry.hasCommittedAt())
            throw new CatalogDataLoss("Catalog entry identity mismatch");
        return entry;
    }

    @Override public boolean rewriteEntry(NodeAddress address, long revision, Optional<DocumentLookup> lookup,
            List<CatalogRecords.Link> links, List<CatalogRecords.ConceptTag> concepts) {
        if (lookup.isEmpty() && (!links.isEmpty() || !concepts.isEmpty()))
            throw new IllegalArgumentException("Link and concept rows go with the lookup of a CORE part");
        CatalogRecords.requireDistinct(links);
        CatalogRecords.requireDistinctTags(concepts);
        return inTransaction("Catalog entry rebuild", connection -> {
            final long documentId;
            try (var select = connection.prepareStatement("""
                    SELECT document_id FROM catalog_documents
                    WHERE NOT purged AND account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ?
                    AND current_revision = ? FOR UPDATE""")) {
                bindAddress(select, 1, address);
                select.setLong(5, revision);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return false;
                    documentId = rows.getLong(1);
                }
            }
            final StoredManifest manifest;
            try (var select = connection.prepareStatement(
                    "SELECT manifest, manifest_sha256 FROM catalog_revisions WHERE document_id = ? AND revision = ?")) {
                select.setLong(1, documentId);
                select.setLong(2, revision);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) throw new CatalogDataLoss("Current revision has no manifest");
                    manifest = parseManifest(rows.getBytes(1), rows.getString(2), address, revision);
                }
            }
            var entry = entryBytes(manifest, lookup);
            try (var update = connection.prepareStatement("UPDATE catalog_documents SET catalog_entry = ?, catalog_entry_sha256 = ?, "
                    + LOOKUP_ASSIGNMENTS + " WHERE document_id = ?")) {
                update.setBytes(1, entry);
                update.setString(2, sha256Hex(entry));
                bindLookup(update, 3, manifest, lookup);
                update.setLong(3 + LOOKUP_COLUMNS, documentId);
                update.executeUpdate();
            }
            writeMetadata(connection, documentId, lookup);
            writeLinks(connection, documentId, links);
            writeConcepts(connection, documentId, concepts);
            return true;
        });
    }

    /** Replaces the document's concept rows; an empty list clears them. */
    private static void writeConcepts(Connection connection, long documentId, List<CatalogRecords.ConceptTag> concepts)
            throws SQLException {
        try (var delete = connection.prepareStatement("DELETE FROM catalog_document_concepts WHERE document_id = ?")) {
            delete.setLong(1, documentId);
            delete.executeUpdate();
        }
        if (concepts.isEmpty()) return;
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_document_concepts (document_id, field_path, ordinal, scheme_id, concept_id)
                VALUES (?, ?, ?, ?, ?)""")) {
            for (var tag : concepts) {
                insert.setLong(1, documentId);
                insert.setString(2, tag.fieldPath());
                insert.setInt(3, tag.ordinal());
                insert.setString(4, tag.schemeId());
                insert.setString(5, tag.conceptId());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    @Override public List<CatalogRecords.ConceptTag> concepts(NodeAddress address) {
        return inTransaction("Concept rows", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT c.field_path, c.ordinal, c.scheme_id, c.concept_id
                    FROM catalog_document_concepts c JOIN catalog_documents d ON d.document_id = c.document_id
                    WHERE NOT d.purged AND d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                    ORDER BY c.field_path COLLATE "C", c.ordinal""")) {
                bindAddress(select, 1, address);
                var result = new ArrayList<CatalogRecords.ConceptTag>();
                try (var rows = select.executeQuery()) {
                    while (rows.next())
                        result.add(new CatalogRecords.ConceptTag(rows.getString(1), rows.getInt(2), rows.getString(3),
                                rows.getString(4)));
                }
                return List.copyOf(result);
            }
        });
    }

    /** Replaces the document's static link rows; an empty list clears them. */
    private static void writeLinks(Connection connection, long documentId, List<CatalogRecords.Link> links) throws SQLException {
        try (var delete = connection.prepareStatement("DELETE FROM catalog_document_links WHERE document_id = ?")) {
            delete.setLong(1, documentId);
            delete.executeUpdate();
        }
        if (links.isEmpty()) return;
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_document_links (document_id, field_path, ordinal, relation, target_account_id,
                    target_graph_id, target_graph_address_id, target_doc_id, pinned_revision)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            for (var link : links) {
                insert.setLong(1, documentId);
                insert.setString(2, link.fieldPath());
                insert.setInt(3, link.ordinal());
                insert.setString(4, link.relation());
                bindAddress(insert, 5, link.target());
                if (link.pinnedRevision().isPresent()) insert.setLong(9, link.pinnedRevision().getAsLong());
                else insert.setNull(9, Types.BIGINT);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    // ------------------------------------------------------------------ links

    @Override public Optional<CatalogRecords.LinkTarget> linkTarget(NodeAddress target, OptionalLong revision) {
        return inTransaction("Link target lookup", connection -> {
            final long documentId;
            final long current;
            final boolean tombstone;
            final CatalogEntry entry;
            try (var select = connection.prepareStatement("""
                    SELECT document_id, current_revision, tombstone, catalog_entry, catalog_entry_sha256 FROM catalog_documents
                    WHERE NOT purged AND account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ?""")) {
                bindAddress(select, 1, target);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    documentId = rows.getLong(1);
                    current = rows.getLong(2);
                    tombstone = rows.getBoolean(3);
                    entry = parseEntry(rows.getBytes(4), rows.getString(5), target);
                }
            }
            Optional<CatalogRecords.PinnedRevision> pinned = Optional.empty();
            if (revision.isPresent()) {
                try (var select = connection.prepareStatement(
                        "SELECT tombstone, structured_data_type FROM catalog_revisions WHERE document_id = ? AND revision = ?")) {
                    select.setLong(1, documentId);
                    select.setLong(2, revision.getAsLong());
                    try (var rows = select.executeQuery()) {
                        if (rows.next()) pinned = Optional.of(new CatalogRecords.PinnedRevision(revision.getAsLong(),
                                rows.getBoolean(1), Optional.ofNullable(rows.getString(2))));
                    }
                }
            }
            return Optional.of(new CatalogRecords.LinkTarget(current, tombstone, entry, pinned));
        });
    }

    @Override public boolean rewriteRevisionType(NodeAddress address, long revision, Optional<String> structuredDataType) {
        return inTransaction("Revision type rebuild", connection -> {
            try (var update = connection.prepareStatement("""
                    UPDATE catalog_revisions r SET structured_data_type = ? FROM catalog_documents d
                    WHERE d.document_id = r.document_id AND NOT d.purged AND d.account_id = ? AND d.graph_id = ?
                      AND d.graph_address_id = ? AND d.doc_id = ? AND r.revision = ?""")) {
                update.setString(1, structuredDataType.orElse(null));
                bindAddress(update, 2, address);
                update.setLong(6, revision);
                return update.executeUpdate() == 1;
            }
        });
    }

    @Override public List<CatalogRecords.Link> links(NodeAddress address) {
        return inTransaction("Link rows", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT l.field_path, l.ordinal, l.relation, l.target_account_id, l.target_graph_id,
                        l.target_graph_address_id, l.target_doc_id, l.pinned_revision
                    FROM catalog_document_links l JOIN catalog_documents d ON d.document_id = l.document_id
                    WHERE NOT d.purged AND d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                    ORDER BY l.field_path COLLATE "C", l.ordinal""")) {
                bindAddress(select, 1, address);
                var result = new ArrayList<CatalogRecords.Link>();
                try (var rows = select.executeQuery()) { while (rows.next()) result.add(link(rows)); }
                return List.copyOf(result);
            }
        });
    }

    private static final String LINKING_DOCUMENT = "(d.graph_id COLLATE \"C\", d.graph_address_id COLLATE \"C\", d.doc_id COLLATE \"C\")";

    @Override public List<CatalogRecords.Backlink> backlinks(NodeAddress target, Optional<String> relation,
            Optional<CatalogRecords.BacklinkCursor> after, int limit) {
        if (limit < 1 || limit > 1001) throw new IllegalArgumentException("Invalid backlink page size");
        var where = new StringBuilder("NOT d.purged AND l.target_account_id = ? AND l.target_graph_id = ?"
                + " AND l.target_graph_address_id = ? AND l.target_doc_id = ?");
        if (relation.isPresent()) where.append(" AND l.relation = ?");
        if (after.isPresent())
            where.append(" AND (").append(LINKING_DOCUMENT).append(" > (?, ?, ?) OR (").append(LINKING_DOCUMENT)
                    .append(" = (?, ?, ?) AND (l.field_path COLLATE \"C\" > ? OR (l.field_path = ? AND l.ordinal > ?))))");
        var sql = "SELECT l.field_path, l.ordinal, l.relation, l.target_account_id, l.target_graph_id,"
                + " l.target_graph_address_id, l.target_doc_id, l.pinned_revision, d.account_id, d.graph_id,"
                + " d.graph_address_id, d.doc_id, d.current_revision, d.catalog_entry, d.catalog_entry_sha256"
                + " FROM catalog_document_links l JOIN catalog_documents d ON d.document_id = l.document_id WHERE " + where
                + " ORDER BY d.graph_id COLLATE \"C\", d.graph_address_id COLLATE \"C\", d.doc_id COLLATE \"C\","
                + " l.field_path COLLATE \"C\", l.ordinal LIMIT ?";
        return inTransaction("Backlink listing", connection -> {
            try (var select = connection.prepareStatement(sql)) {
                int next = 1;
                bindAddress(select, next, target);
                next += 4;
                if (relation.isPresent()) select.setString(next++, relation.get());
                if (after.isPresent()) {
                    var cursor = after.get();
                    for (int twice = 0; twice < 2; twice++) {
                        select.setString(next++, cursor.source().getGraphId());
                        select.setString(next++, cursor.source().getGraphAddressId());
                        select.setString(next++, cursor.source().getDocId());
                    }
                    select.setString(next++, cursor.fieldPath());
                    select.setString(next++, cursor.fieldPath());
                    select.setInt(next++, cursor.ordinal());
                }
                select.setInt(next, limit);
                var result = new ArrayList<CatalogRecords.Backlink>();
                try (var rows = select.executeQuery()) {
                    while (rows.next()) {
                        var linking = NodeAddress.newBuilder().setAccountId(rows.getString(9)).setGraphId(rows.getString(10))
                                .setGraphAddressId(rows.getString(11)).setDocId(rows.getString(12)).build();
                        result.add(new CatalogRecords.Backlink(linking, rows.getLong(13),
                                parseEntry(rows.getBytes(14), rows.getString(15), linking), link(rows)));
                    }
                }
                return List.copyOf(result);
            }
        });
    }

    /** One link row from its first eight columns. */
    private static CatalogRecords.Link link(ResultSet rows) throws SQLException {
        var target = NodeAddress.newBuilder().setAccountId(rows.getString(4)).setGraphId(rows.getString(5))
                .setGraphAddressId(rows.getString(6)).setDocId(rows.getString(7)).build();
        long pinned = rows.getLong(8);
        var revision = rows.wasNull() ? OptionalLong.empty() : OptionalLong.of(pinned);
        return new CatalogRecords.Link(rows.getString(1), rows.getInt(2), rows.getString(3), target, revision);
    }

    /** The derived columns of a revision, in the order {@link #bindLookup} binds them. */
    private static final String LOOKUP_ASSIGNMENTS = "committed_nanos = ?, lookup_title = ?, lookup_document_type = ?, lookup_source_uri = ?, "
            + "lookup_source_mime_type = ?, lookup_language = ?, lookup_author = ?, lookup_category = ?, "
            + "lookup_datasource_id = ?, lookup_connector_id = ?, owner_datasource_id = ?, owner_module_id = ?";
    private static final int LOOKUP_COLUMNS = 12;

    private static void bindLookup(PreparedStatement statement, int first, StoredManifest manifest, Optional<DocumentLookup> lookup)
            throws SQLException {
        var l = lookup.orElse(DocumentLookup.getDefaultInstance());
        statement.setLong(first++, CatalogEntries.nanos(manifest.getCommittedAt()));
        statement.setString(first, l.hasTitle() ? l.getTitle() : null);
        statement.setString(first + 1, l.hasDocumentType() ? l.getDocumentType() : null);
        statement.setString(first + 2, l.hasSourceUri() ? l.getSourceUri() : null);
        statement.setString(first + 3, l.hasSourceMimeType() ? l.getSourceMimeType() : null);
        statement.setString(first + 4, l.hasLanguage() ? l.getLanguage() : null);
        statement.setString(first + 5, l.hasAuthor() ? l.getAuthor() : null);
        statement.setString(first + 6, l.hasCategory() ? l.getCategory() : null);
        statement.setString(first + 7, lookup.isPresent() ? manifest.getOwnership().getDatasourceId() : null);
        statement.setString(first + 8, lookup.isPresent() && manifest.getOwnership().hasConnectorId()
                ? manifest.getOwnership().getConnectorId() : null);
        var owner = manifest.getTombstone() ? SaveOwner.getDefaultInstance() : manifest.getOwner();
        statement.setString(first + 9, owner.hasIntake() ? owner.getIntake().getDatasourceId() : null);
        statement.setString(first + 10, owner.hasStep() ? owner.getStep().getModuleId() : null);
    }

    /** Replaces the document's metadata rows with the lookup's; a tombstone clears them. */
    private static void writeMetadata(Connection connection, long documentId, Optional<DocumentLookup> lookup) throws SQLException {
        try (var delete = connection.prepareStatement("DELETE FROM catalog_document_metadata WHERE document_id = ?")) {
            delete.setLong(1, documentId);
            delete.executeUpdate();
        }
        if (lookup.isEmpty() || lookup.get().getMetadataCount() == 0) return;
        try (var insert = connection.prepareStatement(
                "INSERT INTO catalog_document_metadata (document_id, key, value) VALUES (?, ?, ?)")) {
            for (var pair : lookup.get().getMetadataMap().entrySet()) {
                insert.setLong(1, documentId);
                insert.setString(2, pair.getKey());
                insert.setString(3, pair.getValue());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    @Override public Optional<CatalogRecords.PurgeState> markPurged(ai.protomolt.proto.repo.v2.PurgeDocumentRequest request,
            com.google.protobuf.Timestamp markedAt) {
        return inTransaction("Purge mark", connection -> PostgresPurges.mark(connection, request, markedAt));
    }

    @Override public void completePurge(String accountId, String purgeId, com.google.protobuf.Timestamp completedAt) {
        inTransaction("Purge completion", connection -> {
            PostgresPurges.complete(connection, accountId, purgeId, completedAt);
            return null;
        });
    }

    /** Commits one accession, or reports why it did not. Never partial. */
    @Override public CommitResult commit(Commit commit) {
        long next = commit.expected().hasNone() ? 1 : Math.addExact(commit.expected().getRevision(), 1);
        var manifest = commit.manifest();
        if (manifest.getRevision() != next || manifest.getPreviousRevision() != next - 1
                || !manifest.getAddress().equals(commit.address()) || !manifest.getOperationId().equals(commit.operationId())
                || !manifest.getIntentSha256().equals(commit.intentSha256()))
            throw new IllegalArgumentException("Commit records do not describe revision " + next + " of this operation");
        if (commit.event().getRevision() != next || !commit.outcome().hasAccession()
                || commit.outcome().getAccession().getRevision() != next)
            throw new IllegalArgumentException("Commit outcome and event must describe revision " + next);
        var account = commit.address().getAccountId();
        boolean conflicted;
        try (var connection = dataSource.getConnection()) {
            try {
                var recorded = selectOutcome(connection, account, commit.operationId());
                if (recorded.isPresent()) {
                    connection.rollback();
                    return new Recorded(recorded.get());
                }
                var denied = recheckAuthority(connection, commit.grantIds(), commit.credential());
                if (denied.isPresent()) {
                    connection.rollback();
                    return denied.get();
                }
                var applied = compareAndSet(connection, commit, commit.expected().hasNone(), next - 1,
                        manifest.getTombstone(), manifest);
                if (applied.isEmpty()) {
                    connection.rollback();
                    conflicted = true;
                } else {
                    var purged = checkCarried(connection, commit.carriedObjects());
                    if (purged.isPresent()) {
                        connection.rollback();
                        return new SourcePurged(purged.get());
                    }
                    var sent = writeAccession(connection, applied.get().documentId(), commit, next, applied.get().lookup());
                    connection.commit();
                    remember(sent);
                    return new Committed();
                }
            } catch (SQLException failure) {
                rollbackQuietly(connection, failure);
                if (!"23505".equals(failure.getSQLState()) || !failure.getMessage().contains(OUTCOME_KEY))
                    throw CatalogException.from("Catalog commit", failure);
                conflicted = true; // The same operation ID committed concurrently elsewhere.
            } catch (RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw CatalogException.from("Catalog commit", failure);
        }
        // A failed transaction's snapshot is not proof that no outcome exists: reread in a fresh one.
        return inTransaction("Conflict outcome reread", connection -> {
            var recorded = selectOutcome(connection, account, commit.operationId());
            if (recorded.isPresent()) return new Recorded(recorded.get());
            // The failed CAS waited for any competing purge transaction. Its durable marker
            // distinguishes a retired address from an absent document or revision conflict.
            try (var select = connection.prepareStatement("""
                    SELECT purged FROM catalog_documents
                    WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ?""")) {
                bindAddress(select, 1, commit.address());
                try (var rows = select.executeQuery()) {
                    if (rows.next() && rows.getBoolean(1)) return new CatalogRecords.TargetPurged();
                }
            }
            var current = selectCurrent(connection, commit.address(), false);
            return new Conflict(current.map(Current::revision).orElse(0L), current.map(Current::tombstone).orElse(false));
        });
    }

    /**
     * Records a terminal rejection. Returns the stored outcome, which is another
     * request's when the operation ID was already recorded.
     */
    @Override public RejectionResult recordRejection(String accountId, String operationId, String intentSha256,
            RecordedOutcome outcome, List<String> grantIds, Optional<CredentialCheck> credential,
            NodeAddress address, boolean creating) {
        if (!outcome.hasRejection()) throw new IllegalArgumentException("Only a rejection is recorded without an accession");
        return inTransaction("Rejection record", connection -> {
            var existing = selectOutcome(connection, accountId, operationId);
            if (existing.isPresent()) return new Recorded(existing.get());
            var denied = recheckAuthority(connection, grantIds, credential);
            if (denied.isPresent()) return denied.get();
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_operation_outcomes (account_id, operation_id, intent_sha256, outcome, recorded_at,
                        rejection_address, rejection_create, rejection_issuer, rejection_credential_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""")) {
                insert.setString(1, accountId);
                insert.setString(2, operationId);
                insert.setString(3, intentSha256);
                insert.setBytes(4, outcome.toByteArray());
                insert.setTimestamp(5, toTimestamp(outcome.getRecordedAt()));
                insert.setBytes(6, address.toByteArray());
                insert.setBoolean(7, creating);
                insert.setString(8, credential.map(CatalogRecords.CredentialCheck::issuer).orElse(null));
                insert.setObject(9, credential.map(CatalogRecords.CredentialCheck::credentialId).orElse(null));
                insert.executeUpdate();
            }
            return new Recorded(selectOutcome(connection, accountId, operationId)
                    .orElseThrow(() -> new IllegalStateException("Recorded rejection is not readable")));
        });
    }

    private Optional<WriteDenied> recheckAuthority(Connection connection, List<String> grantIds,
            Optional<CredentialCheck> credentialCheck) throws SQLException {
        // Every grant authorizing this write, in unsigned UTF-8 order so two commits
        // naming the same grants take their share locks in one order.
        try (var lock = connection.prepareStatement("SELECT snapshot_sha256, revoked FROM lock_catalog_writer_grant(?)")) {
            for (var grantId : grantIds) {
                lock.setString(1, grantId);
                try (var rows = lock.executeQuery()) {
                    if (!rows.next()) return Optional.of(new WriteDenied("writer grant " + grantId + " no longer exists"));
                    if (rows.getBoolean(2)) return Optional.of(new WriteDenied("writer grant " + grantId + " is revoked"));
                }
            }
        }
        if (credentialCheck.isPresent()) {
            var credential = credentialCheck.get();
            try (var lock = connection.prepareStatement(
                    "SELECT generation, revoked FROM lock_catalog_credential_authority(?, ?)")) {
                lock.setString(1, credential.issuer());
                lock.setObject(2, credential.credentialId());
                try (var rows = lock.executeQuery()) {
                    if (!rows.next()) return Optional.of(new WriteDenied("credential is not registered"));
                    if (rows.getBoolean(2) || rows.getLong(1) != credential.generation())
                        return Optional.of(new WriteDenied("credential generation is not current"));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Moves the pointer from {@code expectedRevision}; empty when the document is
     * elsewhere. The entry and its lookup columns follow: a create or a CORE write
     * sets them from the commit's lookup, a tombstone clears them, and a commit
     * that carries CORE keeps the row's current lookup.
     */
    /** The moved pointer's row and the lookup the revision now has: written, sourced or carried. */
    private record Applied(long documentId, Optional<DocumentLookup> lookup) {}

    private static Optional<Applied> compareAndSet(Connection connection, Commit commit, boolean create,
            long expectedRevision, boolean tombstone, StoredManifest manifest) throws SQLException {
        var address = commit.address();
        var now = toTimestamp(manifest.getCommittedAt());
        if (create) {
            var lookup = commit.lookup();
            var entry = entryBytes(manifest, lookup);
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_documents (account_id, graph_id, graph_address_id, doc_id, current_revision,
                        tombstone, updated_at, catalog_entry, catalog_entry_sha256, committed_nanos, lookup_title, lookup_document_type,
                        lookup_source_uri, lookup_source_mime_type, lookup_language, lookup_author, lookup_category,
                        lookup_datasource_id, lookup_connector_id, owner_datasource_id, owner_module_id)
                    VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING document_id""")) {
                bindAddress(insert, 1, address);
                insert.setBoolean(5, tombstone);
                insert.setTimestamp(6, now);
                insert.setBytes(7, entry);
                insert.setString(8, sha256Hex(entry));
                bindLookup(insert, 9, manifest, lookup);
                try (var rows = insert.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    long documentId = rows.getLong(1);
                    writeMetadata(connection, documentId, lookup);
                    writeLinks(connection, documentId, commit.links());
                    writeConcepts(connection, documentId, commit.concepts());
                    return Optional.of(new Applied(documentId, lookup));
                }
            }
        }
        Optional<DocumentLookup> lookup = commit.lookup();
        boolean carried = !tombstone && lookup.isEmpty();
        if (carried) {
            // CORE is unchanged: the current row's lookup stays, under the row lock the update takes next.
            try (var select = connection.prepareStatement("""
                    SELECT catalog_entry, catalog_entry_sha256 FROM catalog_documents
                    WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ? AND current_revision = ?
                    AND NOT purged FOR UPDATE""")) {
                bindAddress(select, 1, address);
                select.setLong(5, expectedRevision);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    var current = parseEntry(rows.getBytes(1), rows.getString(2), address);
                    if (!current.hasLookup()) throw new CatalogDataLoss("Current catalog entry has no lookup to carry");
                    lookup = Optional.of(current.getLookup());
                }
            }
        }
        var entry = entryBytes(manifest, lookup);
        try (var update = connection.prepareStatement("""
                UPDATE catalog_documents SET current_revision = current_revision + 1, tombstone = ?, updated_at = ?, catalog_entry = ?, catalog_entry_sha256 = ?, """
                + LOOKUP_ASSIGNMENTS + """

                WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ? AND current_revision = ? AND NOT purged AND (NOT ? OR NOT tombstone)
                RETURNING document_id""")) {
            update.setBoolean(1, tombstone);
            update.setTimestamp(2, now);
            update.setBytes(3, entry);
            update.setString(4, sha256Hex(entry));
            bindLookup(update, 5, manifest, lookup);
            int next = 5 + LOOKUP_COLUMNS;
            bindAddress(update, next, address);
            update.setLong(next + 4, expectedRevision);
            update.setBoolean(next + 5, tombstone);
            try (var rows = update.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                long documentId = rows.getLong(1);
                // A carried CORE keeps its metadata, link and concept rows; a write or source
                // replaces them, and a tombstone, which carries none, clears them.
                if (!carried) {
                    writeMetadata(connection, documentId, lookup);
                    writeLinks(connection, documentId, commit.links());
                    writeConcepts(connection, documentId, commit.concepts());
                }
                return Optional.of(new Applied(documentId, lookup));
            }
        }
    }

    /**
     * Share-locks carried objects, part and blob references alike, so a purge cannot
     * mark them while this commit references them. Rows are locked in
     * {@code (stack_id, object_key)} order, the order purge takes them in.
     */
    private static Optional<StackRef> checkCarried(Connection connection, List<StackRef> carried) throws SQLException {
        var ordered = carried.stream().sorted(java.util.Comparator.comparing(StackRef::getStackId, CatalogRecords::compareUtf8)
                .thenComparing(StackRef::getKey, CatalogRecords::compareUtf8)).toList();
        try (var select = connection.prepareStatement(
                "SELECT purged, sha256, length FROM catalog_objects WHERE stack_id = ? AND object_key = ? FOR SHARE")) {
            for (var ref : ordered) {
                select.setString(1, ref.getStackId());
                select.setString(2, ref.getKey());
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) throw new CatalogDataLoss("Carried object is not in the catalog: " + ref.getKey());
                    if (!rows.getString(2).equals(ref.getSha256()) || rows.getLong(3) != ref.getLength())
                        throw new CatalogDataLoss("Carried object differs from its catalog record: " + ref.getKey());
                    if (rows.getBoolean(1)) return Optional.of(ref);
                }
            }
        }
        return Optional.empty();
    }

    /** Schema artifact rows sent to the database by this process; for tests and diagnostics. */
    public long artifactsSent() { return artifactsSent.sum(); }

    private void remember(List<String> digests) {
        if (knownArtifacts.size() + digests.size() > KNOWN_ARTIFACTS) knownArtifacts.clear();
        knownArtifacts.addAll(digests);
    }

    /** Writes the accession's rows; returns the artifact digests it sent. */
    private List<String> writeAccession(Connection connection, long documentId, Commit commit, long revision,
            Optional<DocumentLookup> lookup) throws SQLException {
        var now = Timestamp.from(Instant.now());
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_objects (stack_id, object_key, sha256, length, created_at) VALUES (?, ?, ?, ?, ?)""")) {
            for (var ref : commit.newObjects()) {
                insert.setString(1, ref.getStackId());
                insert.setString(2, ref.getKey());
                insert.setString(3, ref.getSha256());
                insert.setLong(4, ref.getLength());
                insert.setTimestamp(5, now);
                insert.addBatch();
            }
            insert.executeBatch();
        }
        // Only artifacts this process has not seen committed are sent (#327).
        var sent = new ArrayList<String>();
        try (var insert = connection.prepareStatement(
                "INSERT INTO catalog_schema_artifacts (sha256, content) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
            for (Map.Entry<String, ByteString> artifact : commit.artifacts().entrySet()) {
                if (knownArtifacts.contains(artifact.getKey())) continue;
                var bytes = artifact.getValue().toByteArray();
                if (!sha256Hex(bytes).equals(artifact.getKey()))
                    throw new IllegalArgumentException("Schema artifact differs from its digest");
                insert.setString(1, artifact.getKey());
                insert.setBytes(2, bytes);
                insert.addBatch();
                sent.add(artifact.getKey());
            }
            if (!sent.isEmpty()) insert.executeBatch();
        }
        artifactsSent.add(sent.size());
        var manifest = commit.manifest();
        var manifestBytes = manifest.toByteArray();
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_revisions (document_id, revision, previous_revision, operation_id, intent_sha256,
                    tombstone, validation_status, committed_at, manifest, manifest_sha256, structured_data_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            insert.setLong(1, documentId);
            insert.setLong(2, revision);
            insert.setLong(3, revision - 1);
            insert.setString(4, commit.operationId());
            insert.setString(5, commit.intentSha256());
            insert.setBoolean(6, manifest.getTombstone());
            insert.setShort(7, (short) manifest.getValidation().getStatusValue());
            insert.setTimestamp(8, toTimestamp(manifest.getCommittedAt()));
            insert.setBytes(9, manifestBytes);
            insert.setString(10, sha256Hex(manifestBytes));
            insert.setString(11, lookup.filter(DocumentLookup::hasStructuredDataType)
                    .map(DocumentLookup::getStructuredDataType).orElse(null));
            insert.executeUpdate();
        }
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_part_refs (document_id, revision, ordinal, selector, state, stack_id, object_key,
                    written_in_revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")) {
            for (int ordinal = 0; ordinal < manifest.getPartsCount(); ordinal++) {
                var part = manifest.getParts(ordinal);
                var entry = part.getEntry();
                insert.setLong(1, documentId);
                insert.setLong(2, revision);
                insert.setInt(3, ordinal);
                insert.setBytes(4, entry.getSelector().toByteArray());
                insert.setShort(5, (short) entry.getStateValue());
                boolean present = entry.getState() == PartState.PART_STATE_PRESENT;
                insert.setString(6, present ? part.getRef().getStackId() : null);
                insert.setString(7, present ? part.getRef().getKey() : null);
                insert.setLong(8, entry.getWrittenInRevision());
                insert.addBatch();
            }
            insert.executeBatch();
        }
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_blob_refs (document_id, revision, blob_id, stack_id, object_key)
                VALUES (?, ?, ?, ?, ?)""")) {
            for (var part : manifest.getPartsList()) {
                for (var blob : part.getBlobRefsList()) {
                    insert.setLong(1, documentId);
                    insert.setLong(2, revision);
                    insert.setString(3, blob.getBlobId());
                    insert.setString(4, blob.getRef().getStackId());
                    insert.setString(5, blob.getRef().getKey());
                    insert.addBatch();
                }
            }
            insert.executeBatch();
        }
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_operation_outcomes (account_id, operation_id, intent_sha256, document_id, revision,
                    outcome, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?)""")) {
            insert.setString(1, commit.address().getAccountId());
            insert.setString(2, commit.operationId());
            insert.setString(3, commit.intentSha256());
            insert.setLong(4, documentId);
            insert.setLong(5, revision);
            insert.setBytes(6, commit.outcome().toByteArray());
            insert.setTimestamp(7, toTimestamp(commit.outcome().getRecordedAt()));
            insert.executeUpdate();
        }
        try (var insert = connection.prepareStatement("""
                INSERT INTO catalog_outbox (event_id, document_id, revision, event_key, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)""")) {
            insert.setString(1, commit.event().getEventId());
            insert.setLong(2, documentId);
            insert.setLong(3, revision);
            insert.setString(4, CatalogRecords.eventKey(commit.address()));
            insert.setBytes(5, commit.event().toByteArray());
            insert.setTimestamp(6, now);
            insert.executeUpdate();
        }
        return sent;
    }

    // ------------------------------------------------------------------ writer authority (administration)

    /** Provisions an immutable grant. Administrative; never reachable from DocumentService. */
    @Override public Grant installGrant(WriterGrant grant) {
        var snapshot = grant.toByteString();
        var sha = sha256Hex(snapshot.toByteArray());
        inTransaction("Grant provisioning", connection -> {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_writer_grants (grant_id, account_id, issuer, credential_id, snapshot,
                        snapshot_sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)""")) {
                insert.setString(1, grant.getGrantId());
                insert.setString(2, grant.getAccountId());
                insert.setString(3, grant.getCredential().getIssuer());
                insert.setObject(4, UUID.fromString(grant.getCredential().getCredentialId()));
                insert.setBytes(5, snapshot.toByteArray());
                insert.setString(6, sha);
                insert.setTimestamp(7, Timestamp.from(Instant.now()));
                insert.executeUpdate();
            }
            return null;
        });
        return new Grant(grant, snapshot, sha, false);
    }

    /** Revokes a grant. Final; prior evidence is kept. */
    @Override public void revokeGrant(String grantId) {
        inTransaction("Grant revocation", connection -> {
            try (var update = connection.prepareStatement("UPDATE catalog_writer_grants SET revoked = true WHERE grant_id = ?")) {
                update.setString(1, grantId);
                if (update.executeUpdate() != 1) throw new IllegalArgumentException("Unknown writer grant " + grantId);
            }
            return null;
        });
    }

    /** Grants for one account and credential, by unsigned UTF-8 grant ID. */
    @Override public List<Grant> grants(String accountId, String issuer, UUID credentialId) {
        return inTransaction("Grant lookup", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT snapshot, snapshot_sha256, revoked FROM catalog_writer_grants
                    WHERE account_id = ? AND issuer = ? AND credential_id = ?""")) {
                select.setString(1, accountId);
                select.setString(2, issuer);
                select.setObject(3, credentialId);
                var result = new ArrayList<Grant>();
                try (var rows = select.executeQuery()) {
                    while (rows.next()) result.add(grant(rows.getBytes(1), rows.getString(2), rows.getBoolean(3)));
                }
                result.sort((left, right) -> compareUtf8(left.grant().getGrantId(), right.grant().getGrantId()));
                return result;
            }
        });
    }

    /** Whether any grant in the account covers this owner scope, for any credential. */
    @Override public boolean ownerKnown(String accountId, java.util.function.Predicate<WriterGrant> covers) {
        return inTransaction("Owner lookup", connection -> {
            try (var select = connection.prepareStatement(
                    "SELECT snapshot, snapshot_sha256, revoked FROM catalog_writer_grants WHERE account_id = ?")) {
                select.setString(1, accountId);
                try (var rows = select.executeQuery()) {
                    while (rows.next()) if (covers.test(grant(rows.getBytes(1), rows.getString(2), rows.getBoolean(3)).grant()))
                        return true;
                }
            }
            return false;
        });
    }

    /** Registers a credential at generation 1. Administrative. */
    @Override public void registerCredential(String issuer, UUID credentialId, String principal) {
        inTransaction("Credential registration", connection -> {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_credential_authorities (issuer, credential_id, generation, principal)
                    VALUES (?, ?, 1, ?)""")) {
                insert.setString(1, issuer);
                insert.setObject(2, credentialId);
                insert.setString(3, principal);
                insert.executeUpdate();
            }
            return null;
        });
    }

    /** Moves a credential to its next generation. Administrative. */
    @Override public long rotateCredential(String issuer, UUID credentialId) {
        return inTransaction("Credential rotation", connection -> {
            try (var update = connection.prepareStatement("""
                    UPDATE catalog_credential_authorities SET generation = generation + 1
                    WHERE issuer = ? AND credential_id = ? RETURNING generation""")) {
                update.setString(1, issuer);
                update.setObject(2, credentialId);
                try (var rows = update.executeQuery()) {
                    if (!rows.next()) throw new IllegalArgumentException("Unknown credential");
                    return rows.getLong(1);
                }
            }
        });
    }

    /** Revokes a credential's current generation. Final. */
    @Override public void revokeCredential(String issuer, UUID credentialId) {
        inTransaction("Credential revocation", connection -> {
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_credential_authorities SET revoked = true WHERE issuer = ? AND credential_id = ?")) {
                update.setString(1, issuer);
                update.setObject(2, credentialId);
                if (update.executeUpdate() != 1) throw new IllegalArgumentException("Unknown credential");
            }
            return null;
        });
    }

    /** The credential's authority row. */
    @Override public Optional<Credential> credential(String issuer, UUID credentialId) {
        return inTransaction("Credential lookup", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT generation, principal, revoked FROM catalog_credential_authorities
                    WHERE issuer = ? AND credential_id = ?""")) {
                select.setString(1, issuer);
                select.setObject(2, credentialId);
                try (var rows = select.executeQuery()) {
                    return rows.next() ? Optional.of(new Credential(rows.getLong(1), rows.getString(2), rows.getBoolean(3)))
                            : Optional.empty();
                }
            }
        });
    }

    // ------------------------------------------------------------------ outbox

    /** Unrelayed events in commit order. */
    @Override public List<OutboxEvent> pendingEvents(int limit) {
        return inTransaction("Outbox claim", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT event_id, event_key, payload, attempts FROM catalog_outbox WHERE relayed_at IS NULL
                    ORDER BY sequence LIMIT ?""")) {
                select.setInt(1, limit);
                var result = new ArrayList<OutboxEvent>();
                try (var rows = select.executeQuery()) {
                    while (rows.next()) result.add(new OutboxEvent(rows.getString(1), rows.getString(2),
                            parseEvent(rows.getBytes(3)), rows.getInt(4)));
                }
                return result;
            }
        });
    }

    /** Stamps an event relayed after the broker acknowledged it. */
    @Override public boolean markRelayed(String eventId) {
        return inTransaction("Outbox relay stamp", connection -> {
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_outbox SET relayed_at = ? WHERE event_id = ? AND relayed_at IS NULL")) {
                update.setTimestamp(1, Timestamp.from(Instant.now()));
                update.setString(2, eventId);
                return update.executeUpdate() == 1;
            }
        });
    }

    /** Stamps a batch of events relayed after the broker acknowledged them. */
    @Override public int markRelayed(List<String> eventIds) {
        if (eventIds.isEmpty()) return 0;
        return inTransaction("Outbox relay stamp", connection -> {
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_outbox SET relayed_at = ? WHERE event_id = ANY(?) AND relayed_at IS NULL")) {
                update.setTimestamp(1, Timestamp.from(Instant.now()));
                update.setArray(2, connection.createArrayOf("text", eventIds.toArray()));
                return update.executeUpdate();
            }
        });
    }

    /** Counts a failed relay attempt; the event stays pending. */
    @Override public void markAttempt(String eventId) {
        inTransaction("Outbox attempt", connection -> {
            try (var update = connection.prepareStatement("UPDATE catalog_outbox SET attempts = attempts + 1 WHERE event_id = ?")) {
                update.setString(1, eventId);
                update.executeUpdate();
            }
            return null;
        });
    }

    /** Counts a failed relay attempt for each named event; the events stay pending. */
    @Override public void markAttempts(List<String> eventIds) {
        if (eventIds.isEmpty()) return;
        inTransaction("Outbox attempt", connection -> {
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_outbox SET attempts = attempts + 1 WHERE event_id = ANY(?)")) {
                update.setArray(1, connection.createArrayOf("text", eventIds.toArray()));
                update.executeUpdate();
            }
            return null;
        });
    }

    /** Every outbox event of one document, in revision order. */
    @Override public List<DocumentEvent> events(NodeAddress address) {
        return inTransaction("Outbox read", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT o.payload FROM catalog_outbox o JOIN catalog_documents d ON d.document_id = o.document_id
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                    ORDER BY o.sequence""")) {
                bindAddress(select, 1, address);
                var result = new ArrayList<DocumentEvent>();
                try (var rows = select.executeQuery()) { while (rows.next()) result.add(parseEvent(rows.getBytes(1))); }
                return result;
            }
        });
    }

    @Override public List<DocumentEvent> events(NodeAddress address, List<Long> revisions) {
        if (revisions.isEmpty()) return List.of();
        return inTransaction("Outbox page read", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT o.payload FROM catalog_outbox o JOIN catalog_documents d ON d.document_id = o.document_id
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                      AND o.revision = ANY(?) ORDER BY o.revision""")) {
                bindAddress(select, 1, address);
                select.setArray(5, connection.createArrayOf("bigint", revisions.toArray()));
                var result = new ArrayList<DocumentEvent>();
                try (var rows = select.executeQuery()) { while (rows.next()) result.add(parseEvent(rows.getBytes(1))); }
                return result;
            }
        });
    }

    // ------------------------------------------------------------------ helpers

    @FunctionalInterface private interface Work<T> { T run(Connection connection) throws SQLException; }

    private <T> T inTransaction(String operation, Work<T> work) {
        try (var connection = dataSource.getConnection()) {
            try {
                var result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException failure) {
                rollbackQuietly(connection, failure);
                throw CatalogException.from(operation, failure);
            } catch (RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw CatalogException.from(operation, failure);
        }
    }

    private static void rollbackQuietly(Connection connection, SQLException failure) {
        try { connection.rollback(); } catch (SQLException rollback) { failure.addSuppressed(rollback); }
    }

    private static Optional<Outcome> selectOutcome(Connection connection, String accountId, String operationId)
            throws SQLException {
        try (var select = connection.prepareStatement(
                """
                SELECT intent_sha256, outcome, rejection_address, rejection_create, rejection_issuer, rejection_credential_id
                FROM catalog_operation_outcomes WHERE account_id = ? AND operation_id = ?""")) {
            select.setString(1, accountId);
            select.setString(2, operationId);
            try (var rows = select.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                try {
                    var addressBytes = rows.getBytes(3);
                    Optional<CatalogRecords.RejectionAccess> access = Optional.empty();
                    if (addressBytes != null) {
                        var issuer = rows.getString(5);
                        var creator = issuer == null ? Optional.<CatalogRecords.CredentialIdentity>empty()
                                : Optional.of(new CatalogRecords.CredentialIdentity(issuer, rows.getObject(6, java.util.UUID.class)));
                        access = Optional.of(new CatalogRecords.RejectionAccess(NodeAddress.parseFrom(addressBytes), rows.getBoolean(4), creator));
                    }
                    return Optional.of(new Outcome(rows.getString(1), RecordedOutcome.parseFrom(rows.getBytes(2)), access));
                } catch (InvalidProtocolBufferException corrupt) {
                    throw new CatalogDataLoss("Recorded outcome does not decode", corrupt);
                }
            }
        }
    }

    private static Optional<Current> selectCurrent(Connection connection, NodeAddress address, boolean lock)
            throws SQLException {
        try (var select = connection.prepareStatement("""
                SELECT current_revision, tombstone FROM catalog_documents
                WHERE NOT purged AND account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ?""" + (lock ? " FOR UPDATE" : ""))) {
            bindAddress(select, 1, address);
            try (var rows = select.executeQuery()) {
                return rows.next() ? Optional.of(new Current(rows.getLong(1), rows.getBoolean(2))) : Optional.empty();
            }
        }
    }

    private static void bindAddress(PreparedStatement statement, int first, NodeAddress address) throws SQLException {
        statement.setString(first, address.getAccountId());
        statement.setString(first + 1, address.getGraphId());
        statement.setString(first + 2, address.getGraphAddressId());
        statement.setString(first + 3, address.getDocId());
    }

    private static StoredManifest parseManifest(byte[] bytes, String sha256, NodeAddress address, long revision) {
        if (!sha256Hex(bytes).equals(sha256)) throw new CatalogDataLoss("Stored manifest differs from its digest");
        try {
            var manifest = StoredManifest.parseFrom(bytes);
            if (!manifest.getAddress().equals(address) || manifest.getRevision() != revision)
                throw new CatalogDataLoss("Stored manifest describes another revision");
            return manifest;
        } catch (InvalidProtocolBufferException corrupt) {
            throw new CatalogDataLoss("Stored manifest does not decode", corrupt);
        }
    }

    private static DocumentEvent parseEvent(byte[] bytes) {
        try {
            return DocumentEvent.parseFrom(bytes);
        } catch (InvalidProtocolBufferException corrupt) {
            throw new CatalogDataLoss("Outbox event does not decode", corrupt);
        }
    }

    private static Grant grant(byte[] snapshot, String sha256, boolean revoked) {
        if (!sha256Hex(snapshot).equals(sha256)) throw new CatalogDataLoss("Writer grant snapshot differs from its digest");
        try {
            return new Grant(WriterGrant.parseFrom(snapshot), ByteString.copyFrom(snapshot), sha256, revoked);
        } catch (InvalidProtocolBufferException corrupt) {
            throw new CatalogDataLoss("Writer grant snapshot does not decode", corrupt);
        }
    }

    private static Timestamp toTimestamp(com.google.protobuf.Timestamp value) {
        return Timestamp.from(Instant.ofEpochSecond(value.getSeconds(), value.getNanos()));
    }


    static int compareUtf8(String left, String right) { return CatalogRecords.compareUtf8(left, right); }

    static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
