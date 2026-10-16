package ai.protomolt.proto.repo.catalog;

import ai.protomolt.proto.repo.catalog.CatalogRecords.Commit;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CommitResult;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Committed;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Conflict;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Credential;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Current;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Grant;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Outcome;
import ai.protomolt.proto.repo.catalog.CatalogRecords.OutboxEvent;
import ai.protomolt.proto.repo.catalog.CatalogRecords.Recorded;
import ai.protomolt.proto.repo.catalog.CatalogRecords.SourcePurged;
import ai.protomolt.proto.repo.catalog.CatalogRecords.WriteDenied;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.RecordedOutcome;
import ai.protomolt.proto.repo.v2.StackRecord;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.WriterGrant;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
        this.owned = new HikariDataSource(hikari);
        this.dataSource = owned;
        try {
            CatalogBaseline.apply(dataSource);
        } catch (RuntimeException failure) {
            owned.close();
            throw failure;
        }
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
                      AND r.revision = ?""")) {
                bindAddress(select, 1, address);
                select.setLong(5, revision);
                try (var rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    return Optional.of(parseManifest(rows.getBytes(1), rows.getString(2), address, revision));
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
                      AND r.revision < ? ORDER BY r.revision DESC LIMIT ?""")) {
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
                    var content = ByteString.copyFrom(rows.getBytes(1));
                    if (!sha256Hex(content.toByteArray()).equals(sha256))
                        throw new CatalogDataLoss("Retained schema artifact differs from its digest");
                    return Optional.of(content);
                }
            }
        });
    }

    // ------------------------------------------------------------------ commit

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
                var denied = recheckAuthority(connection, commit);
                if (denied.isPresent()) {
                    connection.rollback();
                    return denied.get();
                }
                var documentId = compareAndSet(connection, commit.address(), commit.expected().hasNone(), next - 1,
                        manifest.getTombstone());
                if (documentId.isEmpty()) {
                    connection.rollback();
                    conflicted = true;
                } else {
                    var purged = checkCarried(connection, commit.carriedObjects());
                    if (purged.isPresent()) {
                        connection.rollback();
                        return new SourcePurged(purged.get());
                    }
                    var sent = writeAccession(connection, documentId.get(), commit, next);
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
            var current = selectCurrent(connection, commit.address(), false);
            return new Conflict(current.map(Current::revision).orElse(0L), current.map(Current::tombstone).orElse(false));
        });
    }

    /**
     * Records a terminal rejection. Returns the stored outcome, which is another
     * request's when the operation ID was already recorded.
     */
    @Override public Outcome recordRejection(String accountId, String operationId, String intentSha256, RecordedOutcome outcome) {
        if (!outcome.hasRejection()) throw new IllegalArgumentException("Only a rejection is recorded without an accession");
        return inTransaction("Rejection record", connection -> {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_operation_outcomes (account_id, operation_id, intent_sha256, outcome, recorded_at)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""")) {
                insert.setString(1, accountId);
                insert.setString(2, operationId);
                insert.setString(3, intentSha256);
                insert.setBytes(4, outcome.toByteArray());
                insert.setTimestamp(5, toTimestamp(outcome.getRecordedAt()));
                insert.executeUpdate();
            }
            return selectOutcome(connection, accountId, operationId)
                    .orElseThrow(() -> new IllegalStateException("Recorded rejection is not readable"));
        });
    }

    private Optional<CommitResult> recheckAuthority(Connection connection, Commit commit) throws SQLException {
        // Every grant authorizing this write, in unsigned UTF-8 order so two commits
        // naming the same grants take their share locks in one order.
        try (var lock = connection.prepareStatement("SELECT snapshot_sha256, revoked FROM lock_catalog_writer_grant(?)")) {
            for (var grantId : commit.grantIds()) {
                lock.setString(1, grantId);
                try (var rows = lock.executeQuery()) {
                    if (!rows.next()) return Optional.of(new WriteDenied("writer grant " + grantId + " no longer exists"));
                    if (rows.getBoolean(2)) return Optional.of(new WriteDenied("writer grant " + grantId + " is revoked"));
                }
            }
        }
        if (commit.credential().isPresent()) {
            var credential = commit.credential().get();
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

    /** Moves the pointer from {@code expectedRevision}; empty when the document is elsewhere. */
    private static Optional<Long> compareAndSet(Connection connection, NodeAddress address, boolean create,
            long expectedRevision, boolean tombstone) throws SQLException {
        var now = Timestamp.from(Instant.now());
        if (create) {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO catalog_documents (account_id, graph_id, graph_address_id, doc_id, current_revision,
                        tombstone, updated_at)
                    VALUES (?, ?, ?, ?, 1, ?, ?) ON CONFLICT DO NOTHING RETURNING document_id""")) {
                bindAddress(insert, 1, address);
                insert.setBoolean(5, tombstone);
                insert.setTimestamp(6, now);
                try (var rows = insert.executeQuery()) { return rows.next() ? Optional.of(rows.getLong(1)) : Optional.empty(); }
            }
        }
        try (var update = connection.prepareStatement("""
                UPDATE catalog_documents SET current_revision = current_revision + 1, tombstone = ?, updated_at = ?
                WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ? AND current_revision = ?
                RETURNING document_id""")) {
            update.setBoolean(1, tombstone);
            update.setTimestamp(2, now);
            bindAddress(update, 3, address);
            update.setLong(7, expectedRevision);
            try (var rows = update.executeQuery()) { return rows.next() ? Optional.of(rows.getLong(1)) : Optional.empty(); }
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
    private List<String> writeAccession(Connection connection, long documentId, Commit commit, long revision)
            throws SQLException {
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
                    tombstone, validation_status, committed_at, manifest, manifest_sha256)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
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
            insert.setString(4, commit.address().getDocId());
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

    /** Every outbox event of one document, in revision order. */
    @Override public List<DocumentEvent> events(NodeAddress address) {
        return inTransaction("Outbox read", connection -> {
            try (var select = connection.prepareStatement("""
                    SELECT o.payload FROM catalog_outbox o JOIN catalog_documents d ON d.document_id = o.document_id
                    WHERE d.account_id = ? AND d.graph_id = ? AND d.graph_address_id = ? AND d.doc_id = ?
                    ORDER BY o.revision""")) {
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
                "SELECT intent_sha256, outcome FROM catalog_operation_outcomes WHERE account_id = ? AND operation_id = ?")) {
            select.setString(1, accountId);
            select.setString(2, operationId);
            try (var rows = select.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                try {
                    return Optional.of(new Outcome(rows.getString(1), RecordedOutcome.parseFrom(rows.getBytes(2))));
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
                WHERE account_id = ? AND graph_id = ? AND graph_address_id = ? AND doc_id = ?""" + (lock ? " FOR UPDATE" : ""))) {
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
