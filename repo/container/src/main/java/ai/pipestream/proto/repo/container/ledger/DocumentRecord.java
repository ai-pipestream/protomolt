package ai.pipestream.proto.repo.container.ledger;

import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentSecurity;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Persisted document ledger record — one row per stored document state.
 * <p>
 * The actual document bytes live in object storage as independently
 * addressable part objects; this row is the machine-readable claim check:
 * where the parts live, what they hash to, and what the row's lifecycle is.
 * <p>
 * <b>Storage identity.</b> {@link #nodeId} is a deterministic UUIDv5 minted
 * by the caller over the four-segment storage identity
 * {@code hash(doc_id | graph_address_id | account_id | graph_id)}; the same
 * four segments also form the {@code uq_documents_identity} unique
 * constraint, so the identity is enforced twice — once by key derivation and
 * once by the database. The graph segment namespaces pipeline hops: two
 * independent graphs that name a node identically (e.g. {@code "opensearch-sink"})
 * while processing the same multicast document must never resolve to the
 * same row.
 * <p>
 * <b>Row kind and graph shape.</b> Every row is kind-qualified
 * ({@link DocumentRowKind}) and {@code graph_id} is required and non-blank
 * on both flavors. INTAKE rows carry the account's intake graph
 * {@code "intake:<accountId>"} (the intake layer is its own single-node
 * graph, addressed at the datasource node) and never a cluster; PIPELINE
 * rows carry their real graph id. The {@code chk_documents_row_kind} check
 * constraint mirrors that rule so blank-graph or mis-shaped rows are
 * unrepresentable.
 * <p>
 * <b>The {@code updated_at} staleness guard.</b> {@link #updatedAt} is set
 * on insert and on every body (re)write — but deliberately NOT by
 * status-only transitions (a tombstone to
 * {@link DocumentStatus#PENDING_PURGE}, a reprocess marker). A reclaim/purge
 * compares {@code updated_at} against the reclaim's requested_at: if the
 * body was re-staged after the reclaim was requested, the (now-stale) purge
 * is voided. That comparison only works if bookkeeping updates cannot move
 * the timestamp, hence {@code @PrePersist} defaults ONLY — never
 * {@code @PreUpdate}. Body rewrites set {@code updatedAt} explicitly.
 */
@Entity
@Table(name = "documents", check = @jakarta.persistence.CheckConstraint(
        name = "chk_documents_row_kind", constraint = "graph_id <> '' AND ("
        + "(row_kind = 'INTAKE' AND graph_id LIKE 'intake:%' AND cluster_id IS NULL) OR "
        + "(row_kind = 'PIPELINE' AND graph_id NOT LIKE 'intake:%'))"))
public class DocumentRecord {

    /** Default constructor required by the JPA/Hibernate persistence provider. */
    public DocumentRecord() {
    }

    /**
     * Repository's unique identifier (UUID) — primary key. Deterministic
     * UUIDv5 of the four-segment storage identity, minted by the caller; the
     * ledger never generates it.
     */
    @Id
    @Column(name = "node_id", nullable = false)
    public UUID nodeId;

    /**
     * Logical document identifier, stable across all pipeline states.
     * Multiple rows can share a doc_id (one per storage address).
     */
    @Column(name = "doc_id", nullable = false)
    public String docId;

    /**
     * The stored coordinate (destination address) this row describes: a
     * datasource id for intake rows, a graph node address for pipeline rows.
     * Second segment of the storage identity.
     */
    @Column(name = "graph_address_id", nullable = false)
    public String graphAddressId;

    /**
     * The owning graph's id — REQUIRED and non-blank on every row; the
     * fourth segment of the storage identity. INTAKE rows carry the account's
     * intake graph {@code "intake:<accountId>"}; PIPELINE rows carry their
     * real graph id, so two graphs' same-named hops never collide on one
     * row. Blank-graph rows are unrepresentable (check constraint).
     */
    @Column(name = "graph_id", nullable = false)
    public String graphId;

    /**
     * The explicit origin discriminator of this row
     * ({@link DocumentRowKind#INTAKE} | {@link DocumentRowKind#PIPELINE}),
     * mirrored from the save request's address flavor. Drives the storage key
     * layout and the catalog-status transition — nothing infers any of those
     * from blank fields.
     */
    @Column(name = "row_kind", nullable = false)
    public String rowKind;

    /**
     * Routing hint only — NEVER identity. Always NULL for INTAKE rows (the
     * intake layer is its own single-node graph, not part of a processing
     * cluster — enforced by the row-kind check constraint); set to the
     * cluster name for PIPELINE rows processed by a cluster. No storage path,
     * key derivation, or lookup may read this field.
     */
    @Column(name = "cluster_id")
    public String clusterId;

    /** Identifier of the account that owns this document. */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** Identifier of the datasource the document was ingested from. */
    @Column(name = "datasource_id", nullable = false)
    public String datasourceId;

    /** Identifier of the connector that produced the document (nullable). */
    @Column(name = "connector_id")
    public String connectorId;

    /**
     * Root checksum of the part set — the dedupe key. An intake save whose
     * incoming checksum matches the stored AVAILABLE row is elided (the
     * object PUT is skipped and processing restarts from the existing body);
     * see {@link #reprocessCount}.
     */
    @Column(name = "checksum", nullable = false)
    public String checksum;

    /** Name of the drive whose bucket holds the part objects. */
    @Column(name = "drive_name", nullable = false)
    public String driveName;

    /** Storage prefix of the part objects (their keys share this root). */
    @Column(name = "object_key", nullable = false, length = 1024)
    public String objectKey;

    /** Object version identifier of the stored CORE part (nullable). */
    @Column(name = "version_id")
    public String versionId;

    /** Entity tag (ETag) of the CORE part object. */
    @Column(name = "etag", nullable = false)
    public String etag;

    /** Total size of the stored part set in bytes. */
    @Column(name = "size_bytes", nullable = false)
    public Long sizeBytes;

    /** MIME content type of the source document body (nullable). */
    @Column(name = "content_type")
    public String contentType;

    /** Original filename of the source document (nullable). */
    @Column(name = "filename")
    public String filename;

    /**
     * OIS-aligned document-level security as JSON ({@code jsonb}): a typed
     * {@link DocumentSecurity} (inheritance flag + typed access rules). ACLs
     * ride the row so reads can be filtered without an object-storage round
     * trip. Use {@link #readSecurity()}/{@link #writeSecurity(DocumentSecurity)}
     * rather than handling the raw JSON.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "security")
    public String security;

    /**
     * The part manifest as JSON ({@code jsonb}): one entry per part object
     * (CHUNKS: one per chunk set) with object key, SHA-256, size, lifecycle
     * state and writer attribution — the machine-readable map of the stored
     * state. Serialized/parsed by {@link DocumentPartCodec}; use
     * {@link #readManifest()}/{@link #writeManifest(DocumentManifest)} rather
     * than handling the raw JSON. Null only for rows that never carried a
     * body through the part-split store (there is no monolithic fallback).
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "part_manifest")
    public String partManifest;

    /**
     * Source-blob settle policy stamp: when {@code true}, terminal-successful
     * settlement of this row triggers the source-blob purge (delete the
     * intake blob part, tombstone it in the manifest). Stamped at intake save
     * from the hydration config or forced by RTBF.
     */
    @Column(name = "delete_source_blobs_on_settle", nullable = false)
    public boolean deleteSourceBlobsOnSettle;

    /**
     * Provenance reason recorded on the eventual blob tombstone:
     * {@code "POST_PARSE_POLICY"} or {@code "RTBF"}. Null = default policy.
     */
    @Column(name = "source_blob_delete_reason")
    public String sourceBlobDeleteReason;

    /**
     * Storage status ({@link DocumentStatus}): AVAILABLE (default — parts are
     * in object storage and readable), PENDING_PURGE (soft-deleted,
     * awaiting object deletion by the background purger), PURGE_FAILED
     * (object deletion failed after max attempts). Status-only transitions
     * must NOT bump {@link #updatedAt} — see the class Javadoc.
     */
    @Column(name = "status", nullable = false)
    public String status = DocumentStatus.AVAILABLE;

    /**
     * Crawl run identifier carried from the connector's crawl request. Lets
     * all documents from one crawl run be listed/filtered together.
     * Nullable: unset for paths with no crawl context.
     */
    @Column(name = "crawl_id")
    public String crawlId;

    /**
     * How many times a save for this row was elided by checksum dedupe
     * (incoming checksum matched the stored AVAILABLE row, so the object PUT
     * was skipped and processing restarted from the existing body).
     */
    @Column(name = "reprocess_count", nullable = false)
    public int reprocessCount;

    /**
     * When checksum dedupe last elided a save for this row. Null until the
     * first dedupe hit.
     */
    @Column(name = "last_reprocessed_at")
    public Instant lastReprocessedAt;

    /** Timestamp when this record was first created. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /**
     * When the staged body was last (re)written. Set on insert and on every
     * store/re-stage, but deliberately NOT bumped by status-only transitions
     * (a tombstone to PENDING_PURGE, a reprocess marker). A reclaim/purge
     * compares this against the reclaim's requested_at: if the body was
     * re-staged after the reclaim was requested, the (now-stale) purge is
     * voided — the re-emit revives it.
     */
    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;

    /**
     * Deserialize {@link #partManifest} into the typed manifest via the
     * claim-check codec. Null when the row carries no manifest.
     *
     * @return the parsed manifest, or null
     */
    public DocumentManifest readManifest() {
        return partManifest == null ? null : DocumentPartCodec.manifestFromJson(partManifest);
    }

    /**
     * Serialize the typed manifest into {@link #partManifest} via the
     * claim-check codec. Null clears the column.
     *
     * @param manifest the manifest to store, or null
     */
    public void writeManifest(DocumentManifest manifest) {
        this.partManifest = manifest == null ? null : DocumentPartCodec.manifestToJson(manifest);
    }

    /**
     * Deserialize {@link #security} into the typed security payload. Null
     * when the row carries no security metadata.
     *
     * @return the parsed security, or null
     */
    public DocumentSecurity readSecurity() {
        if (security == null) {
            return null;
        }
        DocumentSecurity.Builder builder = DocumentSecurity.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(security, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new LedgerException("unparseable security JSON on document row " + nodeId, e);
        }
        return builder.build();
    }

    /**
     * Serialize the typed security payload into {@link #security}. Null
     * clears the column.
     *
     * @param documentSecurity the security payload to store, or null
     */
    public void writeSecurity(DocumentSecurity documentSecurity) {
        if (documentSecurity == null) {
            this.security = null;
            return;
        }
        try {
            this.security = JsonFormat.printer().print(documentSecurity);
        } catch (InvalidProtocolBufferException e) {
            throw new LedgerException("unprintable DocumentSecurity", e);
        }
    }

    /**
     * Default the audit timestamps on insert so any persist path (including
     * tests and callers that don't set them) satisfies the NOT NULL columns.
     * Deliberately {@code @PrePersist} only — NOT {@code @PreUpdate} — so a
     * status-only update (e.g. a tombstone to PENDING_PURGE) does NOT bump
     * {@link #updatedAt}; only an actual body (re)write does, via an explicit
     * set at the call site.
     */
    @PrePersist
    void onPrePersist() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }
}
