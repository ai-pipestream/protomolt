package ai.pipestream.proto.repo.container.blob;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Generates deterministic UUIDs for document repository node identifiers.
 * <p>
 * A stored row's identity is the name-based UUID (v5-equivalent) of the
 * four-segment composite key {@code doc_id | graph_address_id | account_id |
 * graph_id}. The graph segment is what namespaces otherwise-identical hops:
 * two independent graphs that both name a node "opensearch-sink" and process
 * the same multicast document resolve to DIFFERENT rows, so one graph
 * finishing and reclaiming its own hop can never destroy the other graph's
 * still-in-flight copy (the 2026-07-15 cross-graph collision incident).
 * <ul>
 *   <li>Intake rows ({@code graph_address_id} = {@code datasource_id}) carry
 *       the account's intake graph {@code "intake:<accountId>"} — the intake
 *       layer is its own single-node graph, and every graph hydrates the same
 *       staged intake copy at that identity.</li>
 *   <li>Pipeline rows ({@code graph_address_id} = a graph node id) carry the
 *       owning graph's id.</li>
 * </ul>
 * The graph segment is REQUIRED on every row: a blank or null graph id
 * is rejected, so the dead "blank means intake" convention is unrepresentable.
 * <p>
 * Determinism still buys the same properties as before: consistent Kafka
 * partitioning, idempotent re-saves, and stable primary keys.
 * <p>
 * <b>Identity re-key:</b> flipping intake rows from a blank graph segment to
 * {@code "intake:<accountId>"} changes every intake node id, so all node ids
 * minted before this change are invalidated. There is deliberately no
 * back-compat resolution path.
 */
public final class DocumentIds {

    /** Separator used in composite key to prevent collisions. */
    private static final String SEPARATOR = "|";

    private DocumentIds() {
    }

    /**
     * Generates a deterministic UUID for a repository node identifier from the
     * four logical identifiers. The composite always has four non-blank
     * segments ({@code doc|addr|acct|graph}).
     *
     * @param docId The document identifier
     * @param graphAddressId The graph address ID (datasource_id for intake, graph node id for pipeline rows)
     * @param accountId The account identifier (for multi-tenancy)
     * @param graphId The graph segment of the row's identity — the account's
     *        intake graph {@code "intake:<accountId>"} for intake rows, the
     *        owning graph's id for pipeline rows. REQUIRED: null or blank is
     *        rejected (blank-graph rows are unrepresentable)
     * @return A deterministic UUID generated from the composite key
     */
    public static UUID nodeId(String docId, String graphAddressId, String accountId, String graphId) {
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException("docId cannot be null or blank");
        }
        if (graphAddressId == null || graphAddressId.isBlank()) {
            throw new IllegalArgumentException("graphAddressId cannot be null or blank");
        }
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("accountId cannot be null or blank");
        }
        if (graphId == null || graphId.isBlank()) {
            // The dead "blank means intake" convention is unrepresentable:
            // intake rows carry the account's intake graph id
            // ("intake:<accountId>"), pipeline rows their graph.
            throw new IllegalArgumentException(
                    "graphId cannot be null or blank (intake rows carry intake:<accountId>)");
        }

        String composite = docId + SEPARATOR + graphAddressId + SEPARATOR + accountId + SEPARATOR + graphId;
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generates a deterministic UUID for a raw-blob object, distinct from the
     * document node id of the same logical document.
     * <p>
     * Re-uploading the same {@code (docId, datasourceId, accountId)} yields the
     * same blob key, so a re-crawl overwrites the blob in place (with S3 bucket
     * versioning preserving history) rather than orphaning a randomly-keyed
     * object. The {@code "blob"} discriminator keeps this id different from
     * {@link #nodeId} for the same inputs.
     *
     * @param docId        The document identifier
     * @param datasourceId The datasource identifier
     * @param accountId    The account identifier
     * @return A deterministic UUID for the raw blob
     */
    public static UUID blobId(String docId, String datasourceId, String accountId) {
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException("docId cannot be null or blank");
        }
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId cannot be null or blank");
        }
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("accountId cannot be null or blank");
        }

        String composite = "blob" + SEPARATOR + docId + SEPARATOR + datasourceId + SEPARATOR + accountId;
        return UUID.nameUUIDFromBytes(composite.getBytes(StandardCharsets.UTF_8));
    }
}
