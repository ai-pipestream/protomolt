package ai.protomolt.proto.repo.container.ledger;

/**
 * The explicit origin discriminator of a {@link DocumentRecord} row.
 * <p>
 * Every row is kind-qualified: nothing infers a row's flavor from blank
 * fields anymore. The kind drives the storage key layout and which shape of
 * {@code graph_id} the row is allowed to carry — a rule the database enforces
 * (see {@code chk_documents_row_kind}), so an unrepresentable row can never
 * be written even by a buggy caller:
 * <ul>
 *   <li>{@link #INTAKE}: the datasource-scoped staging copy written by the
 *   intake layer. {@code graph_id} is the account's intake graph
 *   {@code "intake:<accountId>"} (the intake layer is its own single-node
 *   graph, addressed at the datasource node) and {@code cluster_id} is always
 *   NULL.</li>
 *   <li>{@link #PIPELINE}: a post-hop copy staged at a graph node.
 *   {@code graph_id} is a real graph id, never an intake graph.</li>
 * </ul>
 */
public final class DocumentRowKind {

    /** Datasource-scoped staging copy written by the intake layer. */
    public static final String INTAKE = "INTAKE";

    /** Post-hop copy staged at a processing-graph node. */
    public static final String PIPELINE = "PIPELINE";

    private DocumentRowKind() {
    }
}
