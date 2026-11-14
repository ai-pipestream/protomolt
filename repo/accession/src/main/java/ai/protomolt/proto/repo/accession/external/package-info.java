/**
 * Versioned external-system bindings, verified {@code ExternalLink} projections, and
 * durable publication/reconciliation operation ledgers over the parts-based v2
 * {@link ai.protomolt.proto.repo.v2.DocumentServiceGrpc DocumentService} and the
 * registry config lane.
 *
 * <p>ProtoMolt owns binding configuration revisions, repository source-revision
 * association, publication attempt tracking, provider status reconciliation, and
 * monotonic replacement/deletion/purge fencing. External NLP or embedding engines
 * remain stateless producers of input/result digests and processing receipts.
 *
 * <p>All durable state is stored outside the source document:
 * <ul>
 *   <li>Versioned {@link ai.protomolt.proto.repo.external.v1.ExternalSystemBinding}
 *       records live in {@code external-bindings:<account_id>} (and optionally mirror
 *       through {@link ai.protomolt.proto.registry.ConfigDocumentStore} via
 *       {@link ai.protomolt.proto.registry.ConfigSupport#gate}).</li>
 *   <li>Per-(account, binding, source_address) publication ledgers live in
 *       {@code external-publications:<account_id>} as typed {@code CORE} documents.</li>
 * </ul>
 * Never mutating the source document's {@code CORE} part avoids bumping the source
 * document's revision counter or emitting synthetic source accession events, preventing
 * circular publication loops.
 */
package ai.protomolt.proto.repo.accession.external;
