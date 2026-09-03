/**
 * The search service: the user-facing query surface over indexed documents and
 * the indexing RPC durable workflows drive.
 *
 * <p>The service serves a fixed set of <em>mapping subjects</em>
 * ({@link ai.protomolt.proto.search.service.ServedMapping}): each names an
 * index mapping (the queryable surface), the document identity, and
 * optionally a chunk lane whose
 * {@link ai.protomolt.proto.search.index.spi.ChunkingPolicy} derives per-chunk
 * vectors at index time through the embedding lane. Every request is gated
 * by membership — unknown subjects, fields outside the mapping, and vector
 * queries against subjects without a policy are refused loudly by name.
 *
 * <p>{@link ai.protomolt.proto.search.service.SearchServices} wires the
 * stack; {@link ai.protomolt.proto.search.service.SearchServiceModule} mounts it
 * as the {@code search} role over a co-mounted or remote {@code repo} role.
 * {@link ai.protomolt.proto.search.service.RepoDocumentMapping} is the
 * out-of-the-box subject over repository documents.
 */
package ai.protomolt.proto.search.service;
