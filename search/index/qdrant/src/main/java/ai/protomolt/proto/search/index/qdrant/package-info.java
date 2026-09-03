/**
 * Qdrant search-index provider: a {@code SearchEngineIndexer} plugin that maps repo
 * Documents to Qdrant points (one per embedded semantic chunk, named vectors keyed by
 * embedding model, deterministic UUIDv5-style point ids) plus a thin gRPC sink that
 * ensures the collection and upserts the points.
 *
 * <p>The Qdrant gRPC API protos under {@code src/main/proto} are vendored verbatim from
 * the Qdrant project (v1.18.3, Apache-2.0); see the provenance header in each file.
 */
package ai.protomolt.proto.search.index.qdrant;
