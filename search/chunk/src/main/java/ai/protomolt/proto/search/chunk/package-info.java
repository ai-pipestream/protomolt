/**
 * The deterministic chunker. A {@link
 * ai.protomolt.proto.search.index.spi.ChunkingPolicy} names a strategy, a pinned
 * implementation version, and a boundary rule set; {@link
 * ai.protomolt.proto.search.chunk.SentencePackedChunker} executes exactly one
 * (strategy, version, boundary) triple and refuses every other, so a
 * corpus re-chunks only when its policy digest changes, never because a
 * runtime was upgraded underneath it.
 */
package ai.protomolt.proto.search.chunk;
