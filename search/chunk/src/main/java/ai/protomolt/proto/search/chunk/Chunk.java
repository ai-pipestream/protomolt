package ai.protomolt.proto.search.chunk;

/**
 * One derived chunk. Offsets index the ORIGINAL source text
 * ({@code [startOffset, endOffset)}), so provenance survives the
 * derivation; with overlap configured, consecutive chunks' offset ranges
 * overlap by design. {@link #text()} is exactly the source substring at
 * those offsets.
 *
 * @param ordinal zero-based position in the derivation order; chunk
 *        identity downstream is {@code <doc_id>#<generation>#<ordinal>}
 * @param text the chunk's text, verbatim from the source
 * @param startOffset inclusive start in the source text
 * @param endOffset exclusive end in the source text
 * @param tokenCount the chunk's token count under the policy's rules
 */
public record Chunk(int ordinal, String text, int startOffset, int endOffset, int tokenCount) {

    /** Validates offsets and text agreement. */
    public Chunk {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must not be negative");
        }
        if (startOffset < 0 || endOffset < startOffset) {
            throw new IllegalArgumentException(
                    "offsets out of order: [" + startOffset + ", " + endOffset + ")");
        }
        if (text == null || text.length() != endOffset - startOffset) {
            throw new IllegalArgumentException("text must span exactly [startOffset, endOffset)");
        }
        if (tokenCount <= 0) {
            throw new IllegalArgumentException("a chunk carries at least one token");
        }
    }
}
