package ai.pipestream.proto.search.service;

import ai.pipestream.proto.search.index.spi.ChunkingPolicy;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import com.google.protobuf.Message;
import java.util.function.Function;

/**
 * One mapping subject the service serves: the index mapping that gates what is
 * indexed and queryable, the document identity, and (optionally) the chunk
 * lane that derives per-chunk vectors at index time.
 *
 * @param mapping the index mapping; its TEXT fields are the lexical query
 *        surface and every indexed field comes from it
 * @param docIdField the index field name carrying the document identity;
 *        re-indexing replaces by this term
 * @param docId reads the document identity off the source message; a blank
 *        identity is refused at index time
 * @param chunkLane the chunk-and-embed lane, or {@code null} when the
 *        subject serves no vector lane
 */
public record ServedMapping(
        IndexMapping mapping,
        String docIdField,
        Function<Message, String> docId,
        ChunkLane chunkLane) {

    /** Validates the subject definition. */
    public ServedMapping {
        if (mapping == null) {
            throw new IllegalArgumentException("mapping must not be null");
        }
        if (docIdField == null || docIdField.isBlank()) {
            throw new IllegalArgumentException("docIdField must not be blank");
        }
        if (docId == null) {
            throw new IllegalArgumentException("docId must not be null");
        }
    }

    /**
     * The chunk-and-embed lane of a subject: which text chunks, and under
     * which policy.
     *
     * @param policy the chunking policy; the embedding model it names is
     *        resolved through the ServiceLoader when the service mounts
     * @param sourceField the index field name of the chunked source text;
     *        with the policy's {@code vectorField} empty, the vector field
     *        follows the engine convention {@code "<sourceField>#<model>"}
     * @param sourceText reads the chunk source text off the source message;
     *        blank text derives no chunks
     */
    public record ChunkLane(
            ChunkingPolicy policy,
            String sourceField,
            Function<Message, String> sourceText) {

        /** Validates the lane definition. */
        public ChunkLane {
            if (policy == null) {
                throw new IllegalArgumentException("policy must not be null");
            }
            if (sourceField == null || sourceField.isBlank()) {
                throw new IllegalArgumentException("sourceField must not be blank");
            }
            if (sourceText == null) {
                throw new IllegalArgumentException("sourceText must not be null");
            }
        }

        /** The index field name the derived chunk vectors live under. */
        public String vectorField() {
            return policy.vectorField().isEmpty()
                    ? sourceField + "#" + policy.embedding().model()
                    : policy.vectorField();
        }
    }
}
