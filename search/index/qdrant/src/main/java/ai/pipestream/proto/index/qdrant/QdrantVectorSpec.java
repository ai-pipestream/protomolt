package ai.pipestream.proto.index.qdrant;

import qdrant.Collections.Distance;

/**
 * One named vector's collection declaration: its name (a sanitized embedding-model id), the
 * vector size, and the distance function. Sizes and distances come from the plan's VECTOR
 * indexing hints where declared, otherwise from the point data (distance defaulting to
 * {@link Distance#Cosine}).
 */
public record QdrantVectorSpec(String name, int size, Distance distance) {

    public QdrantVectorSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0, got " + size);
        }
        if (distance == null || distance == Distance.UnknownDistance
                || distance == Distance.UNRECOGNIZED) {
            throw new IllegalArgumentException("a concrete distance is required");
        }
    }

    /** A COSINE spec, the platform default similarity. */
    public static QdrantVectorSpec cosine(String name, int size) {
        return new QdrantVectorSpec(name, size, Distance.Cosine);
    }
}
