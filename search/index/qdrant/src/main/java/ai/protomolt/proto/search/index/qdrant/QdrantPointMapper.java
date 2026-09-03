package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.search.index.spi.AnyIndexing;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.MappingValues;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import ai.protomolt.proto.repo.v1.ChunkEmbedding;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SemanticChunk;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import qdrant.Collections.Distance;
import qdrant.Common.PointId;
import qdrant.JsonWithInt.ListValue;
import qdrant.JsonWithInt.Value;
import qdrant.Points.DenseVector;
import qdrant.Points.NamedVectors;
import qdrant.Points.PointStruct;
import qdrant.Points.Vector;
import qdrant.Points.Vectors;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Maps a repo {@link Document} into Qdrant {@link PointStruct}s: one point per semantic
 * chunk that carries at least one embedding, with the chunk's embeddings stored as named
 * vectors keyed by embedding model.
 *
 * <p>Vector source: {@code search_metadata.semantic_results} &rarr;
 * {@link SemanticProcessingResult} &rarr; {@link SemanticChunk} &rarr; {@link ChunkEmbedding}.
 * A chunk with no embeddings produces no point. When several models embed the same chunk,
 * their vectors ride on the single point as named vectors; the vector name is the model id
 * sanitized to {@code [A-Za-z0-9_-]} (every other character becomes {@code _}).
 *
 * <p>Point ids are deterministic (platform tenet): UUIDv5-style
 * {@link UUID#nameUUIDFromBytes} over {@code "qdrant|<doc_id>|<result_id>|<chunk_id>"}, so
 * re-indexing the same document upserts the same points instead of duplicating them.
 *
 * <p>Payload: every non-VECTOR, non-skip field of the {@link IndexMapping} whose value is
 * payload-representable (scalars, enums as names, lists of those, Timestamps as ISO-8601
 * strings), plus {@code doc_id}, {@code result_id}, {@code chunk_id}, {@code chunk_number},
 * {@code chunk_text}, and {@code models} (the vector names on the point). Mapping paths under
 * {@code search_metadata.semantic_results} are skipped — chunk-level data is attached per
 * point, not flattened onto every one.
 *
 * <p>Validation on write: {@link #map} first runs the document through the platform's
 * declared-rules validator ({@code ai.pipestream.proto.validate.v1} options) and throws
 * {@link ValidationResult.ValidationException} on any violation, so an invalid document is
 * never turned into points — the same gate the Kafka serde and {@code ProtobufIndexer}
 * apply on their write paths.
 *
 * <p>Vector hints: when the mapping declares a VECTOR field for the embeddings
 * ({@code search_metadata.semantic_results.chunks.embeddings.vector}, or the mapping's only
 * VECTOR field), its {@code vector_dims} are enforced on every embedding and its
 * {@code vector_similarity} becomes the collection's distance via {@link #vectorSpecs} —
 * the same hint vocabulary OpenSearch ({@code knn_vector}/{@code space_type}) and Lucene
 * consume. With no hint, sizes are read from the data and the distance defaults to COSINE.
 */
public final class QdrantPointMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "qdrant";

    /** Mapping-path prefix whose values belong to individual points, not the shared payload. */
    private static final String SEMANTIC_RESULTS_PREFIX = "search_metadata.semantic_results";

    private final ProtoFieldMapper fieldMapper;
    private final AnyIndexing anyIndexing;

    public QdrantPointMapper(ProtoFieldMapper fieldMapper) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.anyIndexing = AnyIndexing.from(fieldMapper);
    }

    public QdrantPointMapper(IndexerContext context) {
        this.fieldMapper = Objects.requireNonNull(context, "context").fieldMapper();
        this.anyIndexing = AnyIndexing.from(context);
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    /**
     * Maps {@code message} (a repo {@link Document}) to one {@link PointStruct} per embedded
     * semantic chunk, in document order. The document is validated against its declared
     * {@code ai.pipestream.proto.validate.v1} rules first.
     *
     * @throws ValidationResult.ValidationException when the document violates a declared rule
     * @throws MappingException when {@code message} is not a {@link Document}, a mapping path
     *         cannot be read, or an embedding violates the mapping's declared vector dims
     */
    @Override
    public List<PointStruct> map(Message message, IndexMapping mapping) throws MappingException {
        Objects.requireNonNull(mapping, "mapping");
        // Validation on write: declared rules are enforced before anything is indexed,
        // matching the Kafka serde and ProtobufIndexer gates.
        ValidationResult.validate(message).throwIfInvalid();
        if (!(message instanceof Document document)) {
            throw new MappingException("The qdrant mapper indexes repo Documents ("
                    + Document.getDescriptor().getFullName() + ") but got "
                    + message.getDescriptorForType().getFullName(), null);
        }
        IndexMapping expanded = anyIndexing.expand(document, mapping);
        Map<String, Value> documentPayload = documentPayload(document, expanded);
        Optional<ResolvedFieldHint> vectorHint = vectorHint(mapping);
        List<PointStruct> points = new ArrayList<>();
        for (SemanticProcessingResult result : document.getSearchMetadata().getSemanticResultsList()) {
            for (SemanticChunk chunk : result.getChunksList()) {
                NamedVectors.Builder namedVectors = NamedVectors.newBuilder();
                List<String> models = new ArrayList<>();
                for (ChunkEmbedding embedding : chunk.getEmbeddingsList()) {
                    if (embedding.getVectorCount() == 0) {
                        continue;
                    }
                    checkDeclaredDims(embedding, vectorHint);
                    String name = vectorName(embedding.getModel());
                    namedVectors.putVectors(name, Vector.newBuilder()
                            .setDense(DenseVector.newBuilder().addAllData(embedding.getVectorList()))
                            .build());
                    if (!models.contains(name)) {
                        models.add(name);
                    }
                }
                if (namedVectors.getVectorsCount() == 0) {
                    continue; // no embeddings on this chunk: nothing to index
                }
                Map<String, Value> payload = new LinkedHashMap<>(documentPayload);
                payload.put("doc_id", stringValue(document.getDocId()));
                payload.put("result_id", stringValue(result.getResultId()));
                payload.put("chunk_id", stringValue(chunk.getChunkId()));
                payload.put("chunk_number", Value.newBuilder()
                        .setIntegerValue(chunk.getChunkNumber()).build());
                payload.put("chunk_text", stringValue(chunk.getText()));
                ListValue.Builder modelList = ListValue.newBuilder();
                models.forEach(model -> modelList.addValues(stringValue(model)));
                payload.put("models", Value.newBuilder().setListValue(modelList).build());
                points.add(PointStruct.newBuilder()
                        .setId(PointId.newBuilder().setUuid(
                                pointUuid(document.getDocId(), result.getResultId(),
                                        chunk.getChunkId()).toString()))
                        .setVectors(Vectors.newBuilder().setVectors(namedVectors))
                        .putAllPayload(payload)
                        .build());
            }
        }
        return points;
    }

    /** The deterministic point id for one (document, chunk set, chunk) triple. */
    public static UUID pointUuid(String docId, String resultId, String chunkId) {
        return UUID.nameUUIDFromBytes(("qdrant|" + docId + "|" + resultId + "|" + chunkId)
                .getBytes(StandardCharsets.UTF_8));
    }

    /** Qdrant vector name for an embedding model id: {@code [^A-Za-z0-9_-]} &rarr; {@code _}. */
    public static String vectorName(String model) {
        return model.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    /**
     * Named-vector sizes declared by a batch of points, for collection creation. Every point
     * of one embedding model must agree on the dimension.
     *
     * @throws IllegalArgumentException when two points carry the same named vector at
     *         different sizes
     */
    public static Map<String, Integer> vectorDimensions(Collection<PointStruct> points) {
        Map<String, Integer> dimensions = new LinkedHashMap<>();
        for (PointStruct point : points) {
            if (!point.hasVectors() || !point.getVectors().hasVectors()) {
                continue;
            }
            for (Map.Entry<String, Vector> entry
                    : point.getVectors().getVectors().getVectorsMap().entrySet()) {
                int size = entry.getValue().getDense().getDataCount();
                Integer existing = dimensions.putIfAbsent(entry.getKey(), size);
                if (existing != null && existing != size) {
                    throw new IllegalArgumentException("Named vector '" + entry.getKey()
                            + "' has conflicting dimensions " + existing + " and " + size);
                }
            }
        }
        return dimensions;
    }

    /**
     * Named-vector specs for a batch of points, honoring the mapping's VECTOR hint where
     * declared: size and distance come from the hint ({@code vector_dims},
     * {@code vector_similarity}); without a hint the size is read from the data and the
     * distance defaults to COSINE. Every point of one embedding model must agree on the
     * size, and the data must match declared hint dims.
     *
     * @throws IllegalArgumentException when two points carry the same named vector at
     *         different sizes, or the data contradicts the hint
     */
    public static List<QdrantVectorSpec> vectorSpecs(Collection<PointStruct> points,
                                                     IndexMapping mapping) {
        Optional<ResolvedFieldHint> hint = mapping == null ? Optional.empty() : vectorHint(mapping);
        Distance distance = hint.map(h -> distance(h.vectorSimilarity())).orElse(Distance.Cosine);
        List<QdrantVectorSpec> specs = new ArrayList<>();
        vectorDimensions(points).forEach((name, size) ->
                specs.add(new QdrantVectorSpec(name, size, distance)));
        return specs;
    }

    /** Qdrant distance for a hint similarity; MAX_INNER_PRODUCT maps to Dot like OpenSearch's innerproduct. */
    public static Distance distance(VectorSimilarity similarity) {
        return switch (similarity == null ? VectorSimilarity.COSINE : similarity) {
            case COSINE -> Distance.Cosine;
            case L2 -> Distance.Euclid;
            case DOT_PRODUCT, MAX_INNER_PRODUCT -> Distance.Dot;
        };
    }

    /**
     * The mapping's vector hint for chunk embeddings: the VECTOR field under
     * {@code search_metadata.semantic_results} when one is declared, otherwise the mapping's
     * only VECTOR field. Empty when the mapping declares no usable vector hint (or several
     * ambiguous ones outside the semantic-results subtree).
     */
    static Optional<ResolvedFieldHint> vectorHint(IndexMapping mapping) {
        List<ResolvedFieldHint> semantic = new ArrayList<>();
        List<ResolvedFieldHint> all = new ArrayList<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() != IndexFieldKind.VECTOR) {
                continue;
            }
            all.add(field.hint());
            if (field.path().startsWith(SEMANTIC_RESULTS_PREFIX + ".")) {
                semantic.add(field.hint());
            }
        }
        if (!semantic.isEmpty()) {
            return Optional.of(semantic.getFirst());
        }
        return all.size() == 1 ? Optional.of(all.getFirst()) : Optional.empty();
    }

    /** Declared vector_dims, when any, are enforced on every embedding. */
    private static void checkDeclaredDims(ChunkEmbedding embedding,
                                          Optional<ResolvedFieldHint> vectorHint)
            throws MappingException {
        if (vectorHint.isPresent() && vectorHint.get().vectorDims() > 0
                && embedding.getVectorCount() != vectorHint.get().vectorDims()) {
            throw new MappingException("Embedding '" + embedding.getEmbeddingId()
                    + "' (model " + embedding.getModel() + ") has " + embedding.getVectorCount()
                    + " dimensions but the mapping's VECTOR hint declares "
                    + vectorHint.get().vectorDims(),
                    "search_metadata.semantic_results.chunks.embeddings.vector");
        }
    }

    // ---------------------------------------------------------------- payload

    /**
     * Mapping-derived payload shared by every point of the document: scalar-ish values only,
     * VECTOR hints and the semantic-results subtree excluded.
     */
    private Map<String, Value> documentPayload(Document document, IndexMapping mapping)
            throws MappingException {
        Map<String, Value> payload = new LinkedHashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() == IndexFieldKind.VECTOR
                    || field.path().equals(SEMANTIC_RESULTS_PREFIX)
                    || field.path().startsWith(SEMANTIC_RESULTS_PREFIX + ".")) {
                continue;
            }
            Object value = MappingValues.read(fieldMapper, document, field.path(), false);
            if (value == null) {
                continue;
            }
            Value converted = toValue(value);
            if (converted != null) {
                payload.put(field.fieldName(), converted);
            }
        }
        return payload;
    }

    /**
     * Converts a reflected protobuf value into a Qdrant payload {@link Value}, or returns
     * {@code null} when the value has no flat payload representation (nested messages other
     * than Timestamps, raw bytes).
     */
    static Value toValue(Object value) {
        if (value instanceof String string) {
            return stringValue(string);
        }
        if (value instanceof Boolean bool) {
            return Value.newBuilder().setBoolValue(bool).build();
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return Value.newBuilder().setIntegerValue(((Number) value).longValue()).build();
        }
        if (value instanceof Float || value instanceof Double) {
            return Value.newBuilder().setDoubleValue(((Number) value).doubleValue()).build();
        }
        if (value instanceof EnumValueDescriptor enumValue) {
            return stringValue(enumValue.getName());
        }
        if (value instanceof List<?> list) {
            ListValue.Builder listValue = ListValue.newBuilder();
            for (Object element : list) {
                Value converted = toValue(element);
                if (converted != null) {
                    listValue.addValues(converted);
                }
            }
            // Every element unrepresentable: skip the field like the singular case does,
            // instead of writing an empty list.
            return listValue.getValuesCount() == 0
                    ? null
                    : Value.newBuilder().setListValue(listValue).build();
        }
        if (value instanceof Timestamp timestamp) {
            return stringValue(Timestamps.toString(timestamp));
        }
        return null;
    }

    private static Value stringValue(String string) {
        return Value.newBuilder().setStringValue(string).build();
    }

}
