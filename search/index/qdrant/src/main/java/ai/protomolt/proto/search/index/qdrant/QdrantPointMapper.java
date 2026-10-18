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
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.v1.Document;
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
 * segment that carries at least one embedding, with the segment's embeddings stored as named
 * vectors keyed by embedding model.
 *
 * <p>Vector source: {@code search_metadata.segmentations} ({@link Segmentation} &rarr;
 * {@link Segment}) joined with {@code search_metadata.embedding_sets} ({@link EmbeddingSet}
 * &rarr; {@link SegmentVector}) on {@code segmentation_id} and {@code segment_id}.
 * Every matching {@link EmbeddingSet}'s {@code segmentation_digest} must equal its
 * {@link Segmentation}'s {@code content_digest}. A segment with no embeddings produces no
 * point. When several distinct models embed the same segment, their vectors ride on the single
 * point as named vectors; the vector name is the model id sanitized to {@code [A-Za-z0-9_-]}
 * (every other character becomes {@code _}). Colliding vector names on the same segmentation
 * or across distinct model ids are rejected with a {@link MappingException}.
 *
 * <p>Point ids are deterministic (platform tenet): UUIDv5-style
 * {@link UUID#nameUUIDFromBytes} over {@code "qdrant|<doc_id>|<segmentation_id>|<segment_id>"},
 * so re-indexing the same document upserts the same points instead of duplicating them.
 *
 * <p>Payload: every non-VECTOR, non-skip field of the {@link IndexMapping} whose value is
 * payload-representable (scalars, enums as names, lists of those, Timestamps as ISO-8601
 * strings), plus {@code doc_id}, {@code result_id} and {@code segmentation_id}, {@code chunk_id}
 * and {@code segment_id}, {@code chunk_number}, {@code chunk_text}, and {@code models} (the
 * vector names on the point). Mapping paths under {@code search_metadata.segmentations},
 * {@code search_metadata.embedding_sets}, and legacy {@code search_metadata.semantic_results}
 * are skipped — segment-level data is attached per point, not flattened onto every one.
 *
 * <p>Validation on write: {@link #map} first runs the document through the platform's
 * declared-rules validator ({@code ai.protomolt.proto.validate.v1} options) and throws
 * {@link ValidationResult.ValidationException} on any violation, so an invalid document is
 * never turned into points — the same gate the Kafka serde and {@code ProtobufIndexer}
 * apply on their write paths.
 *
 * <p>Vector hints: when the mapping declares a VECTOR field for the embeddings
 * ({@code search_metadata.embedding_sets.vectors.values}, legacy
 * {@code search_metadata.semantic_results.chunks.embeddings.vector}, or the mapping's only
 * VECTOR field), its {@code vector_dims} and {@code vector_similarity} are enforced against
 * every embedding set — the same hint vocabulary OpenSearch ({@code knn_vector}/{@code space_type})
 * and Lucene consume. With no hint, sizes and distances come from the {@link EmbeddingSet}s
 * themselves via {@link #vectorSpecs}.
 */
public final class QdrantPointMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "qdrant";

    /** Mapping-path prefixes whose values belong to individual points, not the shared payload. */
    private static final String SEGMENTATIONS_PREFIX = "search_metadata.segmentations";
    private static final String EMBEDDING_SETS_PREFIX = "search_metadata.embedding_sets";
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
     * semantic segment, in document order. The document is validated against its declared
     * {@code ai.protomolt.proto.validate.v1} rules first.
     *
     * @throws ValidationResult.ValidationException when the document violates a declared rule
     * @throws MappingException when {@code message} is not a {@link Document}, a mapping path
     *         cannot be read, an embedding set's {@code segmentation_digest} mismatches its
     *         segmentation's {@code content_digest}, vector names collide, or an embedding
     *         violates the mapping's declared vector dims or similarity
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
        List<EmbeddingSet> allEmbeddingSets = document.getSearchMetadata().getEmbeddingSetsList();
        Map<String, String> modelByVectorName = new LinkedHashMap<>();
        Map<String, Distance> distanceByVectorName = new LinkedHashMap<>();
        for (Segmentation segmentation : document.getSearchMetadata().getSegmentationsList()) {
            List<EmbeddingSet> matchingSets = matchingEmbeddingSets(
                    segmentation, allEmbeddingSets, vectorHint,
                    modelByVectorName, distanceByVectorName);
            for (Segment segment : segmentation.getSegmentsList()) {
                NamedVectors.Builder namedVectors = NamedVectors.newBuilder();
                List<String> models = new ArrayList<>();
                for (EmbeddingSet embeddingSet : matchingSets) {
                    String name = vectorName(embeddingSet.getModel());
                    for (SegmentVector segmentVector : embeddingSet.getVectorsList()) {
                        if (!segmentVector.getSegmentId().equals(segment.getSegmentId())
                                || segmentVector.getValuesCount() == 0) {
                            continue;
                        }
                        checkDeclaredDims(embeddingSet, segmentVector, vectorHint);
                        if (namedVectors.containsVectors(name)) {
                            throw new MappingException("Duplicate vector '" + name
                                    + "' for segment '" + segment.getSegmentId()
                                    + "' in segmentation '" + segmentation.getSegmentationId() + "'",
                                    "search_metadata.embedding_sets.vectors");
                        }
                        namedVectors.putVectors(name, Vector.newBuilder()
                                .setDense(DenseVector.newBuilder()
                                        .addAllData(segmentVector.getValuesList()))
                                .build());
                        models.add(name);
                    }
                }
                if (namedVectors.getVectorsCount() == 0) {
                    continue; // no embeddings on this segment: nothing to index
                }
                Map<String, Value> payload = new LinkedHashMap<>(documentPayload);
                payload.put("doc_id", stringValue(document.getDocId()));
                payload.put("result_id", stringValue(segmentation.getSegmentationId()));
                payload.put("segmentation_id", stringValue(segmentation.getSegmentationId()));
                payload.put("chunk_id", stringValue(segment.getSegmentId()));
                payload.put("segment_id", stringValue(segment.getSegmentId()));
                payload.put("chunk_number", Value.newBuilder()
                        .setIntegerValue(segment.getOrdinal()).build());
                payload.put("chunk_text", stringValue(segment.getText()));
                ListValue.Builder modelList = ListValue.newBuilder();
                models.forEach(model -> modelList.addValues(stringValue(model)));
                payload.put("models", Value.newBuilder().setListValue(modelList).build());
                PointStruct point = PointStruct.newBuilder()
                        .setId(PointId.newBuilder().setUuid(
                                pointUuid(document.getDocId(), segmentation.getSegmentationId(),
                                        segment.getSegmentId()).toString()))
                        .setVectors(Vectors.newBuilder().setVectors(namedVectors))
                        .putAllPayload(payload)
                        .build();
                points.add(point);
            }
        }
        return points;
    }

    private static List<EmbeddingSet> matchingEmbeddingSets(
            Segmentation segmentation,
            List<EmbeddingSet> allEmbeddingSets,
            Optional<ResolvedFieldHint> vectorHint,
            Map<String, String> modelByVectorName,
            Map<String, Distance> distanceByVectorName) throws MappingException {
        List<EmbeddingSet> matchingSets = new ArrayList<>();
        Map<String, EmbeddingSet> setsByVectorName = new LinkedHashMap<>();
        for (EmbeddingSet embeddingSet : allEmbeddingSets) {
            if (!embeddingSet.getSegmentationId().equals(segmentation.getSegmentationId())) {
                continue;
            }
            if (!segmentation.getContentDigest().equals(embeddingSet.getSegmentationDigest())) {
                throw new MappingException("Embedding set '" + embeddingSet.getEmbeddingSetId()
                        + "' declares segmentation_digest '" + embeddingSet.getSegmentationDigest()
                        + "' that does not match segmentation '" + segmentation.getSegmentationId()
                        + "' content_digest '" + segmentation.getContentDigest() + "'",
                        "search_metadata.embedding_sets.segmentation_digest");
            }
            String name = vectorName(embeddingSet.getModel());
            EmbeddingSet collidingOnSegmentation = setsByVectorName.putIfAbsent(name, embeddingSet);
            if (collidingOnSegmentation != null) {
                throw new MappingException("Embedding sets '"
                        + collidingOnSegmentation.getEmbeddingSetId() + "' (model '"
                        + collidingOnSegmentation.getModel() + "') and '"
                        + embeddingSet.getEmbeddingSetId() + "' (model '"
                        + embeddingSet.getModel() + "') collide on Qdrant vector name '"
                        + name + "' in segmentation '" + segmentation.getSegmentationId() + "'",
                        "search_metadata.embedding_sets.model");
            }
            String previousModel = modelByVectorName.putIfAbsent(name, embeddingSet.getModel());
            if (previousModel != null && !previousModel.equals(embeddingSet.getModel())) {
                throw new MappingException("Models '" + previousModel + "' and '"
                        + embeddingSet.getModel() + "' collide on Qdrant vector name '" + name + "'",
                        "search_metadata.embedding_sets.model");
            }
            Distance setDistance = distance(embeddingSet.getSimilarity());
            checkDeclaredSimilarity(embeddingSet, setDistance, vectorHint);
            Distance previousDistance = distanceByVectorName.putIfAbsent(name, setDistance);
            if (previousDistance != null && previousDistance != setDistance) {
                throw new MappingException("Named vector '" + name + "' (model '"
                        + embeddingSet.getModel() + "') has conflicting similarities "
                        + previousDistance + " and " + setDistance,
                        "search_metadata.embedding_sets.similarity");
            }
            matchingSets.add(embeddingSet);
        }
        return matchingSets;
    }

    /** The deterministic point id for one (document, segmentation, segment) triple. */
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
     * Specs for already-projected points. Points contain no distance declaration, so a
     * VECTOR hint is required for a nonempty batch. When the source Document is available,
     * use {@link #vectorSpecs(Document, IndexMapping)} and pass its specs to the sink.
     *
     * @throws IllegalArgumentException when the hint is missing or dimensions conflict
     */
    public static List<QdrantVectorSpec> vectorSpecs(Collection<PointStruct> points,
                                                     IndexMapping mapping) {
        var dimensions = vectorDimensions(points);
        if (dimensions.isEmpty()) return List.of();
        var hint = (mapping == null ? Optional.<ResolvedFieldHint>empty() : vectorHint(mapping))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Points do not carry similarity; supply a VECTOR hint or derive specs from the source Document"));
        List<QdrantVectorSpec> specs = new ArrayList<>();
        dimensions.forEach((name, size) -> {
            if (hint.vectorDims() > 0 && hint.vectorDims() != size)
                throw new IllegalArgumentException("Named vector '" + name + "' has " + size
                        + " dimensions but the mapping declares " + hint.vectorDims());
            specs.add(new QdrantVectorSpec(name, size, distance(hint.vectorSimilarity())));
        });
        return specs;
    }

    /**
     * Named-vector specs derived directly from a repo {@link Document} and optional
     * {@link IndexMapping}, honoring each {@link EmbeddingSet#getSimilarity()} and enforcing
     * any declared VECTOR hint.
     *
     * @throws ValidationResult.ValidationException when the document violates a declared rule
     * @throws MappingException when a digest, vector name, dimension, or similarity conflicts
     */
    public static List<QdrantVectorSpec> vectorSpecs(Document document, IndexMapping mapping)
            throws MappingException {
        Objects.requireNonNull(document, "document");
        ValidationResult.validate(document).throwIfInvalid();
        Optional<ResolvedFieldHint> vectorHint =
                mapping == null ? Optional.empty() : vectorHint(mapping);
        List<EmbeddingSet> allEmbeddingSets = document.getSearchMetadata().getEmbeddingSetsList();
        Map<String, String> modelByVectorName = new LinkedHashMap<>();
        Map<String, Distance> distanceByVectorName = new LinkedHashMap<>();
        Map<String, Integer> sizeByVectorName = new LinkedHashMap<>();
        for (Segmentation segmentation : document.getSearchMetadata().getSegmentationsList()) {
            List<EmbeddingSet> matchingSets = matchingEmbeddingSets(
                    segmentation, allEmbeddingSets, vectorHint,
                    modelByVectorName, distanceByVectorName);
            for (Segment segment : segmentation.getSegmentsList()) {
                for (EmbeddingSet embeddingSet : matchingSets) {
                    String name = vectorName(embeddingSet.getModel());
                    for (SegmentVector segmentVector : embeddingSet.getVectorsList()) {
                        if (!segmentVector.getSegmentId().equals(segment.getSegmentId())
                                || segmentVector.getValuesCount() == 0) {
                            continue;
                        }
                        checkDeclaredDims(embeddingSet, segmentVector, vectorHint);
                        int size = segmentVector.getValuesCount();
                        Integer existingSize = sizeByVectorName.putIfAbsent(name, size);
                        if (existingSize != null && existingSize != size) {
                            throw new MappingException("Named vector '" + name
                                    + "' has conflicting dimensions " + existingSize
                                    + " and " + size,
                                    "search_metadata.embedding_sets.vectors.values");
                        }
                    }
                }
            }
        }
        List<QdrantVectorSpec> specs = new ArrayList<>();
        sizeByVectorName.forEach((name, size) ->
                specs.add(new QdrantVectorSpec(name, size, distanceByVectorName.get(name))));
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

    /** Qdrant distance for an {@link EmbeddingSet}'s declared {@link Similarity}. */
    public static Distance distance(Similarity similarity) throws MappingException {
        return switch (similarity == null ? Similarity.SIMILARITY_UNSPECIFIED : similarity) {
            case SIMILARITY_COSINE -> Distance.Cosine;
            case SIMILARITY_DOT_PRODUCT -> Distance.Dot;
            case SIMILARITY_EUCLIDEAN -> Distance.Euclid;
            case SIMILARITY_UNSPECIFIED, UNRECOGNIZED ->
                    throw new MappingException("Unsupported embedding similarity: " + similarity,
                            "search_metadata.embedding_sets.similarity");
        };
    }

    /**
     * The mapping's vector hint for segment embeddings: the VECTOR field under
     * {@code search_metadata.embedding_sets} (or legacy {@code search_metadata.semantic_results})
     * when one is declared, otherwise the mapping's only VECTOR field. Empty when the mapping
     * declares no usable vector hint (or several ambiguous ones outside the semantic subtrees).
     */
    static Optional<ResolvedFieldHint> vectorHint(IndexMapping mapping) {
        List<ResolvedFieldHint> semantic = new ArrayList<>();
        List<ResolvedFieldHint> all = new ArrayList<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() != IndexFieldKind.VECTOR) {
                continue;
            }
            all.add(field.hint());
            if (field.path().startsWith(EMBEDDING_SETS_PREFIX + ".")
                    || field.path().startsWith(SEMANTIC_RESULTS_PREFIX + ".")) {
                semantic.add(field.hint());
            }
        }
        if (!semantic.isEmpty()) {
            return Optional.of(semantic.getFirst());
        }
        return all.size() == 1 ? Optional.of(all.getFirst()) : Optional.empty();
    }

    /** Declared vector_dims, when any, are enforced on every embedding. */
    private static void checkDeclaredDims(EmbeddingSet embeddingSet,
                                          SegmentVector segmentVector,
                                          Optional<ResolvedFieldHint> vectorHint)
            throws MappingException {
        if (vectorHint.isPresent() && vectorHint.get().vectorDims() > 0
                && segmentVector.getValuesCount() != vectorHint.get().vectorDims()) {
            throw new MappingException("Embedding set '" + embeddingSet.getEmbeddingSetId()
                    + "' segment '" + segmentVector.getSegmentId()
                    + "' (model " + embeddingSet.getModel() + ") has "
                    + segmentVector.getValuesCount()
                    + " dimensions but the mapping's VECTOR hint declares "
                    + vectorHint.get().vectorDims(),
                    "search_metadata.embedding_sets.vectors.values");
        }
    }

    /** Declared vector_similarity, when any, must agree with the embedding set's similarity. */
    private static void checkDeclaredSimilarity(EmbeddingSet embeddingSet,
                                                Distance embeddingDistance,
                                                Optional<ResolvedFieldHint> vectorHint)
            throws MappingException {
        if (vectorHint.isPresent()) {
            Distance hintDistance = distance(vectorHint.get().vectorSimilarity());
            if (embeddingDistance != hintDistance) {
                throw new MappingException("Embedding set '" + embeddingSet.getEmbeddingSetId()
                        + "' (model " + embeddingSet.getModel() + ") declares similarity "
                        + embeddingSet.getSimilarity() + " (" + embeddingDistance
                        + ") but the mapping's VECTOR hint declares "
                        + vectorHint.get().vectorSimilarity() + " (" + hintDistance + ")",
                        "search_metadata.embedding_sets.similarity");
            }
        }
    }

    // ---------------------------------------------------------------- payload

    /**
     * Mapping-derived payload shared by every point of the document: scalar-ish values only,
     * VECTOR hints and the semantic subtrees excluded.
     */
    private Map<String, Value> documentPayload(Document document, IndexMapping mapping)
            throws MappingException {
        Map<String, Value> payload = new LinkedHashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() == IndexFieldKind.VECTOR
                    || isSemanticSubtree(field.path())) {
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

    private static boolean isSemanticSubtree(String path) {
        return isUnderPrefix(path, SEGMENTATIONS_PREFIX)
                || isUnderPrefix(path, EMBEDDING_SETS_PREFIX)
                || isUnderPrefix(path, SEMANTIC_RESULTS_PREFIX);
    }

    private static boolean isUnderPrefix(String path, String prefix) {
        return path.equals(prefix) || path.startsWith(prefix + ".");
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
