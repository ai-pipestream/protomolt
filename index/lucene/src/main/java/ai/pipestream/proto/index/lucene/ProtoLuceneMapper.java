package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.List;
import java.util.Objects;

/**
 * Lucene document mapper driven by an {@link IndexingPlan} (descriptor indexing hints).
 */
public final class ProtoLuceneMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "lucene";

    private final ProtoFieldMapper fieldMapper;

    public ProtoLuceneMapper(ProtoFieldMapper fieldMapper) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public Document map(Message message, IndexingPlan plan) throws MappingException {
        Objects.requireNonNull(plan, "plan");
        Document document = new Document();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            Object value = fieldMapper.getValue(message, field.path());
            if (value == null) {
                continue;
            }
            add(document, field, value);
        }
        return document;
    }

    /** Legacy projection API. Prefer {@link #map(Message, IndexingPlan)}. */
    public Document map(Message message, List<FieldProjection> projections) throws MappingException {
        Document document = new Document();
        if (projections == null) {
            return document;
        }
        for (FieldProjection projection : projections) {
            Object value = fieldMapper.getValue(message, projection.path());
            if (value == null) {
                continue;
            }
            ResolvedLegacy legacy = new ResolvedLegacy(projection);
            add(document, legacy, value);
        }
        return document;
    }

    private void add(Document document, IndexingPlan.IndexedField field, Object value) {
        add(document, field.fieldName(), field.type(), field.stored(), field.indexed(), value);
    }

    private void add(Document document, ResolvedLegacy field, Object value) {
        IndexFieldKind kind = value instanceof String ? IndexFieldKind.TEXT : IndexFieldKind.KEYWORD;
        add(document, field.luceneFieldName(), kind, field.stored(), field.indexed(), value);
    }

    private static void add(
            Document document,
            String name,
            IndexFieldKind kind,
            boolean stored,
            boolean indexed,
            Object value) {
        org.apache.lucene.document.Field.Store store =
                stored ? org.apache.lucene.document.Field.Store.YES : org.apache.lucene.document.Field.Store.NO;

        if (value instanceof ByteString bytes) {
            if (stored) {
                document.add(new StoredField(name, bytes.toByteArray()));
            }
            return;
        }
        if (value instanceof Timestamp ts) {
            long millis = ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
            if (indexed) {
                document.add(new LongPoint(name, millis));
            }
            if (stored) {
                document.add(new StoredField(name, millis));
            }
            return;
        }

        switch (kind) {
            case TEXT -> {
                if (indexed) {
                    document.add(new TextField(name, String.valueOf(value), store));
                } else if (stored) {
                    document.add(new StoredField(name, String.valueOf(value)));
                }
            }
            case KEYWORD, DATE, BOOLEAN -> {
                String stringValue = value instanceof com.google.protobuf.Descriptors.EnumValueDescriptor ev
                        ? ev.getName()
                        : String.valueOf(value);
                if (indexed) {
                    document.add(new StringField(name, stringValue, store));
                } else if (stored) {
                    document.add(new StoredField(name, stringValue));
                }
            }
            case INT32 -> {
                int v = ((Number) value).intValue();
                if (indexed) {
                    document.add(new IntPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case INT64 -> {
                long v = ((Number) value).longValue();
                if (indexed) {
                    document.add(new LongPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case FLOAT -> {
                float v = ((Number) value).floatValue();
                if (indexed) {
                    document.add(new FloatPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case DOUBLE -> {
                double v = ((Number) value).doubleValue();
                if (indexed) {
                    document.add(new DoublePoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case BINARY -> {
                if (stored && value instanceof byte[] bytes) {
                    document.add(new StoredField(name, bytes));
                }
            }
            case VECTOR -> addVector(document, name, value);
            case OBJECT, NESTED, UNSPECIFIED, SKIP -> {
                if (stored) {
                    document.add(new StoredField(name, String.valueOf(value)));
                }
            }
        }
    }

    /**
     * Indexes a float vector with Lucene HNSW ({@link KnnFloatVectorField}).
     * Accepts {@code float[]}, {@code double[]}, or {@code List} of numbers
     * (e.g. protobuf {@code repeated float}).
     */
    private static void addVector(Document document, String name, Object value) {
        float[] vector = toFloatVector(value);
        if (vector.length == 0) {
            return;
        }
        document.add(new KnnFloatVectorField(name, vector, VectorSimilarityFunction.COSINE));
    }

    static float[] toFloatVector(Object value) {
        if (value instanceof float[] floats) {
            return floats;
        }
        if (value instanceof double[] doubles) {
            float[] out = new float[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                out[i] = (float) doubles[i];
            }
            return out;
        }
        if (value instanceof List<?> list) {
            float[] out = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                if (!(element instanceof Number number)) {
                    throw new IllegalArgumentException(
                            "VECTOR field values must be numeric; got " + element);
                }
                out[i] = number.floatValue();
            }
            return out;
        }
        throw new IllegalArgumentException(
                "VECTOR expects float[], double[], or List<? extends Number>; got "
                        + (value == null ? "null" : value.getClass().getName()));
    }

    private record ResolvedLegacy(FieldProjection projection) {
        String luceneFieldName() {
            return projection.luceneFieldName();
        }

        boolean stored() {
            return projection.stored();
        }

        boolean indexed() {
            return projection.indexed();
        }
    }

    public record FieldProjection(String path, String luceneFieldName, boolean stored, boolean indexed) {
        public FieldProjection {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(luceneFieldName, "luceneFieldName");
        }
    }
}
