package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.Value;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoLuceneMapperTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void projectsStructPathsIntoLuceneFields() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Pipestream").build())
                .putFields("lang", Value.newBuilder().setStringValue("en").build())
                .build();

        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("title", "title", true, true),
                new ProtoLuceneMapper.FieldProjection("lang", "lang", true, true)
        ));

        assertThat(doc.get("title")).isEqualTo("Pipestream");
        assertThat(doc.get("lang")).isEqualTo("en");
    }

    @Test
    void skipsNullPaths() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("only").build())
                .build();
        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("title", "title", true, true),
                new ProtoLuceneMapper.FieldProjection("missing", "missing", true, true)
        ));
        assertThat(doc.get("title")).isEqualTo("only");
        assertThat(doc.get("missing")).isNull();
    }

    @Test
    void storedOnlyNumericField() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("score", Value.newBuilder().setNumberValue(3.5).build())
                .build();
        Document doc = mapper.map(message, List.of(
                new ProtoLuceneMapper.FieldProjection("score", "score", true, false)
        ));
        assertThat(doc.get("score")).isEqualTo("3.5");
    }

    @Test
    void repeatedInt64FieldEmitsOneLuceneFieldPerElement() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("values", FieldDescriptorProto.Type.TYPE_INT64);
        FieldDescriptor values = descriptor.findFieldByName("values");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(values, 7L)
                .addRepeatedField(values, 9L)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("values", "values", ResolvedFieldHint.of(IndexFieldKind.INT64))));

        Document doc = mapper.map(message, plan);

        // one indexed point + one stored field per element
        assertThat(doc.getFields("values")).hasSize(4);
        List<Long> stored = Arrays.stream(doc.getFields("values"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(7L, 9L);
    }

    @Test
    void repeatedStringFieldEmitsOneKeywordPerElement() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("tags", FieldDescriptorProto.Type.TYPE_STRING);
        FieldDescriptor tags = descriptor.findFieldByName("tags");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(tags, "alpha")
                .addRepeatedField(tags, "beta")
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("tags", "tags", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        Document doc = mapper.map(message, plan);

        List<String> keywords = Arrays.stream(doc.getFields("tags"))
                .map(IndexableField::stringValue)
                .toList();
        assertThat(keywords).containsExactly("alpha", "beta");
    }

    @Test
    void dynamicTimestampFieldIndexedAsEpochMillis() throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        DynamicMessage timestamp = DynamicMessage.newBuilder(tsDescriptor)
                .setField(tsDescriptor.findFieldByName("seconds"), 1_700_000_000L)
                .setField(tsDescriptor.findFieldByName("nanos"), 500_000_000)
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, timestamp)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created", ResolvedFieldHint.of(IndexFieldKind.DATE))));

        Document doc = mapper.map(message, plan);

        // one indexed LongPoint + one stored field, both carrying epoch millis — never text-format strings
        assertThat(doc.getFields("created")).hasSize(2);
        assertThat(Arrays.stream(doc.getFields("created")).map(IndexableField::numericValue).toList())
                .doesNotContainNull();
        List<Long> stored = Arrays.stream(doc.getFields("created"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(1_700_000_000_500L);
    }

    @Test
    void dateHintOnInt64EpochFieldIndexedAsLongPoint() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("created_ms", FieldDescriptorProto.Type.TYPE_INT64);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("created_ms"), 1_700_000_000_000L)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created_ms", "created_ms", ResolvedFieldHint.of(IndexFieldKind.DATE))));

        Document doc = mapper.map(message, plan);

        // one indexed LongPoint + one stored field, both numeric — not a keyword string
        assertThat(doc.getFields("created_ms")).hasSize(2);
        assertThat(Arrays.stream(doc.getFields("created_ms")).map(IndexableField::numericValue).toList())
                .doesNotContainNull();
        List<Long> stored = Arrays.stream(doc.getFields("created_ms"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(1_700_000_000_000L);
    }

    @Test
    void unsetIntermediateMessageInPlanPathSkipsField() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "kept")
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner.name", "inner_name", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexingPlan.IndexedField("title", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        Document doc = mapper.map(message, plan);

        assertThat(doc.get("title")).isEqualTo("kept");
        assertThat(doc.getFields("inner_name")).isEmpty();
    }

    @Test
    void genuinelyInvalidPlanPathStillThrows() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("nope.name", "nope", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThatThrownBy(() -> mapper.map(message, plan)).isInstanceOf(MappingException.class);
    }

    @Test
    void indexedOnlyByteStringProducesExactMatchField() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("digest", FieldDescriptorProto.Type.TYPE_BYTES);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("digest"),
                        com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("digest", "digest",
                        new ResolvedFieldHint(IndexFieldKind.BINARY, false, true, "", 0))));

        Document doc = mapper.map(message, plan);

        // hinted indexed-only bytes must never be dropped silently
        assertThat(doc.getFields("digest")).hasSize(1);
        assertThat(doc.getFields("digest")[0].binaryValue().bytes)
                .startsWith((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void storedByteStringKeepsRawBytes() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("digest", FieldDescriptorProto.Type.TYPE_BYTES);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("digest"),
                        com.google.protobuf.ByteString.copyFrom(new byte[]{9, 8}))
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("digest", "digest",
                        new ResolvedFieldHint(IndexFieldKind.BINARY, true, false, "", 0))));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("digest")).hasSize(1);
        assertThat(doc.getFields("digest")[0].fieldType().stored()).isTrue();
        assertThat(doc.getFields("digest")[0].binaryValue().bytes).startsWith((byte) 9, (byte) 8);
    }

    @Test
    void objectHintedMessageStoresCompactJson() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage inner = DynamicMessage.newBuilder(innerDescriptor)
                .setField(innerDescriptor.findFieldByName("name"), "n1")
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"), inner)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner", "inner",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, plan);

        // compact JsonFormat JSON, not protobuf text format
        assertThat(doc.get("inner")).isEqualTo("{\"name\":\"n1\"}");
    }

    /**
     * A message value reaching a string-shaped field must render as canonical JSON like every
     * other message-valued path. These two kinds once fell through to {@code String.valueOf},
     * which emits protobuf text format — so the same nested message indexed under two different
     * encodings depending only on its hint.
     */
    @Test
    void textAndKeywordHintedMessagesRenderAsCompactJson() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"),
                        DynamicMessage.newBuilder(innerDescriptor)
                                .setField(innerDescriptor.findFieldByName("name"), "n1")
                                .build())
                .build();

        for (IndexFieldKind kind : List.of(IndexFieldKind.TEXT, IndexFieldKind.KEYWORD)) {
            IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                    new IndexingPlan.IndexedField("inner", "inner", ResolvedFieldHint.of(kind))));

            Document doc = mapper.map(message, plan);

            assertThat(doc.get("inner")).as("%s-hinted message", kind).isEqualTo("{\"name\":\"n1\"}");
        }
    }

    /**
     * {@code ResolvedFieldHint.of(OBJECT)} resolves to stored <em>and</em> indexed, so the JSON
     * must be searchable, not merely retrievable. The two flags were once treated as alternatives
     * with storage winning, which left every default-hinted OBJECT and NESTED field absent from
     * the index while {@code doc.get} still returned its value — so retrieval-based assertions
     * passed and no query ever matched.
     */
    @Test
    void objectHintedMessageIsIndexedAsWellAsStored() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"),
                        DynamicMessage.newBuilder(innerDescriptor)
                                .setField(innerDescriptor.findFieldByName("name"), "n1")
                                .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner", "inner",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, plan);

        // one field carrying both duties, not two fields duplicating the value on retrieval
        assertThat(doc.getFields("inner")).hasSize(1);
        IndexableField field = doc.getFields("inner")[0];
        assertThat(field.fieldType().stored()).isTrue();
        assertThat(field.fieldType().indexOptions())
                .isNotEqualTo(org.apache.lucene.index.IndexOptions.NONE);
        assertThat(doc.get("inner")).isEqualTo("{\"name\":\"n1\"}");
    }

    @Test
    void objectHintedFieldThatIsIndexedOnlyIsStillSearchableAndNotStored() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"),
                        DynamicMessage.newBuilder(innerDescriptor)
                                .setField(innerDescriptor.findFieldByName("name"), "n1")
                                .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner", "inner",
                        ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                                .stored(false).indexed(true).build())));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("inner")).hasSize(1);
        assertThat(doc.getFields("inner")[0].fieldType().stored()).isFalse();
        assertThat(doc.getFields("inner")[0].fieldType().indexOptions())
                .isNotEqualTo(org.apache.lucene.index.IndexOptions.NONE);
    }

    @Test
    void objectHintedFieldThatIsStoredOnlyIsNotIndexed() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"),
                        DynamicMessage.newBuilder(innerDescriptor)
                                .setField(innerDescriptor.findFieldByName("name"), "n1")
                                .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner", "inner",
                        ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                                .stored(true).indexed(false).build())));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("inner")).hasSize(1);
        assertThat(doc.getFields("inner")[0].fieldType().stored()).isTrue();
        assertThat(doc.getFields("inner")[0].fieldType().indexOptions())
                .isEqualTo(org.apache.lucene.index.IndexOptions.NONE);
    }

    /**
     * Java division truncates toward zero, so a pre-epoch instant at SECONDS resolution once
     * rounded up: -1500ms became -1s rather than -2s, placing the value one second later than the
     * same instant indexed in millis and breaking range filters that straddle the epoch.
     */
    @Test
    void preEpochTimestampAtSecondsResolutionFloorsRatherThanTruncating() throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        // 1969-12-31T23:59:58.500Z — protobuf keeps nanos non-negative for negative seconds
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, DynamicMessage.newBuilder(tsDescriptor)
                        .setField(tsDescriptor.findFieldByName("seconds"), -2L)
                        .setField(tsDescriptor.findFieldByName("nanos"), 500_000_000)
                        .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.pipestream.proto.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, plan);

        List<Long> stored = Arrays.stream(doc.getFields("created"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(-2L);
    }

    @Test
    void preEpochTimestampsKeepTheirOrderAcrossTheEpochBoundary() throws Exception {
        assertThat(secondsResolutionValue(-2L, 500_000_000))
                .isLessThan(secondsResolutionValue(-1L, 0));
        assertThat(secondsResolutionValue(-1L, 0))
                .isLessThan(secondsResolutionValue(0L, 0));
        assertThat(secondsResolutionValue(0L, 0))
                .isLessThan(secondsResolutionValue(1L, 0));
    }

    /** Indexes a single Timestamp at SECONDS resolution and returns the stored value. */
    private long secondsResolutionValue(long seconds, int nanos) throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, DynamicMessage.newBuilder(tsDescriptor)
                        .setField(tsDescriptor.findFieldByName("seconds"), seconds)
                        .setField(tsDescriptor.findFieldByName("nanos"), nanos)
                        .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.pipestream.proto.index.spi.DateResolution.SECONDS)
                                .build())));

        return Arrays.stream(mapper.map(message, plan).getFields("created"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .findFirst()
                .orElseThrow();
    }

    @Test
    void objectHintedMapFieldStoresOneJsonObject() throws Exception {
        Descriptor descriptor = mapFieldDescriptor();
        FieldDescriptor labels = descriptor.findFieldByName("labels");
        Descriptor entryType = labels.getMessageType();
        DynamicMessage entryA = DynamicMessage.newBuilder(entryType)
                .setField(entryType.findFieldByName("key"), "env")
                .setField(entryType.findFieldByName("value"), "prod")
                .build();
        DynamicMessage entryB = DynamicMessage.newBuilder(entryType)
                .setField(entryType.findFieldByName("key"), "team")
                .setField(entryType.findFieldByName("value"), "search")
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(labels, entryA)
                .addRepeatedField(labels, entryB)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("labels", "labels",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, plan);

        // one JSON object string for the whole map, not one MapEntry toString per pair
        assertThat(doc.getFields("labels")).hasSize(1);
        assertThat(doc.get("labels")).isEqualTo("{\"env\":\"prod\",\"team\":\"search\"}");
    }

    @Test
    void vectorHintWithMatchingDimsBuildsKnnField() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_FLOAT);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(embedding, 0.1f)
                .addRepeatedField(embedding, 0.2f)
                .addRepeatedField(embedding, 0.3f)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 3))));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("embedding")).hasSize(1);
        assertThat(doc.getFields("embedding")[0])
                .isInstanceOf(org.apache.lucene.document.KnnFloatVectorField.class);
        float[] vector = ((org.apache.lucene.document.KnnFloatVectorField) doc.getFields("embedding")[0])
                .vectorValue();
        assertThat(vector).containsExactly(new float[]{0.1f, 0.2f, 0.3f}, org.assertj.core.data.Offset.offset(0.0001f));
    }

    @Test
    void vectorHintWithDimsMismatchFallsBackToStoredJson() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_FLOAT);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(embedding, 0.5f)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 3))));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("embedding")).hasSize(1);
        assertThat(doc.getFields("embedding")[0].fieldType().stored()).isTrue();
        assertThat(doc.get("embedding")).isEqualTo("[0.5]");
    }

    @Test
    void numericHintOnNonNumericValueThrowsMappingException() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("count", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("count"), "not-a-number")
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("count", "count", ResolvedFieldHint.of(IndexFieldKind.INT64))));

        assertThatThrownBy(() -> mapper.map(message, plan))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("count")
                .hasMessageContaining("INT64")
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void includeDefaultsIndexesImplicitPresenceDefaults() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("archived", FieldDescriptorProto.Type.TYPE_BOOL);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("archived", "archived",
                        ResolvedFieldHint.of(IndexFieldKind.BOOLEAN))));

        // default behaviour: fields at their default value are skipped
        assertThat(mapper.map(message, plan).getFields("archived")).isEmpty();

        ProtoLuceneMapper withDefaults = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()), true);
        Document doc = withDefaults.map(message, plan);
        assertThat(doc.get("archived")).isEqualTo("false");
    }

    @Test
    void vectorSimilaritiesMapToLuceneFunctions() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_FLOAT);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(embedding, 0.1f)
                .addRepeatedField(embedding, 0.2f)
                .build();
        Map<ai.pipestream.proto.index.spi.VectorSimilarity, VectorSimilarityFunction> expected = Map.of(
                ai.pipestream.proto.index.spi.VectorSimilarity.COSINE, VectorSimilarityFunction.COSINE,
                ai.pipestream.proto.index.spi.VectorSimilarity.DOT_PRODUCT, VectorSimilarityFunction.DOT_PRODUCT,
                ai.pipestream.proto.index.spi.VectorSimilarity.L2, VectorSimilarityFunction.EUCLIDEAN,
                ai.pipestream.proto.index.spi.VectorSimilarity.MAX_INNER_PRODUCT,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);

        for (var entry : expected.entrySet()) {
            IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                    new IndexingPlan.IndexedField("embedding", "embedding",
                            ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                    .vectorDims(2)
                                    .vectorSimilarity(entry.getKey())
                                    .build())));

            Document doc = mapper.map(message, plan);

            assertThat(doc.getFields("embedding")).hasSize(1);
            assertThat(doc.getFields("embedding")[0].fieldType().vectorSimilarityFunction())
                    .as("similarity %s", entry.getKey())
                    .isEqualTo(entry.getValue());
        }
    }

    @Test
    void byteVectorFromRepeatedInt32BuildsKnnByteField() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_INT32);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(embedding, 1)
                .addRepeatedField(embedding, -2)
                .addRepeatedField(embedding, 127)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(3)
                                .vectorElementType(ai.pipestream.proto.index.spi.VectorElementType.BYTE)
                                .vectorSimilarity(ai.pipestream.proto.index.spi.VectorSimilarity.DOT_PRODUCT)
                                .build())));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("embedding")).hasSize(1);
        assertThat(doc.getFields("embedding")[0])
                .isInstanceOf(org.apache.lucene.document.KnnByteVectorField.class);
        var field = (org.apache.lucene.document.KnnByteVectorField) doc.getFields("embedding")[0];
        assertThat(field.vectorValue()).containsExactly((byte) 1, (byte) -2, (byte) 127);
        assertThat(field.fieldType().vectorSimilarityFunction())
                .isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
    }

    @Test
    void byteVectorFromBytesFieldBuildsKnnByteField() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_BYTES);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("embedding"),
                        com.google.protobuf.ByteString.copyFrom(new byte[]{5, 6, 7}))
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(3)
                                .vectorElementType(ai.pipestream.proto.index.spi.VectorElementType.BYTE)
                                .build())));

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("embedding")).hasSize(1);
        var field = (org.apache.lucene.document.KnnByteVectorField) doc.getFields("embedding")[0];
        assertThat(field.vectorValue()).containsExactly((byte) 5, (byte) 6, (byte) 7);
    }

    @Test
    void subFieldsEmitAdditionalIndexableFields() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("title", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "Hello")
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                                .build())));

        Document doc = mapper.map(message, plan);

        // main text field plus one indexed-only keyword companion named "title.raw"
        assertThat(doc.get("title")).isEqualTo("Hello");
        assertThat(doc.getFields("title.raw")).hasSize(1);
        assertThat(doc.getFields("title.raw")[0].stringValue()).isEqualTo("Hello");
        assertThat(doc.getFields("title.raw")[0].fieldType().tokenized()).isFalse();
        assertThat(doc.getFields("title.raw")[0].fieldType().stored()).isFalse();
    }

    @Test
    void sortableAndFacetableKeywordAddDocValues() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("status", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("status"), "open")
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("status", "status",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .sortable(true)
                                .facetable(true)
                                .build())));

        Document doc = mapper.map(message, plan);

        List<DocValuesType> docValues = Arrays.stream(doc.getFields("status"))
                .map(field -> field.fieldType().docValuesType())
                .filter(type -> type != DocValuesType.NONE)
                .toList();
        // Lucene allows one doc-values type per field; the multi-valued form serves both
        // faceting and sorting (SortedSetSortField), so it wins when both are hinted.
        assertThat(docValues).containsExactly(DocValuesType.SORTED_SET);
    }

    @Test
    void sortableAndFacetableNumericsAddNumericDocValues() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("count", FieldDescriptorProto.Type.TYPE_INT64);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("count"), 42L)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("count", "count",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64)
                                .sortable(true)
                                .facetable(true)
                                .build())));

        Document doc = mapper.map(message, plan);

        List<DocValuesType> docValues = Arrays.stream(doc.getFields("count"))
                .map(field -> field.fieldType().docValuesType())
                .filter(type -> type != DocValuesType.NONE)
                .toList();
        // One doc-values type per field: SORTED_NUMERIC serves faceting and sorting
        // (SortedNumericSortField) alike.
        assertThat(docValues).containsExactly(DocValuesType.SORTED_NUMERIC);
    }

    @Test
    void sortableFloatAndDoubleAddTypedDocValues() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("score", FieldDescriptorProto.Type.TYPE_DOUBLE);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("score"), 2.5d)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("score", "score",
                        ResolvedFieldHint.builder(IndexFieldKind.DOUBLE)
                                .sortable(true)
                                .build())));

        Document doc = mapper.map(message, plan);

        List<DocValuesType> docValues = Arrays.stream(doc.getFields("score"))
                .map(field -> field.fieldType().docValuesType())
                .filter(type -> type != DocValuesType.NONE)
                .toList();
        assertThat(docValues).containsExactly(DocValuesType.NUMERIC);
    }

    @Test
    void nullValueSubstitutesMissingField() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("status", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("status", "status",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .nullValue("unknown")
                                .build())));

        Document doc = mapper.map(message, plan);

        assertThat(doc.get("status")).isEqualTo("unknown");
    }

    @Test
    void missingFieldWithoutNullValueStaysAbsent() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("status", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("status", "status",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThat(mapper.map(message, plan).getFields("status")).isEmpty();
    }

    @Test
    void mapModeFlattenEmitsOneFieldPerKey() throws Exception {
        IndexingPlan plan = mapPlan(ai.pipestream.proto.index.spi.MapMode.FLATTEN);

        Document doc = mapper.map(labelsMessage(), plan);

        assertThat(doc.get("labels.env")).isEqualTo("prod");
        assertThat(doc.get("labels.team")).isEqualTo("search");
        assertThat(doc.getFields("labels")).isEmpty();
    }

    @Test
    void mapModeEntriesEmitsOneJsonObjectPerEntry() throws Exception {
        IndexingPlan plan = mapPlan(ai.pipestream.proto.index.spi.MapMode.ENTRIES);

        Document doc = mapper.map(labelsMessage(), plan);

        List<String> entries = Arrays.stream(doc.getFields("labels"))
                .map(IndexableField::stringValue)
                .toList();
        assertThat(entries).containsExactly(
                "{\"key\":\"env\",\"value\":\"prod\"}",
                "{\"key\":\"team\",\"value\":\"search\"}");
    }

    @Test
    void mapModeJsonEmitsWholeMapJsonEvenOnScalarHint() throws Exception {
        Descriptor descriptor = mapFieldDescriptor();
        // an explicit mode wins for any hinted kind, not just OBJECT/NESTED
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("labels", "labels",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .mapMode(ai.pipestream.proto.index.spi.MapMode.JSON)
                                .build())));

        Document doc = mapper.map(labelsMessage(), plan);

        assertThat(doc.getFields("labels")).hasSize(1);
        assertThat(doc.get("labels")).isEqualTo("{\"env\":\"prod\",\"team\":\"search\"}");
    }

    @Test
    void mapModeSkipEmitsNothing() throws Exception {
        IndexingPlan plan = mapPlan(ai.pipestream.proto.index.spi.MapMode.SKIP);

        Document doc = mapper.map(labelsMessage(), plan);

        assertThat(doc.getFields()).isEmpty();
    }

    @Test
    void intRangeFromGteLteBoundsBuildsIntRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_INT32);
        DynamicMessage message = rangeMessage(descriptor, 3, 9);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.INT_RANGE);

        Document doc = mapper.map(message, plan);

        assertThat(doc.getFields("pages")).hasSize(1);
        org.apache.lucene.document.IntRange range =
                (org.apache.lucene.document.IntRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(3);
        assertThat(range.getMax(0)).isEqualTo(9);
    }

    @Test
    void longRangeFromMinMaxBoundsBuildsLongRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("min", "max", FieldDescriptorProto.Type.TYPE_INT64);
        DynamicMessage message = rangeMessage(descriptor, 10L, 20L);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.LONG_RANGE);

        Document doc = mapper.map(message, plan);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(10L);
        assertThat(range.getMax(0)).isEqualTo(20L);
    }

    @Test
    void floatRangeBuildsFloatRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_FLOAT);
        DynamicMessage message = rangeMessage(descriptor, 0.5f, 1.5f);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.FLOAT_RANGE);

        Document doc = mapper.map(message, plan);

        org.apache.lucene.document.FloatRange range =
                (org.apache.lucene.document.FloatRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(0.5f);
        assertThat(range.getMax(0)).isEqualTo(1.5f);
    }

    @Test
    void doubleRangeBuildsDoubleRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_DOUBLE);
        DynamicMessage message = rangeMessage(descriptor, 0.25d, 0.75d);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.DOUBLE_RANGE);

        Document doc = mapper.map(message, plan);

        org.apache.lucene.document.DoubleRange range =
                (org.apache.lucene.document.DoubleRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(0.25d);
        assertThat(range.getMax(0)).isEqualTo(0.75d);
    }

    @Test
    void dateRangeFromTimestampBoundsBuildsLongRangeOfEpochMillis() throws Exception {
        Descriptor descriptor = timestampRangeDescriptor();
        DynamicMessage message = timestampRangeMessage(descriptor, 1_700_000_000L, 1_700_000_100L);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.DATE_RANGE);

        Document doc = mapper.map(message, plan);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(1_700_000_000_000L);
        assertThat(range.getMax(0)).isEqualTo(1_700_000_100_000L);
    }

    @Test
    void dateRangeHonoursSecondsResolution() throws Exception {
        Descriptor descriptor = timestampRangeDescriptor();
        DynamicMessage message = timestampRangeMessage(descriptor, 1_700_000_000L, 1_700_000_100L);
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("pages", "pages",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE_RANGE)
                                .dateResolution(ai.pipestream.proto.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, plan);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(1_700_000_000L);
        assertThat(range.getMax(0)).isEqualTo(1_700_000_100L);
    }

    @Test
    void rangeWithoutResolvableBoundsThrowsMappingException() throws Exception {
        Descriptor descriptor = rangeDescriptor("low", "high", FieldDescriptorProto.Type.TYPE_INT32);
        DynamicMessage message = rangeMessage(descriptor, 1, 2);
        IndexingPlan plan = rangePlan(descriptor, IndexFieldKind.INT_RANGE);

        assertThatThrownBy(() -> mapper.map(message, plan))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("(gte,lte) or (min,max)");
    }

    @Test
    void dateResolutionSecondsEmitsEpochSeconds() throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, DynamicMessage.newBuilder(tsDescriptor)
                        .setField(tsDescriptor.findFieldByName("seconds"), 1_700_000_000L)
                        .setField(tsDescriptor.findFieldByName("nanos"), 500_000_000)
                        .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.pipestream.proto.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, plan);

        List<Long> stored = Arrays.stream(doc.getFields("created"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(1_700_000_000L);
    }

    @Test
    void emptyProjectionsYieldEmptyDocument() throws Exception {
        assertThat(mapper.map(Struct.getDefaultInstance(), List.<ProtoLuceneMapper.FieldProjection>of()).getFields()).isEmpty();
        assertThat(mapper.map(Struct.getDefaultInstance(), (List<ProtoLuceneMapper.FieldProjection>) null).getFields()).isEmpty();
    }

    /** Explicit-mode plan over {@link #mapFieldDescriptor()} with an OBJECT hint. */
    private static IndexingPlan mapPlan(ai.pipestream.proto.index.spi.MapMode mode) throws Exception {
        Descriptor descriptor = mapFieldDescriptor();
        return new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("labels", "labels",
                        ResolvedFieldHint.builder(IndexFieldKind.OBJECT).mapMode(mode).build())));
    }

    private static DynamicMessage labelsMessage() throws Exception {
        Descriptor descriptor = mapFieldDescriptor();
        FieldDescriptor labels = descriptor.findFieldByName("labels");
        Descriptor entryType = labels.getMessageType();
        return DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(labels, DynamicMessage.newBuilder(entryType)
                        .setField(entryType.findFieldByName("key"), "env")
                        .setField(entryType.findFieldByName("value"), "prod")
                        .build())
                .addRepeatedField(labels, DynamicMessage.newBuilder(entryType)
                        .setField(entryType.findFieldByName("key"), "team")
                        .setField(entryType.findFieldByName("value"), "search")
                        .build())
                .build();
    }

    private static IndexingPlan rangePlan(Descriptor descriptor, IndexFieldKind rangeKind) {
        return new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("pages", "pages", ResolvedFieldHint.of(rangeKind))));
    }

    private static DynamicMessage rangeMessage(Descriptor descriptor, Object lower, Object upper) {
        FieldDescriptor pages = descriptor.findFieldByName("pages");
        Descriptor boundsType = pages.getMessageType();
        List<FieldDescriptor> bounds = boundsType.getFields();
        return DynamicMessage.newBuilder(descriptor)
                .setField(pages, DynamicMessage.newBuilder(boundsType)
                        .setField(bounds.get(0), lower)
                        .setField(bounds.get(1), upper)
                        .build())
                .build();
    }

    private static DynamicMessage timestampRangeMessage(
            Descriptor descriptor, long lowerSeconds, long upperSeconds) {
        FieldDescriptor pages = descriptor.findFieldByName("pages");
        Descriptor boundsType = pages.getMessageType();
        Descriptor tsType = boundsType.getFields().get(0).getMessageType();
        return DynamicMessage.newBuilder(descriptor)
                .setField(pages, DynamicMessage.newBuilder(boundsType)
                        .setField(boundsType.getFields().get(0), DynamicMessage.newBuilder(tsType)
                                .setField(tsType.findFieldByName("seconds"), lowerSeconds)
                                .build())
                        .setField(boundsType.getFields().get(1), DynamicMessage.newBuilder(tsType)
                                .setField(tsType.findFieldByName("seconds"), upperSeconds)
                                .build())
                        .build())
                .build();
    }

    private static Descriptor rangeDescriptor(
            String lowerName, String upperName, FieldDescriptorProto.Type boundType) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("range_" + lowerName + "_" + boundType.name().toLowerCase() + ".proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Bounds")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(lowerName)
                                .setNumber(1)
                                .setType(boundType)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(upperName)
                                .setNumber(2)
                                .setType(boundType)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("RangeDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("pages")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Bounds")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("RangeDoc");
    }

    private static Descriptor timestampRangeDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("ts_range.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Bounds")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("gte")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Timestamp")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("lte")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Timestamp")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("RangeDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("pages")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Bounds")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{TimestampProto.getDescriptor()})
                .findMessageTypeByName("RangeDoc");
    }

    private static Descriptor singularFieldDescriptor(String fieldName, FieldDescriptorProto.Type type) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName(fieldName + "_singular.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(fieldName)
                                .setNumber(1)
                                .setType(type)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }

    private static Descriptor timestampDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("ts_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("TsDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("created")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Timestamp")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{TimestampProto.getDescriptor()})
                .findMessageTypeByName("TsDoc");
    }

    private static Descriptor nestedDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("nested_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Inner")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("name")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("inner")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Inner")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }

    private static Descriptor mapFieldDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("map_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("MapDoc")
                        .addNestedType(DescriptorProto.newBuilder()
                                .setName("LabelsEntry")
                                .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder()
                                        .setMapEntry(true))
                                .addField(FieldDescriptorProto.newBuilder()
                                        .setName("key")
                                        .setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                .addField(FieldDescriptorProto.newBuilder()
                                        .setName("value")
                                        .setNumber(2)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("labels")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.MapDoc.LabelsEntry")
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("MapDoc");
    }

    private static Descriptor repeatedFieldDescriptor(String fieldName, FieldDescriptorProto.Type type) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName(fieldName + ".proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName(fieldName)
                                .setNumber(1)
                                .setType(type)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
