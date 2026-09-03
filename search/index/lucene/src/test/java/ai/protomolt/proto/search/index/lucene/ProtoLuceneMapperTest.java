package ai.protomolt.proto.search.index.lucene;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("values", "values", ResolvedFieldHint.of(IndexFieldKind.INT64))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("tags", "tags", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("created", "created", ResolvedFieldHint.of(IndexFieldKind.DATE))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("created_ms", "created_ms", ResolvedFieldHint.of(IndexFieldKind.DATE))));

        Document doc = mapper.map(message, mapping);

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
    void unsetIntermediateMessageInMappingPathSkipsField() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "kept")
                .build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("inner.name", "inner_name", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("title", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        Document doc = mapper.map(message, mapping);

        assertThat(doc.get("title")).isEqualTo("kept");
        assertThat(doc.getFields("inner_name")).isEmpty();
    }

    @Test
    void genuinelyInvalidMappingPathStillThrows() throws Exception {
        Descriptor descriptor = nestedDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("nope.name", "nope", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThatThrownBy(() -> mapper.map(message, mapping)).isInstanceOf(MappingException.class);
    }

    @Test
    void indexedOnlyByteStringProducesExactMatchField() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("digest", FieldDescriptorProto.Type.TYPE_BYTES);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("digest"),
                        com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("digest", "digest",
                        new ResolvedFieldHint(IndexFieldKind.BINARY, false, true, "", 0))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("digest", "digest",
                        new ResolvedFieldHint(IndexFieldKind.BINARY, true, false, "", 0))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("inner", "inner",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, mapping);

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
            IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                    new IndexMapping.IndexedField("inner", "inner", ResolvedFieldHint.of(kind))));

            Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("inner", "inner",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("inner", "inner",
                        ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                                .stored(false).indexed(true).build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("inner", "inner",
                        ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                                .stored(true).indexed(false).build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.protomolt.proto.search.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.protomolt.proto.search.index.spi.DateResolution.SECONDS)
                                .build())));

        return Arrays.stream(mapper.map(message, mapping).getFields("created"))
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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("labels", "labels",
                        ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 3))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 3))));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("count", "count", ResolvedFieldHint.of(IndexFieldKind.INT64))));

        assertThatThrownBy(() -> mapper.map(message, mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("count")
                .hasMessageContaining("INT64")
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void includeDefaultsIndexesImplicitPresenceDefaults() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("archived", FieldDescriptorProto.Type.TYPE_BOOL);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("archived", "archived",
                        ResolvedFieldHint.of(IndexFieldKind.BOOLEAN))));

        // default behaviour: fields at their default value are skipped
        assertThat(mapper.map(message, mapping).getFields("archived")).isEmpty();

        ProtoLuceneMapper withDefaults = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()), true);
        Document doc = withDefaults.map(message, mapping);
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
        Map<ai.protomolt.proto.search.index.spi.VectorSimilarity, VectorSimilarityFunction> expected = Map.of(
                ai.protomolt.proto.search.index.spi.VectorSimilarity.COSINE, VectorSimilarityFunction.COSINE,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.DOT_PRODUCT, VectorSimilarityFunction.DOT_PRODUCT,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.L2, VectorSimilarityFunction.EUCLIDEAN,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.MAX_INNER_PRODUCT,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);

        for (var entry : expected.entrySet()) {
            IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                    new IndexMapping.IndexedField("embedding", "embedding",
                            ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                    .vectorDims(2)
                                    .vectorSimilarity(entry.getKey())
                                    .build())));

            Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(3)
                                .vectorElementType(ai.protomolt.proto.search.index.spi.VectorElementType.BYTE)
                                .vectorSimilarity(ai.protomolt.proto.search.index.spi.VectorSimilarity.DOT_PRODUCT)
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(3)
                                .vectorElementType(ai.protomolt.proto.search.index.spi.VectorElementType.BYTE)
                                .build())));

        Document doc = mapper.map(message, mapping);

        assertThat(doc.getFields("embedding")).hasSize(1);
        var field = (org.apache.lucene.document.KnnByteVectorField) doc.getFields("embedding")[0];
        assertThat(field.vectorValue()).containsExactly((byte) 5, (byte) 6, (byte) 7);
    }

    @Test
    void toFloatVectorConvertsEverySupportedShape() {
        assertThat(ProtoLuceneMapper.toFloatVector(new float[]{1f, 2f, 3f}))
                .containsExactly(1f, 2f, 3f);
        assertThat(ProtoLuceneMapper.toFloatVector(new double[]{1.5, 2.5}))
                .containsExactly(1.5f, 2.5f);
        assertThat(ProtoLuceneMapper.toFloatVector(List.of(0.1f, 0.2f)))
                .containsExactly(0.1f, 0.2f);
        assertThat(ProtoLuceneMapper.toFloatVector(List.of(0.25, 0.75)))
                .containsExactly(0.25f, 0.75f);
        assertThat(ProtoLuceneMapper.toFloatVector(List.of())).isEmpty();
        assertThat(ProtoLuceneMapper.toFloatVector(new double[0])).isEmpty();
    }

    @Test
    void toFloatVectorRejectsNonNumericElementsAndUnsupportedTypes() {
        assertThatThrownBy(() -> ProtoLuceneMapper.toFloatVector(List.of(1f, "not-a-number")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numeric");
        assertThatThrownBy(() -> ProtoLuceneMapper.toFloatVector("not-a-vector"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("java.lang.String");
        assertThatThrownBy(() -> ProtoLuceneMapper.toFloatVector(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void emptyVectorIsSkipped() throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_FLOAT);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 3))));

        assertThat(mapper.map(message, mapping).getFields("embedding")).isEmpty();
    }

    @Test
    void knnVectorFieldRoundTripsThroughLuceneIndexWriter(@TempDir Path dir) throws Exception {
        Descriptor descriptor = repeatedFieldDescriptor("embedding", FieldDescriptorProto.Type.TYPE_FLOAT);
        FieldDescriptor embedding = descriptor.findFieldByName("embedding");
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        new ResolvedFieldHint(IndexFieldKind.VECTOR, true, true, "", 2))));

        try (LuceneIndexWriter writer = new LuceneIndexWriter(dir)) {
            writer.add(vectorDoc(mapper, descriptor, embedding, mapping, "near", 1f, 0f));
            writer.add(vectorDoc(mapper, descriptor, embedding, mapping, "far", 0f, 1f));
            writer.commit();
            assertThat(writer.numDocs()).isEqualTo(2);
        }

        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(dir))) {
            TopDocs hits = new IndexSearcher(reader).search(
                    new KnnFloatVectorQuery("embedding", new float[]{0.9f, 0.1f}, 1), 1);
            assertThat(hits.scoreDocs).hasSize(1);
            Document hit = new IndexSearcher(reader)
                    .storedFields().document(hits.scoreDocs[0].doc);
            assertThat(hit.get("id")).isEqualTo("near");
        }
    }

    private static Document vectorDoc(ProtoLuceneMapper mapper, Descriptor descriptor,
                                      FieldDescriptor embedding, IndexMapping mapping,
                                      String id, float... vector) throws Exception {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (float component : vector) {
            builder.addRepeatedField(embedding, component);
        }
        Document doc = mapper.map(builder.build(), mapping);
        doc.add(new org.apache.lucene.document.StringField(
                "id", id, org.apache.lucene.document.Field.Store.YES));
        return doc;
    }

    @Test
    void subFieldsEmitAdditionalIndexableFields() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("title", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "Hello")
                .build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("status", "status",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .sortable(true)
                                .facetable(true)
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("count", "count",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64)
                                .sortable(true)
                                .facetable(true)
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("score", "score",
                        ResolvedFieldHint.builder(IndexFieldKind.DOUBLE)
                                .sortable(true)
                                .build())));

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("status", "status",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .nullValue("unknown")
                                .build())));

        Document doc = mapper.map(message, mapping);

        assertThat(doc.get("status")).isEqualTo("unknown");
    }

    @Test
    void missingFieldWithoutNullValueStaysAbsent() throws Exception {
        Descriptor descriptor = singularFieldDescriptor("status", FieldDescriptorProto.Type.TYPE_STRING);
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("status", "status",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThat(mapper.map(message, mapping).getFields("status")).isEmpty();
    }

    @Test
    void mapModeFlattenEmitsOneFieldPerKey() throws Exception {
        IndexMapping mapping = mapModeMapping(ai.protomolt.proto.search.index.spi.MapMode.FLATTEN);

        Document doc = mapper.map(labelsMessage(), mapping);

        assertThat(doc.get("labels.env")).isEqualTo("prod");
        assertThat(doc.get("labels.team")).isEqualTo("search");
        assertThat(doc.getFields("labels")).isEmpty();
    }

    @Test
    void mapModeEntriesEmitsOneJsonObjectPerEntry() throws Exception {
        IndexMapping mapping = mapModeMapping(ai.protomolt.proto.search.index.spi.MapMode.ENTRIES);

        Document doc = mapper.map(labelsMessage(), mapping);

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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("labels", "labels",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .mapMode(ai.protomolt.proto.search.index.spi.MapMode.JSON)
                                .build())));

        Document doc = mapper.map(labelsMessage(), mapping);

        assertThat(doc.getFields("labels")).hasSize(1);
        assertThat(doc.get("labels")).isEqualTo("{\"env\":\"prod\",\"team\":\"search\"}");
    }

    @Test
    void mapModeSkipEmitsNothing() throws Exception {
        IndexMapping mapping = mapModeMapping(ai.protomolt.proto.search.index.spi.MapMode.SKIP);

        Document doc = mapper.map(labelsMessage(), mapping);

        assertThat(doc.getFields()).isEmpty();
    }

    @Test
    void intRangeFromGteLteBoundsBuildsIntRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_INT32);
        DynamicMessage message = rangeMessage(descriptor, 3, 9);
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.INT_RANGE);

        Document doc = mapper.map(message, mapping);

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
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.LONG_RANGE);

        Document doc = mapper.map(message, mapping);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(10L);
        assertThat(range.getMax(0)).isEqualTo(20L);
    }

    @Test
    void floatRangeBuildsFloatRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_FLOAT);
        DynamicMessage message = rangeMessage(descriptor, 0.5f, 1.5f);
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.FLOAT_RANGE);

        Document doc = mapper.map(message, mapping);

        org.apache.lucene.document.FloatRange range =
                (org.apache.lucene.document.FloatRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(0.5f);
        assertThat(range.getMax(0)).isEqualTo(1.5f);
    }

    @Test
    void doubleRangeBuildsDoubleRange() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_DOUBLE);
        DynamicMessage message = rangeMessage(descriptor, 0.25d, 0.75d);
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.DOUBLE_RANGE);

        Document doc = mapper.map(message, mapping);

        org.apache.lucene.document.DoubleRange range =
                (org.apache.lucene.document.DoubleRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(0.25d);
        assertThat(range.getMax(0)).isEqualTo(0.75d);
    }

    @Test
    void dateRangeFromTimestampBoundsBuildsLongRangeOfEpochMillis() throws Exception {
        Descriptor descriptor = timestampRangeDescriptor();
        DynamicMessage message = timestampRangeMessage(descriptor, 1_700_000_000L, 1_700_000_100L);
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.DATE_RANGE);

        Document doc = mapper.map(message, mapping);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(1_700_000_000_000L);
        assertThat(range.getMax(0)).isEqualTo(1_700_000_100_000L);
    }

    @Test
    void dateRangeHonoursSecondsResolution() throws Exception {
        Descriptor descriptor = timestampRangeDescriptor();
        DynamicMessage message = timestampRangeMessage(descriptor, 1_700_000_000L, 1_700_000_100L);
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("pages", "pages",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE_RANGE)
                                .dateResolution(ai.protomolt.proto.search.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, mapping);

        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("pages")[0];
        assertThat(range.getMin(0)).isEqualTo(1_700_000_000L);
        assertThat(range.getMax(0)).isEqualTo(1_700_000_100L);
    }

    @Test
    void rangeWithoutResolvableBoundsThrowsMappingException() throws Exception {
        Descriptor descriptor = rangeDescriptor("low", "high", FieldDescriptorProto.Type.TYPE_INT32);
        DynamicMessage message = rangeMessage(descriptor, 1, 2);
        IndexMapping mapping = rangeMapping(descriptor, IndexFieldKind.INT_RANGE);

        assertThatThrownBy(() -> mapper.map(message, mapping))
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
        IndexMapping mapping = new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.protomolt.proto.search.index.spi.DateResolution.SECONDS)
                                .build())));

        Document doc = mapper.map(message, mapping);

        List<Long> stored = Arrays.stream(doc.getFields("created"))
                .filter(field -> field.fieldType().stored())
                .map(field -> field.numericValue().longValue())
                .toList();
        assertThat(stored).containsExactly(1_700_000_000L);
    }

    @Test
    void unpacksRegistryKnownAnyIntoPrefixedInnerFields() throws Exception {
        AnyEnvelope env = AnyEnvelope.create();
        DynamicMessage message = env.packed("Opinion", 12);
        ProtoLuceneMapper lucene = new ProtoLuceneMapper(env.context());
        IndexMapping mapping = env.factory.create(env.envelope);

        Document doc = lucene.map(message, mapping);

        assertThat(doc.get("payload_title")).isEqualTo("Opinion");
        assertThat(doc.getField("payload_page_count").numericValue().intValue()).isEqualTo(12);
        assertThat(doc.get("doc_id")).isEqualTo("doc-1");
        assertThat(doc.get("payload")).isNull();
    }

    @Test
    void unknownAnyTypeUrlFailsWithPathAndTypeUrlWithoutReturningADocument() throws Exception {
        AnyEnvelope env = AnyEnvelope.create();
        DynamicMessage message = env.unknownType();
        ProtoLuceneMapper lucene = new ProtoLuceneMapper(env.context());
        IndexMapping mapping = env.factory.create(env.envelope);

        assertThatThrownBy(() -> lucene.map(message, mapping))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload")
                .hasMessageContaining("type.googleapis.com/ai.pipestream.test.MissingType");
    }

    @Test
    void unsetAnyDoesNotFailAndOmitsInnerFields() throws Exception {
        AnyEnvelope env = AnyEnvelope.create();
        DynamicMessage message = DynamicMessage.newBuilder(env.envelope)
                .setField(env.envelope.findFieldByName("doc_id"), "doc-1")
                .build();
        ProtoLuceneMapper lucene = new ProtoLuceneMapper(env.context());
        IndexMapping mapping = env.factory.create(env.envelope);

        Document doc = lucene.map(message, mapping);

        assertThat(doc.get("doc_id")).isEqualTo("doc-1");
        assertThat(doc.get("payload_title")).isNull();
        assertThat(doc.getFields("payload")).isEmpty();
    }

    @Test
    void emptyProjectionsYieldEmptyDocument() throws Exception {
        assertThat(mapper.map(Struct.getDefaultInstance(), List.<ProtoLuceneMapper.FieldProjection>of()).getFields()).isEmpty();
        assertThat(mapper.map(Struct.getDefaultInstance(), (List<ProtoLuceneMapper.FieldProjection>) null).getFields()).isEmpty();
    }

    private record AnyEnvelope(
            Descriptor envelope,
            Descriptor inner,
            IndexMappingFactory factory,
            DescriptorRegistry registry) {

        static AnyEnvelope create() throws Exception {
            FileDescriptor file = FileDescriptor.buildFrom(
                    FileDescriptorProto.newBuilder()
                            .setName("any_lucene.proto")
                            .setPackage("ai.pipestream.test")
                            .setSyntax("proto3")
                            .addDependency("google/protobuf/any.proto")
                            .addMessageType(DescriptorProto.newBuilder()
                                    .setName("InnerPayload")
                                    .addField(FieldDescriptorProto.newBuilder()
                                            .setName("title")
                                            .setNumber(1)
                                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                    .addField(FieldDescriptorProto.newBuilder()
                                            .setName("page_count")
                                            .setNumber(2)
                                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                            .addMessageType(DescriptorProto.newBuilder()
                                    .setName("Envelope")
                                    .addField(FieldDescriptorProto.newBuilder()
                                            .setName("doc_id")
                                            .setNumber(1)
                                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                    .addField(FieldDescriptorProto.newBuilder()
                                            .setName("payload")
                                            .setNumber(2)
                                            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                            .setTypeName(".google.protobuf.Any")
                                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                            .build(),
                    new FileDescriptor[]{AnyProto.getDescriptor()});
            Descriptor envelope = file.findMessageTypeByName("Envelope");
            Descriptor inner = file.findMessageTypeByName("InnerPayload");
            DescriptorRegistry registry = new DescriptorRegistry();
            registry.register(inner);
            CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                    .put(inner.getFullName(), "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
            return new AnyEnvelope(envelope, inner, IndexMappingFactory.defaults(catalog), registry);
        }

        IndexerContext context() {
            return new IndexerContext(new ProtoFieldMapperImpl(registry), registry, factory);
        }

        DynamicMessage packed(String title, int pageCount) {
            DynamicMessage innerMessage = DynamicMessage.newBuilder(inner)
                    .setField(inner.findFieldByName("title"), title)
                    .setField(inner.findFieldByName("page_count"), pageCount)
                    .build();
            return DynamicMessage.newBuilder(envelope)
                    .setField(envelope.findFieldByName("doc_id"), "doc-1")
                    .setField(envelope.findFieldByName("payload"), Any.pack(innerMessage))
                    .build();
        }

        DynamicMessage unknownType() {
            return DynamicMessage.newBuilder(envelope)
                    .setField(envelope.findFieldByName("doc_id"), "doc-1")
                    .setField(envelope.findFieldByName("payload"), Any.newBuilder()
                            .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                            .setValue(com.google.protobuf.ByteString.copyFromUtf8("x"))
                            .build())
                    .build();
        }
    }

    /** Explicit-mode mapping over {@link #mapFieldDescriptor()} with an OBJECT hint. */
    private static IndexMapping mapModeMapping(ai.protomolt.proto.search.index.spi.MapMode mode) throws Exception {
        Descriptor descriptor = mapFieldDescriptor();
        return new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("labels", "labels",
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

    private static IndexMapping rangeMapping(Descriptor descriptor, IndexFieldKind rangeKind) {
        return new IndexMapping(descriptor.getFullName(), List.of(
                new IndexMapping.IndexedField("pages", "pages", ResolvedFieldHint.of(rangeKind))));
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
