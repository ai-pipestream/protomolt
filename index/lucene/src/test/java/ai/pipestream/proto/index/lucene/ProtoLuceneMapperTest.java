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
import org.apache.lucene.index.IndexableField;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

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
    void emptyProjectionsYieldEmptyDocument() throws Exception {
        assertThat(mapper.map(Struct.getDefaultInstance(), List.<ProtoLuceneMapper.FieldProjection>of()).getFields()).isEmpty();
        assertThat(mapper.map(Struct.getDefaultInstance(), (List<ProtoLuceneMapper.FieldProjection>) null).getFields()).isEmpty();
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
