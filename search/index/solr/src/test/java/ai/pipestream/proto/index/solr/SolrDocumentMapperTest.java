package ai.pipestream.proto.index.solr;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SolrDocumentMapperTest {

    private final SolrDocumentMapper mapper =
            new SolrDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void projectsSelectedPaths() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setStringValue("doc-1").build())
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .build();
        Map<String, Object> doc = mapper.map(message, List.of(
                new SolrDocumentMapper.FieldProjection("id", "id"),
                new SolrDocumentMapper.FieldProjection("title", "title_s")
        ));
        assertThat(doc).containsEntry("id", "doc-1").containsEntry("title_s", "Hello");
    }

    @Test
    void nullProjectionsYieldEmptyMap() throws Exception {
        assertThat(mapper.map(Struct.getDefaultInstance(), (List<SolrDocumentMapper.FieldProjection>) null)).isEmpty();
    }

    @Test
    void skipsMissingPaths() throws Exception {
        Struct message = Struct.newBuilder()
                .putFields("id", Value.newBuilder().setStringValue("1").build())
                .build();
        Map<String, Object> doc = mapper.map(message, List.of(
                new SolrDocumentMapper.FieldProjection("nope", "n")
        ));
        assertThat(doc).isEmpty();
    }

    @Test
    void coercesRepeatedEnumToNames() throws Exception {
        Descriptor descriptor = docDescriptor();
        FieldDescriptor colors = descriptor.findFieldByName("colors");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(colors, colors.getEnumType().findValueByName("RED"))
                .addRepeatedField(colors, colors.getEnumType().findValueByName("BLUE"))
                .build();

        Map<String, Object> doc = mapper.map(message, List.of(
                new SolrDocumentMapper.FieldProjection("colors", "colors")
        ));

        assertThat(doc.get("colors")).isEqualTo(List.of("RED", "BLUE"));
    }

    @Test
    void coercesNestedMessageToJsonString() throws Exception {
        Descriptor descriptor = docDescriptor();
        Descriptor innerDescriptor = descriptor.findFieldByName("inner").getMessageType();
        DynamicMessage inner = DynamicMessage.newBuilder(innerDescriptor)
                .setField(innerDescriptor.findFieldByName("name"), "n1")
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("inner"), inner)
                .build();

        Map<String, Object> doc = mapper.map(message, List.of(
                new SolrDocumentMapper.FieldProjection("inner", "inner_json")
        ));

        assertThat(doc.get("inner_json")).isInstanceOf(String.class);
        assertThat((String) doc.get("inner_json")).contains("\"name\"").contains("n1");
    }

    @Test
    void dateHintedTimestampBecomesIso8601String() throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        DynamicMessage timestamp = DynamicMessage.newBuilder(tsDescriptor)
                .setField(tsDescriptor.findFieldByName("seconds"), 1_700_000_000L)
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, timestamp)
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created", ResolvedFieldHint.of(IndexFieldKind.DATE))));

        Map<String, Object> doc = mapper.map(message, plan);

        // the raw RFC3339 string, never the quoted JSON literal "\"...\""
        assertThat(doc.get("created")).isEqualTo("2023-11-14T22:13:20Z");
    }

    @Test
    void primitivePrintingWellKnownTypesLoseTheirJsonQuotes() throws Exception {
        Descriptor descriptor = wktDescriptor();
        Descriptor duration = descriptor.findFieldByName("ttl").getMessageType();
        Descriptor int64Value = descriptor.findFieldByName("count").getMessageType();
        Descriptor boolValue = descriptor.findFieldByName("flag").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("ttl"), DynamicMessage.newBuilder(duration)
                        .setField(duration.findFieldByName("seconds"), 90L)
                        .build())
                .setField(descriptor.findFieldByName("count"), DynamicMessage.newBuilder(int64Value)
                        .setField(int64Value.findFieldByName("value"), 42L)
                        .build())
                .setField(descriptor.findFieldByName("flag"), DynamicMessage.newBuilder(boolValue)
                        .setField(boolValue.findFieldByName("value"), true)
                        .build())
                .build();

        Map<String, Object> doc = mapper.map(message, List.of(
                new SolrDocumentMapper.FieldProjection("ttl", "ttl"),
                new SolrDocumentMapper.FieldProjection("count", "count"),
                new SolrDocumentMapper.FieldProjection("flag", "flag")
        ));

        assertThat(doc.get("ttl")).isEqualTo("90s");
        // Int64Value prints as a JSON string per proto3 canonical JSON
        assertThat(doc.get("count")).isEqualTo("42");
        assertThat(doc.get("flag")).isEqualTo(true);
    }

    @Test
    void unsetIntermediateMessageInPlanPathSkipsField() throws Exception {
        Descriptor descriptor = docDescriptor();
        FieldDescriptor colors = descriptor.findFieldByName("colors");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(colors, colors.getEnumType().findValueByName("RED"))
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("inner.name", "inner_name", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexingPlan.IndexedField("colors", "colors", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        Map<String, Object> doc = mapper.map(message, plan);

        assertThat(doc).containsEntry("colors", List.of("RED")).doesNotContainKey("inner_name");
    }

    @Test
    void includeDefaultsWritesImplicitPresenceDefaults() throws Exception {
        Descriptor descriptor = boolDocDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("archived", "archived",
                        ResolvedFieldHint.of(IndexFieldKind.BOOLEAN))));

        // default behaviour: fields at their default value are skipped
        assertThat(mapper.map(message, plan)).doesNotContainKey("archived");

        SolrDocumentMapper withDefaults = new SolrDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()), true);
        assertThat(withDefaults.map(message, plan)).containsEntry("archived", false);
    }

    @Test
    void genuinelyInvalidPlanPathStillThrows() throws Exception {
        Descriptor descriptor = docDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("nope.name", "nope", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThatThrownBy(() -> mapper.map(message, plan)).isInstanceOf(MappingException.class);
    }

    @Test
    void nullValueSubstitutesMissingField() throws Exception {
        Descriptor descriptor = boolDocDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("archived", "archived",
                        ResolvedFieldHint.builder(IndexFieldKind.BOOLEAN).nullValue("false").build())));

        // the substitute is coerced to the hinted type: a boolean, not the string "false"
        assertThat(mapper.map(message, plan)).containsEntry("archived", false);
    }

    @Test
    void skipIfMissingFalseStillSkipsWithoutSubstitute() throws Exception {
        Descriptor descriptor = boolDocDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor).build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("archived", "archived",
                        ResolvedFieldHint.builder(IndexFieldKind.BOOLEAN).skipIfMissing(false).build())));

        // Solr documents cannot hold explicit nulls
        assertThat(mapper.map(message, plan)).doesNotContainKey("archived");
    }

    @Test
    void mapModeDefaultsToEntryJsonStrings() throws Exception {
        Map<String, Object> doc = mapper.map(labelsMessage(), mapPlan(null));

        assertThat(doc.get("labels")).isEqualTo(List.of(
                "{\"key\":\"env\",\"value\":\"prod\"}",
                "{\"key\":\"team\",\"value\":\"search\"}"));
    }

    @Test
    void mapModeFlattenEmitsUnderscoreKeyFields() throws Exception {
        Map<String, Object> doc = mapper.map(labelsMessage(),
                mapPlan(ai.pipestream.proto.index.spi.MapMode.FLATTEN));

        assertThat(doc).containsEntry("labels_env", "prod").containsEntry("labels_team", "search");
        assertThat(doc).doesNotContainKey("labels");
    }

    @Test
    void mapModeJsonEmitsOneJsonString() throws Exception {
        Map<String, Object> doc = mapper.map(labelsMessage(),
                mapPlan(ai.pipestream.proto.index.spi.MapMode.JSON));

        assertThat(doc.get("labels")).isEqualTo("{\"env\":\"prod\",\"team\":\"search\"}");
    }

    @Test
    void mapModeSkipOmitsField() throws Exception {
        Map<String, Object> doc = mapper.map(labelsMessage(),
                mapPlan(ai.pipestream.proto.index.spi.MapMode.SKIP));

        assertThat(doc).isEmpty();
    }

    @Test
    void intRangeFromGteLteBoundsEmitsMinMaxFields() throws Exception {
        Descriptor descriptor = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_INT32);
        Map<String, Object> doc = mapper.map(
                rangeMessage(descriptor, 3, 9), rangePlan(descriptor, IndexFieldKind.INT_RANGE));

        assertThat(doc).containsEntry("pages_min", 3).containsEntry("pages_max", 9);
        assertThat(doc).doesNotContainKey("pages");
    }

    @Test
    void longRangeFromMinMaxBoundsEmitsMinMaxFields() throws Exception {
        Descriptor descriptor = rangeDescriptor("min", "max", FieldDescriptorProto.Type.TYPE_INT64);
        Map<String, Object> doc = mapper.map(
                rangeMessage(descriptor, 10L, 20L), rangePlan(descriptor, IndexFieldKind.LONG_RANGE));

        assertThat(doc).containsEntry("pages_min", 10L).containsEntry("pages_max", 20L);
    }

    @Test
    void floatAndDoubleRangesEmitNumericBounds() throws Exception {
        Descriptor floats = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_FLOAT);
        assertThat(mapper.map(rangeMessage(floats, 0.5f, 1.5f),
                rangePlan(floats, IndexFieldKind.FLOAT_RANGE)))
                .containsEntry("pages_min", 0.5f).containsEntry("pages_max", 1.5f);

        Descriptor doubles = rangeDescriptor("gte", "lte", FieldDescriptorProto.Type.TYPE_DOUBLE);
        assertThat(mapper.map(rangeMessage(doubles, 0.25d, 0.75d),
                rangePlan(doubles, IndexFieldKind.DOUBLE_RANGE)))
                .containsEntry("pages_min", 0.25d).containsEntry("pages_max", 0.75d);
    }

    @Test
    void dateRangeFromTimestampBoundsEmitsIso8601MinMax() throws Exception {
        Descriptor descriptor = timestampRangeDescriptor();
        Map<String, Object> doc = mapper.map(
                timestampRangeMessage(descriptor, 1_700_000_000L, 1_700_000_100L),
                rangePlan(descriptor, IndexFieldKind.DATE_RANGE));

        assertThat(doc).containsEntry("pages_min", "2023-11-14T22:13:20Z")
                .containsEntry("pages_max", "2023-11-14T22:15:00Z");
    }

    @Test
    void rangeWithoutResolvableBoundsThrowsMappingException() throws Exception {
        Descriptor descriptor = rangeDescriptor("low", "high", FieldDescriptorProto.Type.TYPE_INT32);

        assertThatThrownBy(() -> mapper.map(
                rangeMessage(descriptor, 1, 2), rangePlan(descriptor, IndexFieldKind.INT_RANGE)))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("(gte,lte) or (min,max)");
    }

    @Test
    void dateResolutionDoesNotChangeIso8601Emission() throws Exception {
        Descriptor descriptor = timestampDescriptor();
        FieldDescriptor created = descriptor.findFieldByName("created");
        Descriptor tsDescriptor = created.getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(created, DynamicMessage.newBuilder(tsDescriptor)
                        .setField(tsDescriptor.findFieldByName("seconds"), 1_700_000_000L)
                        .build())
                .build();
        IndexingPlan plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("created", "created",
                        ResolvedFieldHint.builder(IndexFieldKind.DATE)
                                .dateResolution(ai.pipestream.proto.index.spi.DateResolution.SECONDS)
                                .build())));

        // resolution applies only where dates are emitted numerically; documents stay ISO-8601
        assertThat(mapper.map(message, plan)).containsEntry("created", "2023-11-14T22:13:20Z");
    }

    private IndexingPlan mapPlan(ai.pipestream.proto.index.spi.MapMode mode) throws Exception {
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
        return DynamicMessage.newBuilder(descriptor)
                .setField(pages, DynamicMessage.newBuilder(boundsType)
                        .setField(boundsType.getFields().get(0), lower)
                        .setField(boundsType.getFields().get(1), upper)
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
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{com.google.protobuf.TimestampProto.getDescriptor()})
                .findMessageTypeByName("RangeDoc");
    }

    private static Descriptor boolDocDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("bool_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("BoolDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("archived")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("BoolDoc");
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
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{com.google.protobuf.TimestampProto.getDescriptor()})
                .findMessageTypeByName("TsDoc");
    }

    private static Descriptor wktDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("wkt_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/duration.proto")
                .addDependency("google/protobuf/wrappers.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("WktDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("ttl")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Duration")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("count")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Int64Value")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("flag")
                                .setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.BoolValue")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{
                        com.google.protobuf.DurationProto.getDescriptor(),
                        com.google.protobuf.WrappersProto.getDescriptor()})
                .findMessageTypeByName("WktDoc");
    }

    private static Descriptor docDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addEnumType(EnumDescriptorProto.newBuilder()
                        .setName("Color")
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("COLOR_UNSPECIFIED").setNumber(0))
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(1))
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2)))
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
                                .setName("colors")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_ENUM)
                                .setTypeName(".ai.pipestream.test.Color")
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("inner")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.test.Inner")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
