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
