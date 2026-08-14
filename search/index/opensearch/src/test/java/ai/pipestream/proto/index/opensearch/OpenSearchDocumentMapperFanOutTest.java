package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fan-out edge cases for the OpenSearch mapper: message leaves, map leaves and hint
 * shapes that have no multi-valued form, all reached through a repeated ancestor.
 */
class OpenSearchDocumentMapperFanOutTest {

    private final OpenSearchDocumentMapper mapper =
            new OpenSearchDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void messageLeavesUnderAFanOutBecomeAListOfJsonObjects() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf(f.chunkWithMeta("m1"), f.chunkWithMeta("m2"));
        IndexingPlan plan = f.plan(new IndexingPlan.IndexedField(
                "chunks.meta", "chunks_meta", ResolvedFieldHint.of(IndexFieldKind.OBJECT), true));

        assertThat(mapper.map(message, plan))
                .containsEntry("chunks_meta", List.of(Map.of("label", "m1"), Map.of("label", "m2")));
    }

    @Test
    void mapLeavesUnderAFanOutFlattenIntoOneObject() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf(f.chunkWithAttr("k1", "v1"), f.chunkWithAttr("k2", "v2"));
        IndexingPlan plan = f.plan(new IndexingPlan.IndexedField(
                "chunks.attrs", "chunks_attrs",
                ResolvedFieldHint.builder(IndexFieldKind.OBJECT).mapMode(MapMode.FLATTEN).build(),
                true));

        // Entries concatenated by the fan-out are still recognised as one map field.
        assertThat(mapper.map(message, plan))
                .containsEntry("chunks_attrs", Map.of("k1", "v1", "k2", "v2"));
    }

    @Test
    void nullValueSubstituteAppliesWhenNoElementContributes() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf();
        IndexingPlan plan = f.plan(new IndexingPlan.IndexedField(
                "chunks.text", "chunks_text",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).nullValue("N/A").build(), true));

        assertThat(mapper.map(message, plan)).containsEntry("chunks_text", "N/A");
    }

    @Test
    void skipIfMissingFalseEmitsAnExplicitNullForAnEmptyFanOut() throws Exception {
        Fixture f = Fixture.create();
        IndexingPlan plan = f.plan(new IndexingPlan.IndexedField(
                "chunks.text", "chunks_text",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).skipIfMissing(false).build(), true));

        assertThat(mapper.map(f.docOf(), plan)).containsEntry("chunks_text", null);
    }

    @Test
    void rangeHintUnderAFanOutFailsLoudRatherThanEmittingAWrongShape() throws Exception {
        Fixture f = Fixture.create();
        DynamicMessage message = f.docOf(f.chunkWithSpan(1, 5), f.chunkWithSpan(7, 9));
        IndexingPlan plan = f.plan(new IndexingPlan.IndexedField(
                "chunks.span", "chunks_span", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE), true));

        // A bounds message has no flattened form; the document must not be half-built.
        assertThatThrownBy(() -> mapper.map(message, plan))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("bounds message");
    }

    private record Fixture(Descriptor doc, Descriptor chunk, Descriptor inner) {

        static Fixture create() throws Exception {
            FileDescriptor file = file();
            return new Fixture(
                    file.findMessageTypeByName("Doc"),
                    file.findMessageTypeByName("Chunk"),
                    file.findMessageTypeByName("Inner"));
        }

        IndexingPlan plan(IndexingPlan.IndexedField... fields) {
            return new IndexingPlan(doc.getFullName(), List.of(fields));
        }

        DynamicMessage docOf(DynamicMessage... chunks) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(doc)
                    .setField(doc.findFieldByName("doc_id"), "d1");
            for (DynamicMessage element : chunks) {
                builder.addRepeatedField(doc.findFieldByName("chunks"), element);
            }
            return builder.build();
        }

        DynamicMessage chunkWithMeta(String label) {
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("meta"), DynamicMessage.newBuilder(inner)
                            .setField(inner.findFieldByName("label"), label)
                            .build())
                    .build();
        }

        DynamicMessage chunkWithAttr(String key, String value) {
            Descriptor entry = chunk.findNestedTypeByName("AttrsEntry");
            return DynamicMessage.newBuilder(chunk)
                    .addRepeatedField(chunk.findFieldByName("attrs"), DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("key"), key)
                            .setField(entry.findFieldByName("value"), value)
                            .build())
                    .build();
        }

        DynamicMessage chunkWithSpan(int gte, int lte) {
            Descriptor span = chunk.findFieldByName("span").getMessageType();
            return DynamicMessage.newBuilder(chunk)
                    .setField(chunk.findFieldByName("span"), DynamicMessage.newBuilder(span)
                            .setField(span.findFieldByName("gte"), gte)
                            .setField(span.findFieldByName("lte"), lte)
                            .build())
                    .build();
        }

        private static FileDescriptor file() throws Exception {
            String pkg = ".ai.pipestream.test.osfanout";
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("opensearch_fanout.proto")
                    .setPackage("ai.pipestream.test.osfanout")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Inner")
                            .addField(string("label", 1)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Span")
                            .addField(int32("gte", 1))
                            .addField(int32("lte", 2)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Chunk")
                            .addField(string("text", 1))
                            .addField(message("meta", 2, pkg + ".Inner",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(message("attrs", 3, pkg + ".Chunk.AttrsEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(message("span", 4, pkg + ".Span",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addNestedType(DescriptorProto.newBuilder()
                                    .setName("AttrsEntry")
                                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                    .addField(string("key", 1))
                                    .addField(string("value", 2))))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Doc")
                            .addField(string("doc_id", 1))
                            .addField(message("chunks", 2, pkg + ".Chunk",
                                    FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
        }

        private static FieldDescriptorProto.Builder string(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder int32(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder message(
                String name, int number, String typeName, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeName)
                    .setLabel(label);
        }
    }
}
