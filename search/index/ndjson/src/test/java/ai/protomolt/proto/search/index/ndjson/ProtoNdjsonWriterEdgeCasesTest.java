package ai.protomolt.proto.search.index.ndjson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

class ProtoNdjsonWriterEdgeCasesTest {

    private final ProtoNdjsonWriter writer = new ProtoNdjsonWriter();

    @Test
    void bulkCreateRejectsNonObjectSource() {
        Timestamp ts = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> writer.writeBulkCreate(out, "docs", "id-1", ts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object")
                .hasMessageContaining("google.protobuf.Timestamp");
        assertThat(out).isEmpty();
    }

    @Test
    void blankIdsAreOmittedLikeMissingIds() {
        Struct doc = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .build();
        StringBuilder out = new StringBuilder();
        writer.writeBulkIndex(out, "docs", "   ", doc);
        assertThat(out.toString().lines().findFirst().orElseThrow())
                .isEqualTo("{\"index\":{\"_index\":\"docs\"}}");

        assertThat(writer.bulkIndexAction("docs", "")).isEqualTo("{\"index\":{\"_index\":\"docs\"}}");
    }

    @Test
    void bulkDeleteRejectsANullIdWithoutWriting() {
        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> writer.writeBulkDelete(out, "docs", null))
                .isInstanceOf(NullPointerException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void bulkWritesRejectNullsWithoutWriting() {
        Struct doc = Struct.newBuilder().build();
        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> writer.writeBulkIndex(null, "docs", "id-1", doc))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> writer.writeBulkIndex(out, null, "id-1", doc))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> writer.writeBulkIndex(out, "docs", "id-1", null))
                .isInstanceOf(NullPointerException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void toJsonLineRejectsNull() {
        assertThatThrownBy(() -> writer.toJsonLine(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsNullOptions() {
        assertThatThrownBy(() -> new ProtoNdjsonWriter(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("options");
    }

    @Test
    void descriptorRegistryIsEmptyByDefaultAndPresentWhenSupplied() {
        assertThat(writer.descriptorRegistry()).isEmpty();
        ProtoNdjsonWriter withRegistry =
                new ProtoNdjsonWriter(NdjsonOptions.defaults(), new DescriptorRegistry());
        assertThat(withRegistry.descriptorRegistry()).isPresent();
    }

    @Test
    void includingDefaultValueFieldsPrintsUnsetScalars() throws Exception {
        var descriptor = SampleDoc.descriptor();
        var message = com.google.protobuf.DynamicMessage.newBuilder(descriptor).build();
        ProtoNdjsonWriter withDefaults = new ProtoNdjsonWriter(
                NdjsonOptions.builder().includingDefaultValueFields(true).build());
        assertThat(withDefaults.toJsonLine(message))
                .contains("\"page_count\":0")
                .contains("\"display_name\":\"\"");
        assertThat(writer.toJsonLine(message)).isEqualTo("{}");
    }

    @Test
    void writeLinesWithNoMessagesWritesNothing() {
        StringBuilder out = new StringBuilder();
        writer.writeLines(out, java.util.List.of());
        assertThat(out).isEmpty();
    }

    @Test
    void writerOverloadWritesOneLine() throws Exception {
        var descriptor = SampleDoc.descriptor();
        var message = com.google.protobuf.DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("display_name"), "Ada")
                .build();
        StringWriter out = new StringWriter();
        writer.writeLine(out, message);
        assertThat(out.toString()).isEqualTo("{\"display_name\":\"Ada\"}\n");
    }

    /** Minimal local message so the default-fields test does not depend on test resources. */
    private static final class SampleDoc {
        static com.google.protobuf.Descriptors.Descriptor descriptor() throws Exception {
            var file = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                    com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                            .setName("sample_edge.proto")
                            .setPackage("ai.protomolt.test.edge")
                            .setSyntax("proto3")
                            .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                                    .setName("SampleDoc")
                                    .addField(field("display_name", 1,
                                            com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type
                                                    .TYPE_STRING))
                                    .addField(field("page_count", 2,
                                            com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type
                                                    .TYPE_INT32)))
                            .build(),
                    new com.google.protobuf.Descriptors.FileDescriptor[0]);
            return file.findMessageTypeByName("SampleDoc");
        }

        private static com.google.protobuf.DescriptorProtos.FieldDescriptorProto field(
                String name, int number,
                com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type type) {
            return com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(type)
                    .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label
                            .LABEL_OPTIONAL)
                    .build();
        }
    }
}
