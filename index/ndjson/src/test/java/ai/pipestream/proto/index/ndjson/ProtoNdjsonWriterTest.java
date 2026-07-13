package ai.pipestream.proto.index.ndjson;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoNdjsonWriterTest {

    private final ProtoNdjsonWriter writer = new ProtoNdjsonWriter();

    @Test
    void encodesStructAsDocumentShapedJson() {
        // JsonFormat emits Struct as a plain JSON object (ideal for OpenSearch).
        Struct message = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .build();

        String line = writer.toJsonLine(message);
        assertThat(line).doesNotContain("\n");
        assertThat(line).isEqualTo("{\"title\":\"Hello\"}");
    }

    @Test
    void preservesProtoFieldNamesForCustomMessages() throws Exception {
        Descriptor descriptor = sampleDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("display_name"), "Ada")
                .setField(descriptor.findFieldByName("page_count"), 3)
                .build();

        assertThat(writer.toJsonLine(message))
                .contains("\"display_name\":\"Ada\"")
                .contains("\"page_count\":3");

        ProtoNdjsonWriter camel = new ProtoNdjsonWriter(
                NdjsonOptions.builder().preservingProtoFieldNames(false).omitWhitespace(true).build());
        assertThat(camel.toJsonLine(message))
                .contains("\"displayName\":\"Ada\"")
                .contains("\"pageCount\":3");
    }

    @Test
    void writesNdjsonLines() {
        Struct a = Struct.newBuilder()
                .putFields("n", Value.newBuilder().setNumberValue(1).build())
                .build();
        Struct b = Struct.newBuilder()
                .putFields("n", Value.newBuilder().setNumberValue(2).build())
                .build();

        StringBuilder out = new StringBuilder();
        writer.writeLines(out, java.util.List.of(a, b));

        assertThat(out.toString()).isEqualTo("{\"n\":1.0}\n{\"n\":2.0}\n");
    }

    @Test
    void writesOpenSearchBulkIndexPair() {
        Struct doc = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .build();
        StringBuilder out = new StringBuilder();
        writer.writeBulkIndex(out, "docs", "id-1", doc);

        String[] lines = out.toString().split("\n", -1);
        assertThat(lines[0]).isEqualTo("{\"index\":{\"_index\":\"docs\",\"_id\":\"id-1\"}}");
        assertThat(lines[1]).isEqualTo("{\"title\":\"Hello\"}");
    }

    @Test
    void bulkIndexRejectsNonObjectSource() {
        // Timestamp becomes an RFC3339 JSON string via JsonFormat — invalid as a bulk _source.
        Timestamp ts = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> writer.writeBulkIndex(out, "docs", "id-1", ts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object")
                .hasMessageContaining("google.protobuf.Timestamp");
        assertThat(out).isEmpty();
    }

    @Test
    void rejectsPrettyPrintedOptionsForLineOrientedOutput() {
        // omitWhitespace(false) would emit multi-line JSON: structurally invalid NDJSON/bulk.
        NdjsonOptions pretty = NdjsonOptions.builder().omitWhitespace(false).build();
        assertThatThrownBy(() -> new ProtoNdjsonWriter(pretty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("omitWhitespace");
    }

    @Test
    void bulkPairIsAppendedAtomically() {
        Struct doc = Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Hello").build())
                .build();
        // Appendable that fails every write: a failure must never leave a dangling action line.
        class FailingAppendable implements Appendable {
            final StringBuilder written = new StringBuilder();
            int calls;

            @Override
            public Appendable append(CharSequence csq) throws java.io.IOException {
                calls++;
                throw new java.io.IOException("boom");
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) throws java.io.IOException {
                return append(csq.subSequence(start, end));
            }

            @Override
            public Appendable append(char c) throws java.io.IOException {
                return append(String.valueOf(c));
            }
        }
        FailingAppendable failing = new FailingAppendable();
        assertThatThrownBy(() -> writer.writeBulkIndex(failing, "docs", "id-1", doc))
                .isInstanceOf(java.io.UncheckedIOException.class);
        // one atomic append for the action+source pair, nothing written on failure
        assertThat(failing.calls).isEqualTo(1);
        assertThat(failing.written).isEmpty();

        // and on success both lines arrive in a single append
        class RecordingAppendable implements Appendable {
            final java.util.List<String> appends = new java.util.ArrayList<>();

            @Override
            public Appendable append(CharSequence csq) {
                appends.add(csq.toString());
                return this;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) {
                return append(csq.subSequence(start, end));
            }

            @Override
            public Appendable append(char c) {
                return append(String.valueOf(c));
            }
        }
        RecordingAppendable recording = new RecordingAppendable();
        writer.writeBulkCreate(recording, "docs", "id-1", doc);
        assertThat(recording.appends).hasSize(1);
        assertThat(recording.appends.getFirst())
                .isEqualTo("{\"create\":{\"_index\":\"docs\",\"_id\":\"id-1\"}}\n{\"title\":\"Hello\"}\n");
    }

    @Test
    void bulkDeleteRejectsBlankId() {
        StringBuilder out = new StringBuilder();
        assertThatThrownBy(() -> writer.writeBulkDelete(out, "docs", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(out).isEmpty();
    }

    @Test
    void bulkIndexWithoutIdOmitsIdField() {
        Struct message = Struct.newBuilder().build();
        StringBuilder out = new StringBuilder();
        writer.writeBulkIndex(out, "docs", null, message);
        assertThat(out.toString().lines().findFirst().orElseThrow())
                .isEqualTo("{\"index\":{\"_index\":\"docs\"}}");
    }

    private static Descriptor sampleDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("sample.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("SampleDoc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("display_name")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("page_count")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("SampleDoc");
    }
}
