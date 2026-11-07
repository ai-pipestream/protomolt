package ai.protomolt.proto.meta;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases of {@link DescriptorMetadata#materializeJsonNames}: restoring annotated JSON
 * names dropped by a text round-trip, without touching fields that already carry one.
 */
class MaterializeJsonNamesTest {

    private static DescriptorProtos.FieldDescriptorProto field(
            String name, int number, String jsonName, String annotatedJsonName) {
        var options = DescriptorProtos.FieldOptions.newBuilder();
        if (annotatedJsonName != null) {
            options.setExtension(
                    MetadataProto.field, FieldMeta.newBuilder().setJsonName(annotatedJsonName).build());
        }
        var builder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setOptions(options.build());
        if (jsonName != null) {
            builder.setJsonName(jsonName);
        }
        return builder.build();
    }

    private static DescriptorProtos.FileDescriptorSet setOf(DescriptorProtos.FieldDescriptorProto... fields) {
        var message = DescriptorProtos.DescriptorProto.newBuilder().setName("Doc");
        for (var f : fields) {
            message.addField(f);
        }
        var file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("doc.proto")
                .setPackage("test")
                .addMessageType(message.build());
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file.build()).build();
    }

    @Test
    void restoresAnnotatedJsonNameWhenMissing() {
        var out = DescriptorMetadata.materializeJsonNames(
                setOf(field("doc_id", 1, null, "docId")));

        assertThat(out.getFile(0).getMessageType(0).getField(0).getJsonName()).isEqualTo("docId");
    }

    @Test
    void keepsAnExistingJsonName() {
        var out = DescriptorMetadata.materializeJsonNames(
                setOf(field("doc_id", 1, "keptName", "docId")));

        assertThat(out.getFile(0).getMessageType(0).getField(0).getJsonName()).isEqualTo("keptName");
    }

    @Test
    void ignoresFieldsWithoutTheMetadataOption() {
        var out = DescriptorMetadata.materializeJsonNames(
                setOf(field("plain", 1, null, null)));

        assertThat(out.getFile(0).getMessageType(0).getField(0).hasJsonName()).isFalse();
    }

    @Test
    void ignoresAnEmptyAnnotatedJsonName() {
        var out = DescriptorMetadata.materializeJsonNames(
                setOf(field("doc_id", 1, null, "")));

        assertThat(out.getFile(0).getMessageType(0).getField(0).hasJsonName()).isFalse();
    }
}
