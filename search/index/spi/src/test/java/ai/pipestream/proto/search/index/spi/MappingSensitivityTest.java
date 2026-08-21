package ai.pipestream.proto.search.index.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.meta.FieldMeta;
import ai.pipestream.proto.meta.MetadataProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

/**
 * The mapping carries each field's declared {@code meta.v1} sensitivity, which is what
 * lets consumers past the descriptor refuse restricted content without re-resolving the
 * path. A field declaring none reads as the empty string, never null.
 */
class MappingSensitivityTest {

    private static Descriptor recordDescriptor() throws Exception {
        FieldOptions classified = FieldOptions.newBuilder()
                .setExtension(MetadataProto.field,
                        FieldMeta.newBuilder().setSensitivity("pii").build())
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("sensitivity_test.proto")
                .setSyntax("proto3")
                .setPackage("test")
                .addDependency(MetadataProto.getDescriptor().getFullName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Record")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("id").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("notes").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(classified)))
                .build();
        FileDescriptor descriptor = FileDescriptor.buildFrom(file,
                new FileDescriptor[] {MetadataProto.getDescriptor()});
        return descriptor.findMessageTypeByName("Record");
    }

    @Test
    void aDeclaredSensitivityReachesTheMappedField() throws Exception {
        IndexMapping mapping = IndexMappingFactory
                .defaults(new CatalogIndexingHintSource())
                .create(recordDescriptor());

        IndexMapping.IndexedField notes = mapping.find("notes").orElseThrow();
        assertThat(notes.sensitivity()).isEqualTo("pii");
        assertThat(notes.classified()).isTrue();

        IndexMapping.IndexedField id = mapping.find("id").orElseThrow();
        assertThat(id.sensitivity()).isEmpty();
        assertThat(id.classified()).isFalse();
    }

    @Test
    void aHandBuiltFieldDefaultsToUnclassified() {
        IndexMapping.IndexedField field = new IndexMapping.IndexedField(
                "title", "title", ResolvedFieldHint.of(IndexFieldKind.TEXT));
        assertThat(field.sensitivity()).isEmpty();
        assertThat(field.classified()).isFalse();

        IndexMapping.IndexedField nulled = new IndexMapping.IndexedField(
                "title", "title", ResolvedFieldHint.of(IndexFieldKind.TEXT), false, null);
        assertThat(nulled.sensitivity()).isEmpty();
    }
}
