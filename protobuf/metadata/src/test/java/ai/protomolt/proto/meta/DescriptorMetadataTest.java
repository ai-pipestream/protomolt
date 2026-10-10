package ai.protomolt.proto.meta;

import ai.protomolt.proto.meta.testdata.AnnotatedDoc;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DescriptorMetadataTest {

    @Test
    void readsMessageAndFieldMetadataIntoBag() {
        var bag = DescriptorMetadata.asBag(AnnotatedDoc.getDescriptor());

        assertThat(bag)
                .containsEntry("message.description", "Search document")
                .containsEntry("message.owner", "search-platform")
                .containsEntry("message.sensitivity", "internal")
                .containsEntry("message.labels.domain", "docs")
                .containsEntry("field.doc_id.description", "Stable document id")
                .containsEntry("field.doc_id.display_name", "Document ID")
                .containsEntry("field.doc_id.labels.role", "id")
                .containsEntry("field.title.owner", "content");
    }

    @Test
    void fieldAndMessageHelpers() {
        assertThat(DescriptorMetadata.message(AnnotatedDoc.getDescriptor()))
                .get()
                .extracting(MessageMeta::getOwner)
                .isEqualTo("search-platform");

        var docId = AnnotatedDoc.getDescriptor().findFieldByName("doc_id");
        assertThat(DescriptorMetadata.field(docId))
                .get()
                .extracting(FieldMeta::getSensitivity)
                .isEqualTo("public");
    }

    @Test
    void readsMetadataFromDescriptorsBuiltWithoutTheExtensionsRegistered() throws Exception {
        // Parsing without a registry leaves the meta.v1 options as unknown fields, as a
        // descriptor compiled from source or loaded from bytes elsewhere would carry them.
        var file = AnnotatedDoc.getDescriptor().getFile();
        var bytes = file.toProto().toByteString();
        var unregistered = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                com.google.protobuf.DescriptorProtos.FileDescriptorProto.parseFrom(bytes),
                file.getDependencies().toArray(com.google.protobuf.Descriptors.FileDescriptor[]::new));
        var doc = unregistered.findMessageTypeByName(AnnotatedDoc.getDescriptor().getName());
        var docId = doc.findFieldByName("doc_id");
        assertThat(docId.getOptions().hasExtension(MetadataProto.field)).isFalse();

        assertThat(DescriptorMetadata.field(docId)).get()
                .extracting(FieldMeta::getSensitivity).isEqualTo("public");
        assertThat(DescriptorMetadata.message(doc)).get()
                .extracting(MessageMeta::getOwner).isEqualTo("search-platform");
        assertThat(DescriptorMetadata.asBag(doc)).isEqualTo(DescriptorMetadata.asBag(AnnotatedDoc.getDescriptor()));
    }
}
