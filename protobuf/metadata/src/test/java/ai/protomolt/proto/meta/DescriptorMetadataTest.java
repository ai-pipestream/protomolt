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
}
