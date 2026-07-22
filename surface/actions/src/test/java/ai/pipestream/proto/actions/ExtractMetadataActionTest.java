package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtractMetadataActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void extractsMessageAndFieldMetadataAsAFlatBag() throws Exception {
        ObjectNode result = catalog.execute("extract-metadata",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}"));
        assertThat(result).isEqualTo(obj("""
                {"type": "actions.test.Person",
                 "metadata": {
                   "field.name.description": "Full name",
                   "field.name.sensitivity": "pii",
                   "message.description": "A person",
                   "message.owner": "identity-team"
                 }}
                """));
    }

    @Test
    void metadataSurvivesDescriptorSetSerialization() throws Exception {
        ObjectNode input = obj("{\"schema\": {\"descriptorSetBase64\": \"\"}, \"type\": \"actions.test.Person\"}");
        ((ObjectNode) input.get("schema")).put("descriptorSetBase64",
                java.util.Base64.getEncoder().encodeToString(
                        com.google.protobuf.DescriptorProtos.FileDescriptorSet.newBuilder()
                                .addFile(TestFixtures.personFile().toProto())
                                .build().toByteArray()));
        ObjectNode result = catalog.execute("extract-metadata", input);
        assertThat(result.get("metadata").get("message.owner").asText()).isEqualTo("identity-team");
    }

    @Test
    void unannotatedTypeYieldsAnEmptyBag() throws Exception {
        ObjectNode input = obj("{\"schema\": {\"sources\": {}}}");
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        ObjectNode result = catalog.execute("extract-metadata", input);
        assertThat(result).isEqualTo(obj("{\"type\": \"t.Doc\", \"metadata\": {}}"));
    }

    @Test
    void unknownTypeIsUnknownType() {
        assertThatThrownBy(() -> catalog.execute("extract-metadata",
                obj("{\"schema\": {\"type\": \"actions.test.Ghost\"}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unknown-type"));
    }
}
