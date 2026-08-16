package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The delete-and-unindex envelope: the repository delete step and the
 * door's un-index step, wired from one explicit input, with a schema
 * payload that carries both wire contracts.
 */
class SearchWorkflowsTest {

    @Test
    void theDeleteAndUnindexEnvelopeWiresBothSteps() {
        ObjectNode workflow = SearchWorkflows.deleteAndUnindexWorkflow(
                "inprocess:repo", "inprocess:search", 1_000);

        assertThat(workflow.path("name").asText())
                .isEqualTo(SearchWorkflows.DELETE_AND_UNINDEX_WORKFLOW);
        assertThat(workflow.path("inputType").asText())
                .isEqualTo("ai.pipestream.proto.search.v1.DeleteAndUnindexRequest");

        JsonNode delete = workflow.path("steps").get(0);
        assertThat(delete.path("name").asText()).isEqualTo("delete");
        assertThat(delete.path("target").asText()).isEqualTo("inprocess:repo");
        assertThat(delete.path("method").asText())
                .isEqualTo("ai.pipestream.proto.repo.v1.DocumentService/DeleteDocument");
        assertThat(rules(delete)).containsExactly(
                "by_reference.address = input.address",
                "purge_storage = input.purge_storage");

        JsonNode unindex = workflow.path("steps").get(1);
        assertThat(unindex.path("name").asText()).isEqualTo("unindex");
        assertThat(unindex.path("target").asText()).isEqualTo("inprocess:search");
        assertThat(unindex.path("method").asText())
                .isEqualTo("ai.pipestream.proto.search.v1.SearchIndexService/DeleteDocument");
        assertThat(rules(unindex)).containsExactly(
                "mapping_subject = input.mapping_subject",
                "doc_id = input.address.doc_id");

        assertThat(workflow.path("output").path("type").asText())
                .isEqualTo("ai.pipestream.proto.search.v1.DeleteDocumentResponse");
    }

    @Test
    void theDeleteEnvelopeSchemaCarriesBothContracts() throws Exception {
        FileDescriptorSet schema = FileDescriptorSet.parseFrom(Base64.getDecoder()
                .decode(SearchWorkflows.deleteDescriptorSetBase64()));
        assertThat(schema.getFileList()).extracting(FileDescriptorProto::getName)
                .contains(
                        "ai/pipestream/proto/search/v1/search_service.proto",
                        "ai/pipestream/proto/repo/v1/document_service.proto");
    }

    @Test
    void blankTargetsAndNonPositiveDeadlinesAreRefused() {
        assertThatThrownBy(() ->
                SearchWorkflows.deleteAndUnindexWorkflow(" ", "inprocess:search", 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("repoTarget");
        assertThatThrownBy(() ->
                SearchWorkflows.deleteAndUnindexWorkflow("inprocess:repo", " ", 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("searchTarget");
        assertThatThrownBy(() ->
                SearchWorkflows.deleteAndUnindexWorkflow("inprocess:repo", "inprocess:search", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadlineMs");
    }

    private static List<String> rules(JsonNode step) {
        List<String> rules = new ArrayList<>();
        step.path("rules").forEach(rule -> rules.add(rule.asText()));
        return rules;
    }
}
