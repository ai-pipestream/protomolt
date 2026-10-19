package ai.protomolt.proto.search.service;

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
 * The delete-and-unindex envelope: manifest, repository v2 delete and
 * optional purge steps and the service's un-index step, wired from one
 * explicit input, with a schema payload that carries both wire contracts.
 * Both envelopes must also parse through the workflow compiler front-end
 * the jobs submitter uses.
 */
class SearchWorkflowsTest {

    @Test
    void theDeleteAndUnindexEnvelopeWiresBothSteps() {
        ObjectNode workflow = SearchWorkflows.deleteAndUnindexWorkflow(
                "inprocess:repo", "inprocess:search", 1_000);

        assertThat(workflow.path("name").asText())
                .isEqualTo(SearchWorkflows.DELETE_AND_UNINDEX_WORKFLOW);
        assertThat(workflow.path("inputType").asText())
                .isEqualTo("ai.protomolt.proto.search.v1.DeleteAndUnindexRequest");

        JsonNode head = workflow.path("steps").get(0);
        assertThat(head.path("name").asText()).isEqualTo("head");
        assertThat(head.path("target").asText()).isEqualTo("inprocess:repo");
        assertThat(head.path("method").asText())
                .isEqualTo("ai.protomolt.proto.repo.v2.DocumentService/GetManifest");
        assertThat(rules(head)).containsExactly("address = input.address");

        JsonNode delete = workflow.path("steps").get(1);
        assertThat(delete.path("name").asText()).isEqualTo("delete");
        assertThat(delete.path("target").asText()).isEqualTo("inprocess:repo");
        assertThat(delete.path("method").asText())
                .isEqualTo("ai.protomolt.proto.repo.v2.DocumentService/DeleteDocument");
        assertThat(rules(delete)).containsExactly(
                "address = input.address",
                "expected.revision = head.manifest.revision");
        assertThat(celTargets(delete)).containsExactly("operation_id", "reason");

        JsonNode purge = workflow.path("steps").get(2);
        assertThat(purge.path("name").asText()).isEqualTo("purge");
        assertThat(purge.path("method").asText())
                .isEqualTo("ai.protomolt.proto.repo.v2.DocumentService/PurgeDocument");
        assertThat(purge.path("when").asText()).isEqualTo("input.purge_storage");
        assertThat(rules(purge)).containsExactly("address = input.address");
        assertThat(celTargets(purge)).containsExactly("purge_id", "reason");

        JsonNode unindex = workflow.path("steps").get(3);
        assertThat(unindex.path("name").asText()).isEqualTo("unindex");
        assertThat(unindex.path("target").asText()).isEqualTo("inprocess:search");
        assertThat(unindex.path("method").asText())
                .isEqualTo("ai.protomolt.proto.search.v1.SearchIndexService/DeleteDocument");
        assertThat(rules(unindex)).containsExactly(
                "mapping_subject = input.mapping_subject",
                "doc_id = input.address.doc_id");

        assertThat(workflow.path("output").path("type").asText())
                .isEqualTo("ai.protomolt.proto.search.v1.DeleteDocumentResponse");
    }

    @Test
    void theDeleteEnvelopeSchemaCarriesBothContracts() throws Exception {
        FileDescriptorSet schema = FileDescriptorSet.parseFrom(Base64.getDecoder()
                .decode(SearchWorkflows.deleteDescriptorSetBase64()));
        assertThat(schema.getFileList()).extracting(FileDescriptorProto::getName)
                .contains(
                        "ai/protomolt/proto/search/v1/search_service.proto",
                        "ai/protomolt/proto/repo/v2/document_service.proto");
    }

    @Test
    void bothEnvelopesParseThroughTheWorkflowCompiler() throws Exception {
        var context = ai.protomolt.proto.actions.ActionContext.create();
        ai.protomolt.proto.workflow.WorkflowJson.parse(
                SearchWorkflows.deleteAndUnindexWorkflow("inprocess:repo", "inprocess:search", 1_000),
                context);
        ai.protomolt.proto.workflow.WorkflowJson.parse(
                SearchWorkflows.parseAndIndexWorkflow("inprocess:parse", "inprocess:search", 1_000),
                context);
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

    private static List<String> celTargets(JsonNode step) {
        List<String> targets = new ArrayList<>();
        step.path("celRules").forEach(rule -> targets.add(rule.path("target").asText()));
        return targets;
    }
}
