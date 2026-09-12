package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowVerifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bridge-entry envelope: classify, then bridge, both against the
 * archive. The envelope is parsed and verified rather than string-matched,
 * so a rule naming a field the contract does not have fails here instead of
 * at 2am inside a job.
 */
class ArchiveWorkflowsTest {

    private static final String TARGET = "inprocess:repo-1";

    @Test
    void theEnvelopeClassifiesBeforeItBridges() {
        ObjectNode workflow = ArchiveWorkflows.bridgeEntryWorkflow(TARGET, 60_000);

        assertThat(workflow.path("name").asText())
                .isEqualTo(ArchiveWorkflows.BRIDGE_ENTRY_WORKFLOW);
        assertThat(workflow.path("inputType").asText())
                .isEqualTo("ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest");
        assertThat(workflow.path("steps")).hasSize(2);

        JsonNode classify = workflow.path("steps").get(0);
        assertThat(classify.path("name").asText()).isEqualTo("classify");
        assertThat(classify.path("method").asText()).isEqualTo(
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/ClassifyEntry");
        assertThat(classify.path("target").asText()).isEqualTo(TARGET);
        // No declaration is carried: a standing claim is re-resolved against
        // the bytes, and an unclaimed asset is identified from them.
        assertThat(rules(classify)).containsExactly(
                "address = input.address",
                "classified_by = input.bridged_by");

        JsonNode bridge = workflow.path("steps").get(1);
        assertThat(bridge.path("name").asText()).isEqualTo("bridge");
        assertThat(bridge.path("method").asText()).isEqualTo(
                "ai.protomolt.proto.repo.archive.v1.ArchiveService/BridgeEntry");
        assertThat(rules(bridge)).containsExactly(
                "address = input.address",
                "bridges = input.bridges",
                "bridged_by = input.bridged_by");

        assertThat(workflow.path("output").path("type").asText())
                .isEqualTo("ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse");
    }

    @Test
    void theEnvelopeParsesAndVerifiesAgainstTheArchiveContract() throws Exception {
        CompiledWorkflow parsed = WorkflowJson.parse(
                ArchiveWorkflows.bridgeEntryWorkflow(TARGET, 60_000), ActionContext.create());

        assertThat(parsed.name()).isEqualTo(ArchiveWorkflows.BRIDGE_ENTRY_WORKFLOW);
        assertThat(parsed.steps()).extracting(CompiledWorkflow.Step::name)
                .containsExactly("classify", "bridge");
        // Every rule resolves against the real descriptors: an envelope that
        // named a field the archive does not have would fail here.
        assertThat(new WorkflowVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void theSchemaPayloadCarriesTheArchiveContractAndItsImports() throws Exception {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(Base64.getDecoder().decode(
                ArchiveWorkflows.descriptorSetBase64()));

        assertThat(set.getFileList()).extracting(file -> file.getName())
                .contains("ai/protomolt/proto/repo/archive/v1/archive_service.proto",
                        "ai/protomolt/proto/asset/v1/classification.proto",
                        "ai/protomolt/proto/asset/v1/bridge.proto");
    }

    @Test
    void anEnvelopeWithoutATargetOrADeadlineIsRefused() {
        assertThatThrownBy(() -> ArchiveWorkflows.bridgeEntryWorkflow(" ", 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("archiveTarget");
        assertThatThrownBy(() -> ArchiveWorkflows.bridgeEntryWorkflow(TARGET, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadlineMs");
    }

    private static List<String> rules(JsonNode step) {
        List<String> rules = new ArrayList<>();
        step.path("rules").forEach(rule -> rules.add(rule.asText()));
        return rules;
    }
}
