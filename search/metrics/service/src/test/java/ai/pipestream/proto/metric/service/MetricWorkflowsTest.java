package ai.pipestream.proto.metric.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The rebuild-rollup envelope: one checkpointed step calling the metric
 * service, identity rules carrying the whole declared rollup, and a schema
 * payload that lets the jobs executor decode the input without this
 * module on its classpath.
 */
class MetricWorkflowsTest {

    private static List<String> rules(JsonNode step) {
        List<String> rules = new ArrayList<>();
        step.path("rules").forEach(rule -> rules.add(rule.asText()));
        return rules;
    }

    @Test
    void theEnvelopeWiresTheOneRebuildStep() {
        ObjectNode workflow = MetricWorkflows.rebuildRollupWorkflow("inprocess:metrics", 60_000);

        assertThat(workflow.path("name").asText())
                .isEqualTo(MetricWorkflows.REBUILD_ROLLUP_WORKFLOW);
        assertThat(workflow.path("inputType").asText())
                .isEqualTo("ai.pipestream.proto.metric.v1.RebuildRollupRequest");

        JsonNode rebuild = workflow.path("steps").get(0);
        assertThat(workflow.path("steps")).hasSize(1);
        assertThat(rebuild.path("name").asText()).isEqualTo("rebuild");
        assertThat(rebuild.path("target").asText()).isEqualTo("inprocess:metrics");
        assertThat(rebuild.path("method").asText())
                .isEqualTo("ai.pipestream.proto.metric.v1.MetricService/RebuildRollup");
        assertThat(rules(rebuild)).containsExactly(
                "mapping_subject = input.mapping_subject",
                "backend = input.backend",
                "measures = input.measures",
                "dimensions = input.dimensions",
                "filters = input.filters",
                "table = input.table");

        assertThat(workflow.path("output").path("type").asText())
                .isEqualTo("ai.pipestream.proto.metric.v1.RebuildRollupResponse");
    }

    @Test
    void theSchemaPayloadCarriesTheMetricContract() throws Exception {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(Base64.getDecoder().decode(
                MetricWorkflows.rebuildRollupWorkflow("inprocess:metrics", 1_000)
                        .path("schema").path("descriptorSetBase64").asText()));
        assertThat(set.getFileList())
                .anySatisfy(file -> assertThat(file.getName())
                        .isEqualTo("ai/pipestream/proto/metric/v1/metric_service.proto"));
    }

    @Test
    void blankTargetsAndNonPositiveDeadlinesAreRefused() {
        assertThatThrownBy(() -> MetricWorkflows.rebuildRollupWorkflow(" ", 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MetricWorkflows.rebuildRollupWorkflow("inprocess:m", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
