package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.metric.RebuildRollupRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The metric service's workflow: rebuild-rollup, one checkpointed step under
 * the jobs executor calling {@code MetricService/RebuildRollup}. This is
 * the platform's answer to pre-aggregations — declared (the workflow and
 * its input live in the registry), durable (the jobs executor requeues a
 * transient failure with backoff), evidenced (the run's output carries the
 * physical plan and the lake snapshot the replace committed), and optional
 * (nothing runs until an operator submits it).
 */
public final class MetricWorkflows {

    /** The workflow name registries and submitters know this workflow by. */
    public static final String REBUILD_ROLLUP_WORKFLOW = "rebuild-rollup";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MetricWorkflows() {
    }

    /**
     * Builds the rebuild-rollup workflow envelope.
     *
     * @param metricsTarget the MetricService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param deadlineMs per-step deadline in milliseconds; must be positive
     * @return the workflow-definition JSON the jobs submitter accepts
     */
    public static ObjectNode rebuildRollupWorkflow(String metricsTarget, long deadlineMs) {
        if (metricsTarget == null || metricsTarget.isBlank()) {
            throw new IllegalArgumentException("metricsTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", REBUILD_ROLLUP_WORKFLOW);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64());
        workflow.put("inputType", RebuildRollupRequest.getDescriptor().getFullName());
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode rebuild = steps.addObject();
        rebuild.put("name", "rebuild");
        rebuild.put("target", metricsTarget);
        rebuild.put("method", "ai.pipestream.proto.metric.v1.MetricService/RebuildRollup");
        rebuild.put("deadlineMs", deadlineMs);
        rebuild.putArray("rules")
                .add("mapping_subject = input.mapping_subject")
                .add("backend = input.backend")
                .add("measures = input.measures")
                .add("dimensions = input.dimensions")
                .add("filters = input.filters")
                .add("table = input.table");
        ObjectNode output = workflow.putObject("output");
        output.put("type", "ai.pipestream.proto.metric.v1.RebuildRollupResponse");
        output.putArray("rules")
                .add("mapping_subject = rebuild.mapping_subject")
                .add("backend = rebuild.backend")
                .add("table = rebuild.table")
                .add("rows_written = rebuild.rows_written")
                .add("snapshot_id = rebuild.snapshot_id")
                .add("physical_plan = rebuild.physical_plan");
        return workflow;
    }

    /**
     * The metric contract and every transitive import as a base64
     * {@link FileDescriptorSet} — the envelope's schema payload.
     */
    public static String descriptorSetBase64() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(RebuildRollupRequest.getDescriptor().getFile(), files);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        for (FileDescriptor file : files.values()) {
            set.addFile(file.toProto());
        }
        return Base64.getEncoder().encodeToString(set.build().toByteArray());
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, files);
        }
        files.putIfAbsent(file.getName(), file);
    }
}
