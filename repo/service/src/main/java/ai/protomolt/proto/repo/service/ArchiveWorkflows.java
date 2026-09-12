package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The archive's workflow: {@code bridge-entry}, two checkpointed steps under
 * the jobs executor — classify the asset, then run the bridges its
 * classification makes applicable.
 *
 * <p>Bridging an asset synchronously is right for a member listing and
 * wrong for a page of OCR, which is why the same two calls also exist as a
 * durable run: the jobs executor checkpoints the classification before the
 * bridges start, requeues a transient failure with backoff, and keeps the
 * outcomes as the run's evidence. Nothing new executes here — the workflow
 * is the two RPCs the archive already serves, in the order they have to
 * happen.
 *
 * <p>Classification comes first on purpose. Bridging refuses an asset whose
 * classification names no single format, so a run that skipped straight to
 * the bridges would fail on exactly the assets a bridging job exists to
 * work through.
 */
public final class ArchiveWorkflows {

    /** The workflow name registries and submitters know this workflow by. */
    public static final String BRIDGE_ENTRY_WORKFLOW = "bridge-entry";

    private static final String SERVICE = "ai.protomolt.proto.repo.archive.v1.ArchiveService";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ArchiveWorkflows() {
    }

    /**
     * Builds the bridge-entry workflow envelope.
     *
     * @param archiveTarget the ArchiveService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param deadlineMs per-step deadline in milliseconds; must be positive
     * @return the workflow-definition JSON the jobs submitter accepts
     */
    public static ObjectNode bridgeEntryWorkflow(String archiveTarget, long deadlineMs) {
        if (archiveTarget == null || archiveTarget.isBlank()) {
            throw new IllegalArgumentException("archiveTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", BRIDGE_ENTRY_WORKFLOW);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64());
        workflow.put("inputType", BridgeEntryRequest.getDescriptor().getFullName());
        ArrayNode steps = workflow.putArray("steps");

        ObjectNode classify = steps.addObject();
        classify.put("name", "classify");
        classify.put("target", archiveTarget);
        classify.put("method", SERVICE + "/ClassifyEntry");
        classify.put("deadlineMs", deadlineMs);
        // No declaration: a standing claim is re-resolved against the bytes,
        // and an asset with no claim is identified from them.
        classify.putArray("rules")
                .add("address = input.address")
                .add("classified_by = input.bridged_by");

        ObjectNode bridge = steps.addObject();
        bridge.put("name", "bridge");
        bridge.put("target", archiveTarget);
        bridge.put("method", SERVICE + "/BridgeEntry");
        bridge.put("deadlineMs", deadlineMs);
        bridge.putArray("rules")
                .add("address = input.address")
                .add("bridges = input.bridges")
                .add("bridged_by = input.bridged_by");

        ObjectNode output = workflow.putObject("output");
        output.put("type", "ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse");
        output.putArray("rules")
                .add("version = bridge.version")
                .add("outcomes = bridge.outcomes");
        return workflow;
    }

    /**
     * The archive contract and every transitive import as a base64
     * {@link FileDescriptorSet} — the envelope's schema payload, so the jobs
     * executor decodes the input without this module on its classpath.
     *
     * @return the descriptor set, base64-encoded
     */
    public static String descriptorSetBase64() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(BridgeEntryRequest.getDescriptor().getFile(), files);
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
