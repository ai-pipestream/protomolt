package ai.protomolt.proto.parse.service;

import ai.protomolt.proto.parse.v1.ParseDocumentRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The durable-parse workflow: builds the workflow-definition envelope that runs
 * {@code ParseCoordinatorService/ParseDocument} as one checkpointed step
 * under the jobs executor. Submitting it as a workflow run is what makes a
 * parse durable — the job row survives process restart, a transient
 * coordinator failure requeues with backoff, and the step's response is
 * checkpointed so a resumed job never re-parses work that already landed.
 *
 * <p>The envelope's schema rides as a base64 {@link FileDescriptorSet}
 * covering the parse contract and its transitive imports, derived from the
 * generated descriptors — no runtime proto compilation involved.
 */
public final class ParseWorkflows {

    /** The workflow name registries and request topics know this workflow by. */
    public static final String PARSE_DOCUMENT_WORKFLOW = "parse-document";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ParseWorkflows() {
    }

    /**
     * Builds the parse-document workflow envelope.
     *
     * @param coordinatorTarget the ParseCoordinatorService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param deadlineMs per-parse deadline in milliseconds; must be positive
     * @return the workflow-definition JSON the jobs submitter accepts
     */
    public static ObjectNode parseDocumentWorkflow(String coordinatorTarget, long deadlineMs) {
        if (coordinatorTarget == null || coordinatorTarget.isBlank()) {
            throw new IllegalArgumentException("coordinatorTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", PARSE_DOCUMENT_WORKFLOW);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64());
        workflow.put("inputType", ParseDocumentRequest.getDescriptor().getFullName());
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode parse = steps.addObject();
        parse.put("name", "parse");
        parse.put("target", coordinatorTarget);
        parse.put(
                "method",
                "ai.pipestream.proto.parse.v1.ParseCoordinatorService/ParseDocument");
        parse.put("deadlineMs", deadlineMs);
        parse.putArray("rules")
                .add("address = input.address")
                .add("parser_override = input.parser_override");
        ObjectNode output = workflow.putObject("output");
        output.put(
                "type", "ai.pipestream.proto.parse.v1.ParseDocumentResponse");
        output.putArray("rules")
                .add("parser_results = parse.parser_results")
                .add("search_metadata_fold = parse.search_metadata_fold");
        return workflow;
    }

    /**
     * The parse contract and every transitive import as a base64
     * {@link FileDescriptorSet} — the workflow envelope's schema payload.
     */
    public static String descriptorSetBase64() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(ParseDocumentRequest.getDescriptor().getFile(), files);
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
