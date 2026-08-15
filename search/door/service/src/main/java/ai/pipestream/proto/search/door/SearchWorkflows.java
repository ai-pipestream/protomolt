package ai.pipestream.proto.search.door;

import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.search.v1.ParseAndIndexRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The parse-and-index workflow: two checkpointed steps under the jobs
 * executor — the coordinator parses the stored document, then the door
 * indexes it under the request's mapping subject. Submitting it as a
 * workflow run makes ingestion-to-searchable durable end to end: either
 * step's transient failure requeues with backoff, and a completed run
 * means the document answers queries.
 */
public final class SearchWorkflows {

    /** The workflow name registries and request topics know this workflow by. */
    public static final String PARSE_AND_INDEX_WORKFLOW = "parse-and-index";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SearchWorkflows() {
    }

    /**
     * Builds the parse-and-index workflow envelope.
     *
     * @param coordinatorTarget the ParseCoordinatorService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param searchTarget the SearchIndexService endpoint, same forms
     * @param deadlineMs per-step deadline in milliseconds; must be positive
     * @return the workflow-definition JSON the jobs submitter accepts
     */
    public static ObjectNode parseAndIndexWorkflow(
            String coordinatorTarget, String searchTarget, long deadlineMs) {
        if (coordinatorTarget == null || coordinatorTarget.isBlank()) {
            throw new IllegalArgumentException("coordinatorTarget must not be blank");
        }
        if (searchTarget == null || searchTarget.isBlank()) {
            throw new IllegalArgumentException("searchTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", PARSE_AND_INDEX_WORKFLOW);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64());
        workflow.put("inputType", ParseAndIndexRequest.getDescriptor().getFullName());
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode parse = steps.addObject();
        parse.put("name", "parse");
        parse.put("target", coordinatorTarget);
        parse.put("method",
                "ai.pipestream.proto.parse.v1.ParseCoordinatorService/ParseDocument");
        parse.put("deadlineMs", deadlineMs);
        parse.putArray("rules")
                .add("address = input.address");
        ObjectNode index = steps.addObject();
        index.put("name", "index");
        index.put("target", searchTarget);
        index.put("method",
                "ai.pipestream.proto.search.v1.SearchIndexService/IndexDocument");
        index.put("deadlineMs", deadlineMs);
        index.putArray("rules")
                .add("address = input.address")
                .add("mapping_subject = input.mapping_subject");
        ObjectNode output = workflow.putObject("output");
        output.put("type", "ai.pipestream.proto.search.v1.IndexDocumentResponse");
        output.putArray("rules")
                .add("doc_id = index.doc_id")
                .add("chunks_indexed = index.chunks_indexed")
                .add("policy_digest = index.policy_digest");
        return workflow;
    }

    /**
     * The search and parse contracts and every transitive import as a
     * base64 {@link FileDescriptorSet} — the workflow envelope's schema
     * payload.
     */
    public static String descriptorSetBase64() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(ParseAndIndexRequest.getDescriptor().getFile(), files);
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
