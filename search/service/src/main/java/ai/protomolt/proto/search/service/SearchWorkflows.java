package ai.protomolt.proto.search.service;

import ai.protomolt.proto.parse.v1.ParseDocumentRequest;
import ai.protomolt.proto.search.v1.DeleteAndUnindexRequest;
import ai.protomolt.proto.search.v1.ParseAndIndexRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The service's workflows, two checkpointed steps each under the jobs
 * executor. Parse-and-index makes ingestion-to-searchable durable end to
 * end: the coordinator parses the stored document, then the service indexes
 * it under the request's mapping subject — either step's transient failure
 * requeues with backoff, and a completed run means the document answers
 * queries. Delete-and-unindex is its removal-side mirror: the repository
 * deletes the stored document, then the service removes it from the subject's
 * index, so a completed run means the document neither reads back nor
 * answers queries.
 */
public final class SearchWorkflows {

    /** The workflow name registries and request topics know this workflow by. */
    public static final String PARSE_AND_INDEX_WORKFLOW = "parse-and-index";

    /** The removal-side workflow name: repository delete, then un-index. */
    public static final String DELETE_AND_UNINDEX_WORKFLOW = "delete-and-unindex";

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
     * Builds the delete-and-unindex workflow envelope.
     *
     * @param repoTarget the repository DocumentService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param searchTarget the SearchIndexService endpoint, same forms
     * @param deadlineMs per-step deadline in milliseconds; must be positive
     * @return the workflow-definition JSON the jobs submitter accepts
     */
    public static ObjectNode deleteAndUnindexWorkflow(
            String repoTarget, String searchTarget, long deadlineMs) {
        if (repoTarget == null || repoTarget.isBlank()) {
            throw new IllegalArgumentException("repoTarget must not be blank");
        }
        if (searchTarget == null || searchTarget.isBlank()) {
            throw new IllegalArgumentException("searchTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", DELETE_AND_UNINDEX_WORKFLOW);
        workflow.putObject("schema")
                .put("descriptorSetBase64", deleteDescriptorSetBase64());
        workflow.put("inputType", DeleteAndUnindexRequest.getDescriptor().getFullName());
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode delete = steps.addObject();
        delete.put("name", "delete");
        delete.put("target", repoTarget);
        delete.put("method",
                "ai.pipestream.proto.repo.v1.DocumentService/DeleteDocument");
        delete.put("deadlineMs", deadlineMs);
        delete.putArray("rules")
                .add("by_reference.address = input.address")
                .add("purge_storage = input.purge_storage");
        ObjectNode unindex = steps.addObject();
        unindex.put("name", "unindex");
        unindex.put("target", searchTarget);
        unindex.put("method",
                "ai.pipestream.proto.search.v1.SearchIndexService/DeleteDocument");
        unindex.put("deadlineMs", deadlineMs);
        unindex.putArray("rules")
                .add("mapping_subject = input.mapping_subject")
                .add("doc_id = input.address.doc_id");
        ObjectNode output = workflow.putObject("output");
        output.put("type", "ai.pipestream.proto.search.v1.DeleteDocumentResponse");
        output.putArray("rules")
                .add("doc_id = unindex.doc_id")
                .add("chunks_deleted = unindex.chunks_deleted");
        return workflow;
    }

    /**
     * The search and parse contracts and every transitive import as a
     * base64 {@link FileDescriptorSet} — the parse-and-index envelope's
     * schema payload.
     */
    public static String descriptorSetBase64() {
        return descriptorSetBase64(
                ParseAndIndexRequest.getDescriptor().getFile(),
                ParseDocumentRequest.getDescriptor().getFile());
    }

    /**
     * The search and repository contracts and every transitive import as a
     * base64 {@link FileDescriptorSet} — the delete-and-unindex envelope's
     * schema payload.
     */
    public static String deleteDescriptorSetBase64() {
        return descriptorSetBase64(
                DeleteAndUnindexRequest.getDescriptor().getFile(),
                ai.protomolt.proto.repo.v1.DeleteDocumentRequest.getDescriptor().getFile());
    }

    private static String descriptorSetBase64(FileDescriptor... roots) {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        for (FileDescriptor root : roots) {
            collect(root, files);
        }
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
