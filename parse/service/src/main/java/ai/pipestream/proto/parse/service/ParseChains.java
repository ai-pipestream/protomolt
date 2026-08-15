package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The durable-parse chain: builds the chain-definition envelope that runs
 * {@code ParseCoordinatorService/ParseDocument} as one checkpointed step
 * under the jobs executor. Submitting it as a chain job is what makes a
 * parse durable — the job row survives process restart, a transient
 * coordinator failure requeues with backoff, and the step's response is
 * checkpointed so a resumed job never re-parses work that already landed.
 *
 * <p>The envelope's schema rides as a base64 {@link FileDescriptorSet}
 * covering the parse contract and its transitive imports, derived from the
 * generated descriptors — no runtime proto compilation involved.
 */
public final class ParseChains {

    /** The chain name registries and request topics know this chain by. */
    public static final String PARSE_DOCUMENT_CHAIN = "parse-document";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ParseChains() {
    }

    /**
     * Builds the parse-document chain envelope.
     *
     * @param coordinatorTarget the ParseCoordinatorService endpoint — a
     *        {@code host:port} authority or {@code inprocess:<name>}
     * @param deadlineMs per-parse deadline in milliseconds; must be positive
     * @return the chain-definition JSON the jobs submitter accepts
     */
    public static ObjectNode parseDocumentChain(String coordinatorTarget, long deadlineMs) {
        if (coordinatorTarget == null || coordinatorTarget.isBlank()) {
            throw new IllegalArgumentException("coordinatorTarget must not be blank");
        }
        if (deadlineMs <= 0) {
            throw new IllegalArgumentException("deadlineMs must be positive");
        }
        ObjectNode chain = MAPPER.createObjectNode();
        chain.put("name", PARSE_DOCUMENT_CHAIN);
        chain.putObject("schema").put("descriptorSetBase64", descriptorSetBase64());
        chain.put("inputType", ParseDocumentRequest.getDescriptor().getFullName());
        ArrayNode steps = chain.putArray("steps");
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
        ObjectNode output = chain.putObject("output");
        output.put(
                "type", "ai.pipestream.proto.parse.v1.ParseDocumentResponse");
        output.putArray("rules")
                .add("parser_results = parse.parser_results")
                .add("search_metadata_fold = parse.search_metadata_fold");
        return chain;
    }

    /**
     * The parse contract and every transitive import as a base64
     * {@link FileDescriptorSet} — the chain envelope's schema payload.
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
