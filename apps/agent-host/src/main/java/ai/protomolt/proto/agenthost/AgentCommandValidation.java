package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates a turn's command arguments against the delegation request
 * descriptors and the task's deliverable contract. Operates on parsed
 * argument JSON and descriptors only: nothing here sees credentials,
 * provider processes or session state.
 */
final class AgentCommandValidation {

    /** One validator per request type, so CEL programs are compiled once. */
    private static final Map<String, ProtoValidator> VALIDATORS = new ConcurrentHashMap<>();

    private AgentCommandValidation() {
    }

    /** The host's own acknowledgement, which no delegation verb carries. */
    static void validateAck(ObjectNode arguments) {
        if (arguments.size() != 1 || !arguments.path("reason").isTextual()
                || arguments.path("reason").asText().isBlank()
                || arguments.path("reason").asText().length() > 1_024) {
            throw new ModelReplyException(
                    "host-ack arguments must contain one bounded reason");
        }
    }

    /**
     * Refuses a command its delegation request message does not accept.
     *
     * <p>The arguments are the request message's canonical proto3 JSON, so the message is
     * what says whether they are a request at all: an unknown member is a field the caller
     * did not mean, and the declared rules are the ones the coordinator applies when the
     * call arrives. Running them here costs one parse and turns a mid-batch tool failure
     * into a repair turn that quotes the rule the model broke.
     */
    static void validateRequest(String tool, ObjectNode arguments, int index,
                                Map<String, DeliverableContract> contracts) {
        Descriptor descriptor = AgentCommands.REQUESTS.get(tool);
        String where = "command " + index + " " + tool;
        refuseUnknownMembers(arguments, descriptor, where);
        if (AgentCommands.CANDIDATE.equals(tool)) {
            validateCandidate(arguments, where, contracts);
            return;
        }
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(arguments.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ModelReplyException(where + " is not a " + descriptor.getName()
                    + ": " + e.getMessage());
        }
        Message request = builder.build();
        ValidationResult result = VALIDATORS
                .computeIfAbsent(descriptor.getFullName(),
                        name -> ProtoValidator.forMessageType(descriptor))
                .validate(request);
        if (result.valid()) {
            return;
        }
        throw new ModelReplyException(violations(where, result));
    }

    /**
     * Parses a candidate with the deliverable types the host knows and checks its
     * deliverable against the task's contract with the coordinator's own gate, so a
     * deliverable the coordinator would refuse costs a repair turn here instead of an
     * attempt there. A result for a task with no known contract, or a missing result for a
     * task that has one, is refused the same way the reducer refuses it.
     */
    private static void validateCandidate(ObjectNode arguments, String where,
                                          Map<String, DeliverableContract> contracts) {
        SubmitCandidateRequest.Builder builder = SubmitCandidateRequest.newBuilder();
        try {
            JsonFormat.parser().usingTypeRegistry(deliverableTypes(
                            contracts.get(arguments.path("taskId").asText())))
                    .merge(arguments.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            String reason = e.getMessage() == null ? "" : e.getMessage();
            if (reason.contains("Cannot resolve type")) {
                throw new ModelReplyException(where + ": result names a type for which no"
                        + " deliverable contract is known (" + reason + ")");
            }
            throw new ModelReplyException(where + " is not a SubmitCandidateRequest: "
                    + reason);
        }
        SubmitCandidateRequest request = builder.build();
        ValidationResult result = VALIDATORS
                .computeIfAbsent(request.getDescriptorForType().getFullName(),
                        name -> ProtoValidator.forMessageType(
                                request.getDescriptorForType()))
                .validate(request);
        if (!result.valid()) {
            throw new ModelReplyException(violations(where, result));
        }
        DeliverableContract contract = contracts.get(request.getTaskId());
        boolean carried = request.getCandidate().hasResult();
        if (contract == null) {
            if (carried) {
                throw new ModelReplyException(where + ": the candidate carries a"
                        + " deliverable but no deliverable contract is known for task "
                        + request.getTaskId());
            }
            return;
        }
        if (!carried) {
            throw new ModelReplyException(where + ": the task's deliverable contract requires"
                    + " a result of type " + contract.getTypeName());
        }
        Any deliverable = request.getCandidate().getResult();
        List<String> problems = DeliverableContracts.check(contract, deliverable);
        if (!problems.isEmpty()) {
            throw new ModelReplyException(where + ": " + String.join("; ", problems));
        }
    }

    /** Parse only against the addressed task's offer, never another task's same-named type. */
    private static TypeRegistry deliverableTypes(DeliverableContract contract) {
        if (contract == null) return TypeRegistry.getEmptyTypeRegistry();
        try {
            return DeliverableContracts.typeRegistry(contract);
        } catch (IllegalArgumentException unlinkable) {
            return TypeRegistry.getEmptyTypeRegistry();
        }
    }

    private static String violations(String where, ValidationResult result) {
        StringBuilder out = new StringBuilder(where).append(':');
        for (ValidationResult.Violation violation : result.violations()) {
            out.append(' ').append(jsonPath(violation.path())).append(' ')
                    .append(violation.message()).append(';');
        }
        out.setLength(out.length() - 1);
        return out.toString();
    }

    /**
     * Refuses a member no field of the message declares, naming it.
     *
     * <p>The parser refuses unknown members too, but names them by the proto type they were
     * not found in; the model reads the tool name and the member it wrote.
     */
    private static void refuseUnknownMembers(JsonNode node, Descriptor descriptor,
                                             String where) {
        if (!node.isObject() || descriptor.getFullName().startsWith("google.protobuf.")) {
            return;
        }
        for (Map.Entry<String, JsonNode> member : node.properties()) {
            FieldDescriptor field = member(descriptor, member.getKey());
            if (field == null) {
                throw new ModelReplyException(where + " contains unknown field '"
                        + member.getKey() + "'");
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || field.isMapField()) {
                continue;
            }
            JsonNode value = member.getValue();
            if (field.isRepeated() && value.isArray()) {
                for (JsonNode element : value) {
                    refuseUnknownMembers(element, field.getMessageType(), where);
                }
            } else {
                refuseUnknownMembers(value, field.getMessageType(), where);
            }
        }
    }

    /** The field a member names, by its JSON spelling first and its declared name second. */
    private static FieldDescriptor member(Descriptor descriptor, String name) {
        for (FieldDescriptor field : descriptor.getFields()) {
            if (field.getJsonName().equals(name)) {
                return field;
            }
        }
        return descriptor.findFieldByName(name);
    }

    /** A validator path with each field named as the model wrote it in the arguments. */
    static String jsonPath(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : segments(path)) {
            if (out.length() > 0) {
                out.append('.');
            }
            int bracket = segment.indexOf('[');
            String name = bracket < 0 ? segment : segment.substring(0, bracket);
            boolean capitalize = false;
            for (int i = 0; i < name.length(); i++) {
                char character = name.charAt(i);
                if (character == '_') {
                    capitalize = true;
                } else if (capitalize) {
                    out.append(Character.toUpperCase(character));
                    capitalize = false;
                } else {
                    out.append(character);
                }
            }
            if (bracket >= 0) {
                out.append(segment.substring(bracket));
            }
        }
        return out.toString();
    }

    /**
     * The path's dot-separated segments without regex, matching
     * {@code split("\\.")} where it matters: trailing empty segments drop,
     * leading and inner empty segments stay (they contribute nothing to the
     * rendered path either way).
     */
    private static List<String> segments(String path) {
        List<String> parts = new ArrayList<>();
        int from = 0;
        for (int dot = path.indexOf('.'); dot >= 0; dot = path.indexOf('.', from)) {
            parts.add(path.substring(from, dot));
            from = dot + 1;
        }
        parts.add(path.substring(from));
        while (parts.size() > 1 && parts.get(parts.size() - 1).isEmpty()) {
            parts.remove(parts.size() - 1);
        }
        return parts;
    }
}
