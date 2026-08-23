package ai.pipestream.proto.registry.service;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Reads action envelopes into the registry's administrative request messages and holds them to
 * the rules those messages declare.
 *
 * <p>Requests arriving over gRPC pass a validating interceptor before reaching a handler.
 * Requests arriving as catalog verbs do not, so without this the same request would be refused
 * on one surface and accepted on the other.
 */
final class RegistryRequests {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private RegistryRequests() {
    }

    /**
     * Parses and validates one envelope, returning the built message.
     *
     * @param input the action envelope, which is the message's canonical proto3 JSON form
     * @param builder a builder for the message the verb accepts
     * @param verb the verb name, used to name the contract in a refusal
     * @return the validated message
     * @throws ActionException when the envelope does not parse, or breaks a declared rule
     */
    static <B extends Message.Builder> Message validate(ObjectNode input, B builder, String verb)
            throws ActionException {
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + builder.getDescriptorForType().getName()
                            + ": " + e.getMessage());
        }
        Message built = builder.build();
        ValidationResult result = VALIDATOR.validate(built);
        if (!result.valid()) {
            ObjectNode details = new ObjectMapper().createObjectNode();
            ArrayNode violations = details.putArray("violations");
            StringBuilder prose = new StringBuilder();
            for (ValidationResult.Violation violation : result.violations()) {
                ObjectNode node = violations.addObject();
                node.put("field", violation.path());
                node.put("ruleId", violation.ruleId());
                node.put("message", violation.message());
                if (prose.length() > 0) {
                    prose.append("; ");
                }
                prose.append(violation.path()).append(' ').append(violation.message());
            }
            throw new ActionException("invalid-input",
                    verb + " does not satisfy its contract: " + prose, details);
        }
        return built;
    }
}
