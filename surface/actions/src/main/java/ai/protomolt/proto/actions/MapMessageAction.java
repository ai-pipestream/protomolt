package ai.protomolt.proto.actions;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.cel.CelMappingRule;
import ai.protomolt.proto.cel.CelProtoMapper;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;

/** Applies text and/or CEL mapping rules to a JSON message and returns the mapped message. */
final class MapMessageAction implements ProtoAction {

    @Override
    public String name() {
        return "map-message";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Transforms a JSON message with field-mapping rules — text rules like "
                + "'target.path = source.path' (or '-field' to clear) and/or CEL rules "
                + "{filter?, selector?, target, fallback?} where expressions see the current "
                + "message as 'input' — and returns the mapped message as JSON. "
                + "At least one of 'rules' or 'celRules' must be provided.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("MapMessageRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("MapMessageResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(named(input, "type"), "/type");
        // The message is a structure: its shape is the named type, not this contract.
        ObjectNode messageNode = Fields.json(input, "message");
        List<String> textRules = Fields.strings(input, "rules");
        List<CelMappingRule> celRules = celRules(input);
        if (textRules.isEmpty() && celRules.isEmpty()) {
            throw Inputs.invalidInput("At least one of 'rules' or 'celRules' must be provided", "");
        }
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(messageNode.toString(), descriptor);
        } catch (MalformedProtobufJsonException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("pointer", "/message");
            details.put("detail", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new ActionException("invalid-message",
                    "Message is not valid proto3 JSON for " + descriptor.getFullName(), details);
        }
        Message.Builder builder = message.toBuilder();
        ProtoFieldMapperImpl fieldMapper = new ProtoFieldMapperImpl(context.registry());
        try {
            if (!textRules.isEmpty()) {
                fieldMapper.mapInPlace(builder, textRules);
            }
            if (!celRules.isEmpty()) {
                CelEvaluator evaluator = new CelEvaluator(
                        CelEnvironmentFactory.builder().addMessageVar("input", descriptor).build());
                new CelProtoMapper(fieldMapper, evaluator).map(builder, celRules);
            }
        } catch (CelCompilationException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("detail", e.getMessage());
            throw new ActionException("invalid-expression",
                    "CEL mapping expression does not compile: " + e.getMessage(), details);
        } catch (CelEvaluationException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("detail", e.getMessage());
            throw new ActionException("evaluation-failed",
                    "CEL mapping expression failed at runtime: " + e.getMessage(), details);
        } catch (MappingException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("detail", e.getMessage());
            throw new ActionException("mapping-failed",
                    "Mapping rule failed: " + e.getMessage(), details);
        }
        return Reply.of(responseType())
                .set("message", context.transcoder().toJson(builder.build()))
                .build();
    }

    /**
     * The CEL rules a request carries. The message declares each rule's target required, so
     * a rule without one is refused before the verb runs.
     */
    static List<CelMappingRule> celRules(Message input) {
        List<Message> declared = Fields.list(input, "celRules");
        List<CelMappingRule> rules = new ArrayList<>(declared.size());
        for (Message rule : declared) {
            rules.add(new CelMappingRule(
                    Fields.string(rule, "filter"),
                    Fields.string(rule, "selector"),
                    Fields.string(rule, "target"),
                    Fields.strings(rule, "fallback")));
        }
        return rules;
    }

    /** A named type, or null when the caller left the schema's own default to apply. */
    private static String named(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isEmpty() ? null : value;
    }

}
