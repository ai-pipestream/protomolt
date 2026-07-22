package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
    public String description() {
        return "Transforms a JSON message with field-mapping rules — text rules like "
                + "'target.path = source.path' (or '-field' to clear) and/or CEL rules "
                + "{filter?, selector?, target, fallback?} where expressions see the current "
                + "message as 'input' — and returns the mapped message as JSON.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type of the message; required unless the schema already "
                        + "identifies a single message."));
        properties.putObject("message")
                .put("type", "object")
                .put("description", "The message to transform, as canonical proto3 JSON.");
        ObjectNode rules = properties.putObject("rules");
        rules.put("type", "array");
        rules.put("description",
                "Text mapping rules applied in order: 'target = source.path', 'target += source.path', "
                        + "'-field.to.clear'. Paths are protobuf field paths on the message itself.");
        rules.putObject("items").put("type", "string");
        ObjectNode celRules = properties.putObject("celRules");
        celRules.put("type", "array");
        celRules.put("description",
                "CEL mapping rules applied in order after 'rules'; each sees the progressive "
                        + "message as 'input'.");
        ObjectNode celRule = celRules.putObject("items");
        celRule.put("type", "object");
        ObjectNode celRuleProps = celRule.putObject("properties");
        celRuleProps.putObject("filter")
                .put("type", "string")
                .put("description", "Optional boolean CEL gate; the rule is skipped when false.");
        celRuleProps.putObject("selector")
                .put("type", "string")
                .put("description",
                        "Optional CEL value expression; its result is written to 'target'. "
                                + "Literals like \"'text'\" or \"42\" are valid CEL.");
        celRuleProps.putObject("target")
                .put("type", "string")
                .put("description", "Protobuf field path to write.");
        ObjectNode fallback = celRuleProps.putObject("fallback");
        fallback.put("type", "array");
        fallback.put("description", "Text rules applied when no selector is given.");
        fallback.putObject("items").put("type", "string");
        celRule.putArray("required").add("target");
        celRule.put("additionalProperties", false);
        ActionJson.required(schema, "schema", "message");
        ArrayNode variants = schema.putArray("anyOf");
        variants.addObject().putArray("required").add("rules");
        variants.addObject().putArray("required").add("celRules");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        ObjectNode messageNode = Inputs.requireObject(input, "message");
        ArrayNode rulesNode = Inputs.optionalArray(input, "rules");
        ArrayNode celRulesNode = Inputs.optionalArray(input, "celRules");
        List<String> textRules = rulesNode == null
                ? List.of() : Inputs.stringElements(rulesNode, "/rules");
        List<CelMappingRule> celRules = parseCelRules(celRulesNode);
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
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("message", ActionJson.messageToJson(builder.build(), context));
        return output;
    }

    static List<CelMappingRule> parseCelRules(ArrayNode celRulesNode) throws ActionException {
        if (celRulesNode == null) {
            return List.of();
        }
        List<CelMappingRule> rules = new ArrayList<>(celRulesNode.size());
        for (int i = 0; i < celRulesNode.size(); i++) {
            JsonNode node = celRulesNode.get(i);
            String pointer = "/celRules/" + i;
            if (!node.isObject()) {
                throw Inputs.invalidInput("CEL rules must be objects", pointer);
            }
            ObjectNode rule = (ObjectNode) node;
            String target = Inputs.optionalString(rule, "target");
            if (target == null) {
                throw Inputs.invalidInput("CEL rule requires a 'target' field path",
                        pointer + "/target");
            }
            ArrayNode fallbackNode = Inputs.optionalArray(rule, "fallback");
            List<String> fallback = fallbackNode == null
                    ? List.of() : Inputs.stringElements(fallbackNode, pointer + "/fallback");
            rules.add(new CelMappingRule(
                    Inputs.optionalString(rule, "filter"),
                    Inputs.optionalString(rule, "selector"),
                    target,
                    fallback));
        }
        return rules;
    }
}
