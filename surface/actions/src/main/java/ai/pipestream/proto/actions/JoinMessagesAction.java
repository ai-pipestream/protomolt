package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.shapes.MessageJoiner;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Joins named source messages into one output message — an authored target type populated by
 * scoped mapping rules, or a shape synthesized on the fly (envelope, projection, union)
 * joined by its implied rules.
 */
final class JoinMessagesAction implements ProtoAction {

    @Override
    public String name() {
        return "join-messages";
    }

    @Override
    public String description() {
        return "Joins named source messages into one output. Rules are the map-message "
                + "surface with scoped source paths ('total = order.qty', CEL rules see each "
                + "source as a variable); the output shape is an authored 'target' type or a "
                + "'shape' spec synthesized on the fly (whose implied rules make a projection "
                + "or envelope joinable with no rules at all).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        ObjectNode sources = properties.putObject("sources");
        sources.put("type", "array");
        sources.put("description", "The named source messages.");
        ObjectNode source = sources.putObject("items");
        source.put("type", "object");
        ObjectNode sourceProperties = source.putObject("properties");
        sourceProperties.putObject("name")
                .put("type", "string")
                .put("description", "Scope name rules and CEL expressions reference.");
        sourceProperties.set("schema", ActionJson.schemaSourceSchema());
        sourceProperties.set("type", ActionJson.typeProperty(
                "Fully qualified message type; required unless the schema identifies one."));
        sourceProperties.putObject("message")
                .put("type", "object")
                .put("description", "The source message, as canonical proto3 JSON.");
        source.putArray("required").add("name").add("schema").add("message");
        ObjectNode target = properties.putObject("target");
        target.put("type", "object");
        target.put("description", "Authored output type: a schema plus type name. Give "
                + "either 'target' or 'shape'.");
        ObjectNode targetProperties = target.putObject("properties");
        targetProperties.set("schema", ActionJson.schemaSourceSchema());
        targetProperties.set("type", ActionJson.typeProperty(
                "Fully qualified output message type."));
        ObjectNode shape = properties.putObject("shape");
        shape.put("type", "object");
        shape.put("description", "Synthesized output shape: {mode: envelope|projection|"
                + "union, name, fields?} as in synthesize-shape. Give either 'target' or "
                + "'shape'.");
        ObjectNode rules = properties.putObject("rules");
        rules.put("type", "array");
        rules.put("description", "Scoped text rules applied in order: 'target.path = "
                + "source.path', 'target.path += source.path', '-target.to.clear'; a bare "
                + "source name copies the whole message.");
        rules.putObject("items").put("type", "string");
        ObjectNode celRules = properties.putObject("celRules");
        celRules.put("type", "array");
        celRules.put("description", "CEL rules applied after 'rules'; expressions see each "
                + "source by name plus 'target', the progressive output.");
        celRules.putObject("items").put("type", "object");
        ActionJson.required(schema, "sources");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        List<ShapeSynthesizer.NamedType> named =
                SynthesizeShapeAction.namedSources(input, context);
        MessageScope scope = buildScope(input, named, context);
        ObjectNode targetNode = Inputs.optionalObject(input, "target");
        ObjectNode shapeNode = Inputs.optionalObject(input, "shape");
        if ((targetNode == null) == (shapeNode == null)) {
            throw Inputs.invalidInput("Give exactly one of 'target' or 'shape'", "");
        }
        ArrayNode rulesNode = Inputs.optionalArray(input, "rules");
        List<String> rules = rulesNode == null
                ? List.of() : Inputs.stringElements(rulesNode, "/rules");
        List<CelMappingRule> celRules =
                MapMessageAction.parseCelRules(Inputs.optionalArray(input, "celRules"));

        MessageJoiner joiner = new MessageJoiner();
        ObjectNode output = context.objectMapper().createObjectNode();
        DynamicMessage joined;
        try {
            if (shapeNode != null) {
                ShapeSynthesizer.SynthesizedShape shape = synthesize(shapeNode, named, context);
                joined = joiner.join(shape, scope, rules, celRules);
                output.put("descriptorSetBase64", Base64.getEncoder()
                        .encodeToString(shape.descriptorSet().toByteArray()));
                output.put("protoSource", shape.protoSource());
            } else {
                if (rules.isEmpty() && celRules.isEmpty()) {
                    throw Inputs.invalidInput("An authored 'target' needs 'rules' and/or "
                            + "'celRules' to populate it", "/rules");
                }
                SchemaResolver.ResolvedSchema schema = SchemaResolver.resolveNode(
                        targetNode.get("schema"), "/target/schema", context);
                Descriptor target = schema.message(
                        Inputs.optionalString(targetNode, "type"), "/target/type");
                joined = joiner.join(target, scope, rules, celRules);
            }
        } catch (CelCompilationException e) {
            throw new ActionException("invalid-expression",
                    "CEL join expression does not compile: " + e.getMessage());
        } catch (CelEvaluationException e) {
            throw new ActionException("evaluation-failed",
                    "CEL join expression failed at runtime: " + e.getMessage());
        } catch (MappingException e) {
            throw new ActionException("mapping-failed",
                    "Join rule failed: " + e.getMessage());
        }
        output.put("type", joined.getDescriptorForType().getFullName());
        output.set("message", ActionJson.messageToJson(joined, context));
        return output;
    }

    private static MessageScope buildScope(ObjectNode input,
                                           List<ShapeSynthesizer.NamedType> named,
                                           ActionContext context) throws ActionException {
        ArrayNode sources = Inputs.optionalArray(input, "sources");
        MessageScope.Builder scope = MessageScope.builder();
        for (int i = 0; i < named.size(); i++) {
            ObjectNode source = (ObjectNode) sources.get(i);
            ObjectNode messageNode = Inputs.requireObject(source, "message");
            String pointer = "/sources/" + i + "/message";
            try {
                scope.add(named.get(i).name(), context.transcoder()
                        .fromJsonDynamic(messageNode.toString(), named.get(i).type()));
            } catch (MalformedProtobufJsonException e) {
                throw Inputs.invalidInput("Message is not valid proto3 JSON for "
                        + named.get(i).type().getFullName() + ": "
                        + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
                        pointer);
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput(e.getMessage(), pointer);
            }
        }
        return scope.build();
    }

    private static ShapeSynthesizer.SynthesizedShape synthesize(
            ObjectNode shapeNode, List<ShapeSynthesizer.NamedType> named,
            ActionContext context) throws ActionException {
        String mode = Inputs.requireString(shapeNode, "mode");
        String name = Inputs.requireString(shapeNode, "name");
        ShapeSynthesizer synthesizer = new ShapeSynthesizer();
        try {
            return switch (mode) {
                case "envelope" -> synthesizer.envelope(name, named);
                case "projection" -> {
                    List<ShapeSynthesizer.ProjectedField> fields = new ArrayList<>();
                    ArrayNode fieldsNode = Inputs.optionalArray(shapeNode, "fields");
                    if (fieldsNode == null || fieldsNode.isEmpty()) {
                        throw Inputs.invalidInput("A projection shape needs 'fields'",
                                "/shape/fields");
                    }
                    for (int i = 0; i < fieldsNode.size(); i++) {
                        JsonNode field = fieldsNode.get(i);
                        if (!(field instanceof ObjectNode fieldNode)) {
                            throw Inputs.invalidInput("Each field must be an object",
                                    "/shape/fields/" + i);
                        }
                        fields.add(new ShapeSynthesizer.ProjectedField(
                                Inputs.requireString(fieldNode, "name"),
                                Inputs.requireString(fieldNode, "from")));
                    }
                    yield synthesizer.projection(name, named, fields);
                }
                case "union" -> synthesizer.taggedUnion(name, named);
                default -> throw Inputs.invalidInput(
                        "'shape.mode' must be 'envelope', 'projection', or 'union'; got '"
                                + mode + "'", "/shape/mode");
            };
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "/shape");
        }
    }
}
