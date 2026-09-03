package ai.protomolt.proto.actions;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelMappingRule;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.shapes.MessageJoiner;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ShapeSynthesizer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
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
    public Descriptor requestType() {
        return CatalogContract.request("JoinMessagesRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("JoinMessagesResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        List<ShapeSynthesizer.NamedType> named =
                SynthesizeShapeAction.namedSources(input, context);
        MessageScope scope = buildScope(input, named, context);
        boolean hasTarget = Fields.has(input, "target");
        boolean hasShape = Fields.has(input, "shape");
        if (hasTarget == hasShape) {
            throw Inputs.invalidInput("Give exactly one of 'target' or 'shape'", "");
        }
        List<String> rules = Fields.strings(input, "rules");
        List<CelMappingRule> celRules = MapMessageAction.celRules(input);

        MessageJoiner joiner = new MessageJoiner();
        Reply output = Reply.of(responseType());
        DynamicMessage joined;
        try {
            if (hasShape) {
                ShapeSynthesizer.SynthesizedShape shape =
                        synthesize(Fields.message(input, "shape"), named, context);
                joined = joiner.join(shape, scope, rules, celRules);
                output.set("descriptorSetBase64", Base64.getEncoder()
                                .encodeToString(shape.descriptorSet().toByteArray()))
                        .set("protoSource", shape.protoSource());
            } else {
                if (rules.isEmpty() && celRules.isEmpty()) {
                    throw Inputs.invalidInput("An authored 'target' needs 'rules' and/or "
                            + "'celRules' to populate it", "/rules");
                }
                Message targetSource = Fields.message(input, "target");
                SchemaResolver.ResolvedSchema schema = SchemaResolver.resolveSource(
                        Fields.message(targetSource, "schema"), "/target/schema", context);
                Descriptor target = schema.message(
                        SynthesizeShapeAction.named(targetSource, "type"), "/target/type");
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
        return output
                .set("type", joined.getDescriptorForType().getFullName())
                .set("message", context.transcoder().toJson(joined))
                .build();
    }

    private static MessageScope buildScope(Message input,
                                           List<ShapeSynthesizer.NamedType> named,
                                           ActionContext context) throws ActionException {
        List<Message> sources = Fields.list(input, "sources");
        MessageScope.Builder scope = MessageScope.builder();
        for (int i = 0; i < named.size(); i++) {
            Message source = sources.get(i);
            String pointer = "/sources/" + i + "/message";
            try {
                scope.add(named.get(i).name(), context.transcoder().fromJsonDynamic(
                        Fields.json(source, "message").toString(), named.get(i).type()));
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
            Message shapeNode, List<ShapeSynthesizer.NamedType> named,
            ActionContext context) throws ActionException {
        String mode = Fields.enumName(shapeNode, "mode");
        String name = Fields.string(shapeNode, "name");
        ShapeSynthesizer synthesizer = new ShapeSynthesizer();
        try {
            return switch (mode) {
                case "SHAPE_MODE_ENVELOPE" -> synthesizer.envelope(name, named);
                case "SHAPE_MODE_PROJECTION" -> {
                    List<ShapeSynthesizer.ProjectedField> fields = new ArrayList<>();
                    List<Message> declared = Fields.list(shapeNode, "fields");
                    if (declared.isEmpty()) {
                        throw Inputs.invalidInput("A projection shape needs 'fields'",
                                "/shape/fields");
                    }
                    for (Message field : declared) {
                        fields.add(new ShapeSynthesizer.ProjectedField(
                                Fields.string(field, "name"), Fields.string(field, "from")));
                    }
                    yield synthesizer.projection(name, named, fields);
                }
                case "SHAPE_MODE_UNION" -> synthesizer.taggedUnion(name, named);
                default -> throw Inputs.invalidInput(
                        "'shape.mode' names a shape this synthesizer does not build: '"
                                + mode + "'", "/shape/mode");
            };
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "/shape");
        }
    }
}
