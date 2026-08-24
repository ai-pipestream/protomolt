package ai.pipestream.proto.actions;

import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Synthesizes the output shape of a join or union — a real message type derived from named
 * source types — returning the linked descriptor set, the registrable proto source, and the
 * mapping rules the shape implies.
 */
final class SynthesizeShapeAction implements ProtoAction {

    @Override
    public String name() {
        return "synthesize-shape";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Synthesizes a message type from named source types: an 'envelope' (one field "
                + "per source, each intact), a 'projection' (a flat message whose field types "
                + "are inferred from scoped source paths like 'customer.name'), or a 'union' "
                + "(a oneof over the sources). Returns proto source with the sources' true "
                + "import paths (registrable as a registry subject with references), the "
                + "self-contained descriptor set, and the implied mapping rules.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("SynthesizeShapeRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("SynthesizeShapeResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        String mode = Fields.enumName(input, "mode");
        String name = Fields.string(input, "name");
        List<ShapeSynthesizer.NamedType> sources = namedSources(input, context);
        ShapeSynthesizer synthesizer = new ShapeSynthesizer();
        ShapeSynthesizer.SynthesizedShape shape;
        try {
            // The contract names the mode with an enum and refuses the unset value, so
            // every case here is one the synthesizer implements.
            shape = switch (mode) {
                case "SHAPE_MODE_ENVELOPE" -> synthesizer.envelope(name, sources);
                case "SHAPE_MODE_PROJECTION" -> synthesizer.projection(name, sources,
                        projectedFields(input));
                case "SHAPE_MODE_UNION" -> synthesizer.taggedUnion(name, sources);
                default -> throw Inputs.invalidInput(
                        "'mode' names a shape this synthesizer does not build: " + mode,
                        "/mode");
            };
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "/sources");
        }
        return Reply.of(responseType())
                .set("type", shape.type().getFullName())
                .set("file", shape.file().getName())
                .set("protoSource", shape.protoSource())
                .set("descriptorSetBase64",
                        Base64.getEncoder().encodeToString(shape.descriptorSet().toByteArray()))
                .addAll("impliedRules", shape.impliedRules())
                .build();
    }

    /** Parses and resolves the named source types shared by both shape verbs. */
    static List<ShapeSynthesizer.NamedType> namedSources(Message input, ActionContext context)
            throws ActionException {
        List<Message> sources = Fields.list(input, "sources");
        if (sources.isEmpty()) {
            throw Inputs.invalidInput("'sources' must be a non-empty array", "/sources");
        }
        List<ShapeSynthesizer.NamedType> named = new ArrayList<>(sources.size());
        for (int i = 0; i < sources.size(); i++) {
            Message source = sources.get(i);
            String pointer = "/sources/" + i;
            SchemaResolver.ResolvedSchema schema = SchemaResolver.resolveSource(
                    Fields.message(source, "schema"), pointer + "/schema", context);
            Descriptor type = schema.message(
                    named(source, "type"), pointer + "/type");
            try {
                named.add(new ShapeSynthesizer.NamedType(
                        Fields.string(source, "name"), type));
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput(e.getMessage(), pointer + "/name");
            }
        }
        return named;
    }

    /** A named type, or null when the caller left the schema's own default to apply. */
    static String named(Message message, String field) {
        String value = Fields.string(message, field);
        return value.isEmpty() ? null : value;
    }

    static List<ShapeSynthesizer.ProjectedField> projectedFields(Message input)
            throws ActionException {
        List<Message> fields = Fields.list(input, "fields");
        if (fields.isEmpty()) {
            throw Inputs.invalidInput(
                    "'fields' must be a non-empty array for a projection", "/fields");
        }
        List<ShapeSynthesizer.ProjectedField> projected = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Message field = fields.get(i);
            try {
                projected.add(new ShapeSynthesizer.ProjectedField(
                        Fields.string(field, "name"), Fields.string(field, "from")));
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput(e.getMessage(), "/fields/" + i);
            }
        }
        return projected;
    }
}
