package ai.pipestream.proto.actions;

import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

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
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("SynthesizeShapeRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "SynthesizeShapeRequest", name());
        String mode = Inputs.requireString(input, "mode");
        String name = Inputs.requireString(input, "name");
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
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("type", shape.type().getFullName());
        output.put("file", shape.file().getName());
        output.put("protoSource", shape.protoSource());
        output.put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(shape.descriptorSet().toByteArray()));
        ArrayNode implied = output.putArray("impliedRules");
        shape.impliedRules().forEach(implied::add);
        return output;
    }

    /** Parses and resolves the named source types shared by both shape verbs. */
    static List<ShapeSynthesizer.NamedType> namedSources(ObjectNode input, ActionContext context)
            throws ActionException {
        ArrayNode sources = Inputs.optionalArray(input, "sources");
        if (sources == null || sources.isEmpty()) {
            throw Inputs.invalidInput("'sources' must be a non-empty array", "/sources");
        }
        List<ShapeSynthesizer.NamedType> named = new ArrayList<>(sources.size());
        for (int i = 0; i < sources.size(); i++) {
            JsonNode node = sources.get(i);
            String pointer = "/sources/" + i;
            if (!(node instanceof ObjectNode source)) {
                throw Inputs.invalidInput("Each source must be an object", pointer);
            }
            String name = Inputs.requireString(source, "name");
            SchemaResolver.ResolvedSchema schema = SchemaResolver.resolveNode(
                    source.get("schema"), pointer + "/schema", context);
            Descriptor type = schema.message(
                    Inputs.optionalString(source, "type"), pointer + "/type");
            try {
                named.add(new ShapeSynthesizer.NamedType(name, type));
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput(e.getMessage(), pointer + "/name");
            }
        }
        return named;
    }

    static List<ShapeSynthesizer.ProjectedField> projectedFields(ObjectNode input)
            throws ActionException {
        ArrayNode fields = Inputs.optionalArray(input, "fields");
        if (fields == null || fields.isEmpty()) {
            throw Inputs.invalidInput(
                    "'fields' must be a non-empty array for a projection", "/fields");
        }
        List<ShapeSynthesizer.ProjectedField> projected = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            JsonNode node = fields.get(i);
            if (!(node instanceof ObjectNode field)) {
                throw Inputs.invalidInput("Each field must be an object", "/fields/" + i);
            }
            try {
                projected.add(new ShapeSynthesizer.ProjectedField(
                        Inputs.requireString(field, "name"),
                        Inputs.requireString(field, "from")));
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput(e.getMessage(), "/fields/" + i);
            }
        }
        return projected;
    }
}
