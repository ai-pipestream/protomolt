package ai.pipestream.proto.actions;

import ai.pipestream.proto.shapes.SchemaMerger;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The schema-level join/union: validate (clash report), resolve (rename, prefer, coalesce),
 * emit (merged proto, descriptor set, and both rulesets in one move).
 */
final class MergeSchemasAction implements ProtoAction {

    @Override
    public String name() {
        return "merge-schemas";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Merges the top-level fields of two or more message types into one new type. "
                + "Clash analysis is pure descriptor work: same name + same type coalesces "
                + "(the natural join keys, reported as info); same name + different type or "
                + "cardinality blocks emission until 'resolutions' decides — rename (default "
                + "'<source>_<field>'), prefer one source, or override a coalesce. A "
                + "resolved merge returns the merged proto source (registrable, true import "
                + "paths), the descriptor set, 'joinRules' (one ruleset reading every source "
                + "at once), and 'unionRules' (one ruleset per source). Set reportOnly for "
                + "the validation step alone.";
    }

    @Override
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("MergeSchemasRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "MergeSchemasRequest", name());
        String name = Inputs.requireString(input, "name");
        List<ShapeSynthesizer.NamedType> sources =
                SynthesizeShapeAction.namedSources(input, context);
        Map<String, SchemaMerger.Resolution> resolutions = parseResolutions(input);
        boolean reportOnly = Inputs.optionalBoolean(input, "reportOnly", false);

        SchemaMerger.MergeResult result;
        try {
            result = new SchemaMerger().merge(name, sources,
                    reportOnly ? Map.of() : resolutions);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "");
        }

        ObjectNode output = context.objectMapper().createObjectNode();
        boolean resolved = !reportOnly && result.resolved();
        output.put("resolved", resolved);
        ArrayNode clashes = output.putArray("clashes");
        for (SchemaMerger.Clash clash : result.clashes()) {
            ObjectNode entry = clashes.addObject();
            entry.put("field", clash.field());
            entry.put("kind", clash.kind().name().toLowerCase(Locale.ROOT).replace('_', '-'));
            ArrayNode origins = entry.putArray("origins");
            for (SchemaMerger.Origin origin : clash.origins()) {
                ObjectNode originNode = origins.addObject();
                originNode.put("source", origin.source());
                originNode.put("type", origin.display());
            }
            ObjectNode suggested = entry.putObject("suggested");
            suggested.put("action", clash.suggested().action());
            if (!clash.suggested().names().isEmpty()) {
                ObjectNode names = suggested.putObject("names");
                clash.suggested().names().forEach(names::put);
            }
        }
        if (resolved) {
            ShapeSynthesizer.SynthesizedShape shape = result.shape();
            output.put("type", shape.type().getFullName());
            output.put("file", shape.file().getName());
            output.put("protoSource", shape.protoSource());
            output.put("descriptorSetBase64", Base64.getEncoder()
                    .encodeToString(shape.descriptorSet().toByteArray()));
            ArrayNode joinRules = output.putArray("joinRules");
            shape.impliedRules().forEach(joinRules::add);
            // {"<source>": {"rules": [...]}} — the proto3 JSON of map<string, MergeRuleList>.
            ObjectNode unionRules = output.putObject("unionRules");
            result.unionRules().forEach((sourceName, rules) -> {
                ArrayNode list = unionRules.putObject(sourceName).putArray("rules");
                rules.forEach(list::add);
            });
        }
        return output;
    }

    private static Map<String, SchemaMerger.Resolution> parseResolutions(ObjectNode input)
            throws ActionException {
        ObjectNode node = Inputs.optionalObject(input, "resolutions");
        if (node == null) {
            return Map.of();
        }
        Map<String, SchemaMerger.Resolution> resolutions = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : node.properties()) {
            String pointer = "/resolutions/" + entry.getKey();
            if (!(entry.getValue() instanceof ObjectNode resolution)) {
                throw Inputs.invalidInput("Each resolution must be an object", pointer);
            }
            Map<String, String> names = new LinkedHashMap<>();
            ObjectNode namesNode = Inputs.optionalObject(resolution, "names");
            if (namesNode != null) {
                for (Map.Entry<String, JsonNode> nameEntry : namesNode.properties()) {
                    names.put(nameEntry.getKey(), nameEntry.getValue().asText());
                }
            }
            resolutions.put(entry.getKey(), new SchemaMerger.Resolution(
                    Inputs.requireString(resolution, "action"),
                    Inputs.optionalString(resolution, "source"),
                    names));
        }
        return resolutions;
    }
}
