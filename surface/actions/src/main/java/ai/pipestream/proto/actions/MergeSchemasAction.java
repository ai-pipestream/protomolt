package ai.pipestream.proto.actions;

import ai.pipestream.proto.shapes.SchemaMerger;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
    public Descriptor requestType() {
        return CatalogContract.request("MergeSchemasRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("MergeSchemasResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        String name = Fields.string(input, "name");
        List<ShapeSynthesizer.NamedType> sources =
                SynthesizeShapeAction.namedSources(input, context);
        Map<String, SchemaMerger.Resolution> resolutions = resolutions(input);
        boolean reportOnly = Fields.flag(input, "reportOnly");

        SchemaMerger.MergeResult result;
        try {
            result = new SchemaMerger().merge(name, sources,
                    reportOnly ? Map.of() : resolutions);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput(e.getMessage(), "");
        }

        boolean resolved = !reportOnly && result.resolved();
        Reply output = Reply.of(responseType()).set("resolved", resolved);
        for (SchemaMerger.Clash clash : result.clashes()) {
            Reply entry = output.append("clashes")
                    .set("field", clash.field())
                    .set("kind", clash.kind().name().toLowerCase(Locale.ROOT).replace('_', '-'));
            for (SchemaMerger.Origin origin : clash.origins()) {
                entry.append("origins")
                        .set("source", origin.source())
                        .set("type", origin.display())
                        .build();
            }
            Reply suggested = entry.nest("suggested").set("action", clash.suggested().action());
            clash.suggested().names().forEach((from, to) ->
                    suggested.append("names").set("key", from).set("value", to).build());
            suggested.build();
            entry.build();
        }
        if (resolved) {
            ShapeSynthesizer.SynthesizedShape shape = result.shape();
            output.set("type", shape.type().getFullName())
                    .set("file", shape.file().getName())
                    .set("protoSource", shape.protoSource())
                    .set("descriptorSetBase64", Base64.getEncoder()
                            .encodeToString(shape.descriptorSet().toByteArray()))
                    .addAll("joinRules", shape.impliedRules());
            result.unionRules().forEach((sourceName, rules) -> {
                Reply entry = output.append("unionRules").set("key", sourceName);
                entry.nest("value").addAll("rules", rules).build();
                entry.build();
            });
        }
        return output.build();
    }

    private static Map<String, SchemaMerger.Resolution> resolutions(Message input)
            throws ActionException {
        Map<String, SchemaMerger.Resolution> resolutions = new LinkedHashMap<>();
        for (Object element : Fields.<Message>list(input, "resolutions")) {
            Message pair = (Message) element;
            String field = Fields.string(pair, "key");
            Message resolution = Fields.message(pair, "value");
            resolutions.put(field, new SchemaMerger.Resolution(
                    Fields.string(resolution, "action"),
                    SynthesizeShapeAction.named(resolution, "source"),
                    Fields.map(resolution, "names")));
        }
        return resolutions;
    }
}
