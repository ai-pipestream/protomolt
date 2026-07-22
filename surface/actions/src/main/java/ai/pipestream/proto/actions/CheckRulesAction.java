package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.shapes.MessageJoiner;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.RuleChecker;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Statically validates mapping rules, CEL mapping expressions, and CEL filters against
 * descriptors — and, when sample messages ride along, dry-runs them. Two modes mirror the
 * two rule dialects: one source and no target checks the {@code map-message} dialect
 * (unscoped paths, in place); multiple sources or an explicit target checks the join
 * dialect (scoped paths).
 */
final class CheckRulesAction implements ProtoAction {

    @Override
    public String name() {
        return "check-rules";
    }

    @Override
    public String description() {
        return "Statically validates mapping rules and CEL expressions against descriptors: "
                + "every path must resolve, shapes must line up (repeated vs singular, "
                + "message types), CEL must compile and type-check, and filters must be "
                + "boolean. One source and no target checks the map-message dialect in "
                + "place; multiple sources or a 'target' checks the scoped join dialect. "
                + "When every source carries a 'message', the rules are also dry-run: the "
                + "response includes the mapped output and each filter's verdict.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        ObjectNode sources = properties.putObject("sources");
        sources.put("type", "array");
        sources.put("description", "The named sources: one (in-place mode; the name is the "
                + "CEL variable, conventionally 'input') or many (scoped mode). Each may "
                + "carry a sample 'message' for the dry run.");
        ObjectNode source = sources.putObject("items");
        source.put("type", "object");
        ObjectNode sourceProperties = source.putObject("properties");
        sourceProperties.putObject("name").put("type", "string");
        sourceProperties.set("schema", ActionJson.schemaSourceSchema());
        sourceProperties.set("type", ActionJson.typeProperty(
                "Fully qualified message type; required unless the schema identifies one."));
        sourceProperties.putObject("message")
                .put("type", "object")
                .put("description", "Optional sample message (proto3 JSON) for the dry run.");
        source.putArray("required").add("name").add("schema");
        ObjectNode target = properties.putObject("target");
        target.put("type", "object");
        target.put("description", "The output type rules write to (scoped mode); omitted "
                + "with a single source, rules apply in place.");
        ObjectNode targetProperties = target.putObject("properties");
        targetProperties.set("schema", ActionJson.schemaSourceSchema());
        targetProperties.set("type", ActionJson.typeProperty(
                "Fully qualified output message type."));
        ObjectNode rules = properties.putObject("rules");
        rules.put("type", "array");
        rules.put("description", "Text mapping rules to check.");
        rules.putObject("items").put("type", "string");
        ObjectNode celRules = properties.putObject("celRules");
        celRules.put("type", "array");
        celRules.put("description", "CEL mapping rules to check "
                + "({filter?, selector?, target, fallback?}).");
        celRules.putObject("items").put("type", "object");
        ObjectNode filters = properties.putObject("filters");
        filters.put("type", "array");
        filters.put("description", "Boolean CEL predicates to check (and evaluate in the "
                + "dry run).");
        filters.putObject("items").put("type", "string");
        ActionJson.required(schema, "sources");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        List<ShapeSynthesizer.NamedType> named =
                SynthesizeShapeAction.namedSources(input, context);
        ObjectNode targetNode = Inputs.optionalObject(input, "target");
        boolean inPlace = targetNode == null && named.size() == 1;
        if (targetNode == null && named.size() > 1) {
            throw Inputs.invalidInput(
                    "Multiple sources need a 'target' output type", "/target");
        }
        Descriptor target = inPlace
                ? named.get(0).type()
                : SchemaResolver.resolveNode(targetNode.get("schema"), "/target/schema", context)
                        .message(Inputs.optionalString(targetNode, "type"), "/target/type");

        ArrayNode rulesNode = Inputs.optionalArray(input, "rules");
        List<String> rules = rulesNode == null
                ? List.of() : Inputs.stringElements(rulesNode, "/rules");
        List<CelMappingRule> celRules =
                MapMessageAction.parseCelRules(Inputs.optionalArray(input, "celRules"));
        ArrayNode filtersNode = Inputs.optionalArray(input, "filters");
        List<String> filters = filtersNode == null
                ? List.of() : Inputs.stringElements(filtersNode, "/filters");

        Map<String, Descriptor> sourceTypes = new LinkedHashMap<>();
        named.forEach(source -> sourceTypes.put(source.name(), source.type()));
        RuleChecker checker = new RuleChecker();
        List<RuleChecker.Finding> findings = inPlace
                ? checker.checkInPlace(named.get(0).name(), target, rules, celRules, filters)
                : checker.checkScoped(sourceTypes, target, rules, celRules, filters);

        ObjectNode output = context.objectMapper().createObjectNode();
        ArrayNode findingsNode = output.putArray("findings");
        for (RuleChecker.Finding finding : findings) {
            ObjectNode node = findingsNode.addObject();
            node.put("kind", finding.kind());
            node.put("index", finding.index());
            node.put("rule", finding.rule());
            node.put("error", finding.error());
        }

        Map<String, DynamicMessage> samples = sampleMessages(input, named, context);
        if (findings.isEmpty() && samples != null) {
            dryRun(output, named, samples, target, inPlace, rules, celRules, filters, context);
        }
        output.put("ok", findings.isEmpty() && !output.has("dryRunError"));
        return output;
    }

    /** Every source's sample message, or null when any is missing (no dry run). */
    private static Map<String, DynamicMessage> sampleMessages(
            ObjectNode input, List<ShapeSynthesizer.NamedType> named, ActionContext context)
            throws ActionException {
        ArrayNode sources = Inputs.optionalArray(input, "sources");
        Map<String, DynamicMessage> samples = new LinkedHashMap<>();
        for (int i = 0; i < named.size(); i++) {
            ObjectNode source = (ObjectNode) sources.get(i);
            ObjectNode message = Inputs.optionalObject(source, "message");
            if (message == null) {
                return null;
            }
            try {
                samples.put(named.get(i).name(), context.transcoder()
                        .fromJsonDynamic(message.toString(), named.get(i).type()));
            } catch (MalformedProtobufJsonException e) {
                throw Inputs.invalidInput("Sample message is not valid proto3 JSON for "
                        + named.get(i).type().getFullName(), "/sources/" + i + "/message");
            }
        }
        return samples;
    }

    private static void dryRun(ObjectNode output, List<ShapeSynthesizer.NamedType> named,
                               Map<String, DynamicMessage> samples, Descriptor target,
                               boolean inPlace, List<String> rules,
                               List<CelMappingRule> celRules, List<String> filters,
                               ActionContext context) throws ActionException {
        CelEnvironmentFactory environment = CelEnvironmentFactory.builder();
        named.forEach(source -> environment.addMessageVar(source.name(), source.type()));
        CelEvaluator evaluator = new CelEvaluator(environment.build());
        Map<String, Object> bindings = new LinkedHashMap<>(samples);
        try {
            Message result;
            if (inPlace) {
                String varName = named.get(0).name();
                Message.Builder builder = samples.get(varName).toBuilder();
                ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(context.registry());
                if (!rules.isEmpty()) {
                    mapper.mapInPlace(builder, rules);
                }
                if (!celRules.isEmpty()) {
                    new CelProtoMapper(mapper, evaluator, varName).map(builder, celRules);
                }
                result = builder.build();
            } else {
                MessageScope.Builder scope = MessageScope.builder();
                samples.forEach(scope::add);
                result = new MessageJoiner().join(target, scope.build(), rules, celRules);
            }
            output.put("type", result.getDescriptorForType().getFullName());
            output.set("message", ActionJson.messageToJson(result, context));
            ArrayNode verdicts = output.putArray("filterResults");
            for (String filter : filters) {
                verdicts.add(evaluator.evaluateBooleanOrFail(filter, bindings));
            }
        } catch (Exception e) {
            // The static pass was clean but the sample tripped something dynamic
            // (a Struct path, a conversion): that is a finding, not a crash.
            output.put("dryRunError", e.getMessage());
        }
    }
}
