package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.shapes.MessageJoiner;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.RuleChecker;
import ai.pipestream.proto.shapes.ShapeSynthesizer;
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
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
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
    public Descriptor requestType() {
        return CatalogContract.request("CheckRulesRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("CheckRulesResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        List<ShapeSynthesizer.NamedType> named =
                SynthesizeShapeAction.namedSources(input, context);
        boolean hasTarget = Fields.has(input, "target");
        boolean inPlace = !hasTarget && named.size() == 1;
        if (!hasTarget && named.size() > 1) {
            throw Inputs.invalidInput(
                    "Multiple sources need a 'target' output type", "/target");
        }
        Descriptor target;
        if (inPlace) {
            target = named.get(0).type();
        } else {
            Message targetSource = Fields.message(input, "target");
            target = SchemaResolver.resolveSource(
                            Fields.message(targetSource, "schema"), "/target/schema", context)
                    .message(SynthesizeShapeAction.named(targetSource, "type"), "/target/type");
        }

        List<String> rules = Fields.strings(input, "rules");
        List<CelMappingRule> celRules = MapMessageAction.celRules(input);
        List<String> filters = Fields.strings(input, "filters");

        Map<String, Descriptor> sourceTypes = new LinkedHashMap<>();
        named.forEach(source -> sourceTypes.put(source.name(), source.type()));
        RuleChecker checker = new RuleChecker();
        List<RuleChecker.Finding> findings = inPlace
                ? checker.checkInPlace(named.get(0).name(), target, rules, celRules, filters)
                : checker.checkScoped(sourceTypes, target, rules, celRules, filters);

        Reply output = Reply.of(responseType());
        for (RuleChecker.Finding finding : findings) {
            output.append("findings")
                    .set("kind", finding.kind())
                    .set("index", finding.index())
                    .set("rule", finding.rule())
                    .set("error", finding.error())
                    .build();
        }

        Map<String, DynamicMessage> samples = sampleMessages(input, named, context);
        boolean dryRunFailed = false;
        if (findings.isEmpty() && samples != null) {
            dryRunFailed = dryRun(output, named, samples, target, inPlace,
                    rules, celRules, filters, context);
        }
        return output.set("ok", findings.isEmpty() && !dryRunFailed).build();
    }

    /** Every source's sample message, or null when any is missing (no dry run). */
    private static Map<String, DynamicMessage> sampleMessages(
            Message input, List<ShapeSynthesizer.NamedType> named, ActionContext context)
            throws ActionException {
        List<Message> sources = Fields.list(input, "sources");
        Map<String, DynamicMessage> samples = new LinkedHashMap<>();
        for (int i = 0; i < named.size(); i++) {
            Message source = sources.get(i);
            if (!Fields.has(source, "message")) {
                return null;
            }
            try {
                samples.put(named.get(i).name(), context.transcoder().fromJsonDynamic(
                        Fields.json(source, "message").toString(), named.get(i).type()));
            } catch (MalformedProtobufJsonException e) {
                throw Inputs.invalidInput("Sample message is not valid proto3 JSON for "
                        + named.get(i).type().getFullName(), "/sources/" + i + "/message");
            }
        }
        return samples;
    }

    /** Runs the rules over the samples, reporting a dynamic failure rather than raising it. */
    private static boolean dryRun(Reply output, List<ShapeSynthesizer.NamedType> named,
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
            output.set("type", result.getDescriptorForType().getFullName())
                    .set("message", context.transcoder().toJson(result));
            for (String filter : filters) {
                output.add("filterResults", evaluator.evaluateBooleanOrFail(filter, bindings));
            }
            return false;
        } catch (Exception e) {
            // The static pass was clean but the sample tripped something dynamic
            // (a Struct path, a conversion): that is a finding, not a crash.
            output.set("dryRunError", e.getMessage());
            return true;
        }
    }
}
