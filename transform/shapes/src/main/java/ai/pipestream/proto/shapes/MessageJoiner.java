package ai.pipestream.proto.shapes;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.MappingException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Objects;

/**
 * Builds a target message from a {@link MessageScope}: scoped text rules first, then CEL
 * rules whose expressions see every scope entry as a variable (plus {@code target}, the
 * progressive output). The target is an authored type or a
 * {@link ShapeSynthesizer.SynthesizedShape}, whose implied rules make projections and
 * envelopes joinable with no ruleset at all.
 */
public final class MessageJoiner {

    /** Joins into a synthesized shape using its implied rules plus any extra rules. */
    public DynamicMessage join(ShapeSynthesizer.SynthesizedShape shape, MessageScope scope,
                               List<String> extraRules, List<CelMappingRule> celRules)
            throws MappingException {
        List<String> rules = new java.util.ArrayList<>(shape.impliedRules());
        rules.addAll(extraRules);
        return join(shape.type(), scope, rules, celRules);
    }

    /** Joins into a target type with explicit scoped rules. */
    public DynamicMessage join(Descriptor target, MessageScope scope, List<String> rules,
                               List<CelMappingRule> celRules) throws MappingException {
        Objects.requireNonNull(target, "target");
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.registerFile(target.getFile());
        for (String name : scope.names()) {
            registry.registerFromMessage(scope.get(name));
        }
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(target);
        mapper.map(scope, builder, rules);
        if (!celRules.isEmpty()) {
            CelEnvironmentFactory environment = CelEnvironmentFactory.builder()
                    .addMessageVar("target", target);
            for (String name : scope.names()) {
                environment.addMessageVar(name, scope.get(name).getDescriptorForType());
            }
            new CelProtoMapper(mapper.fieldMapper(), new CelEvaluator(environment.build()),
                    "target", scope.asBindings())
                    .map(builder, celRules);
        }
        return builder.build();
    }

    /** Wraps one message as a case of a synthesized tagged union. */
    public DynamicMessage wrap(ShapeSynthesizer.SynthesizedShape union, String caseName,
                               Message value) {
        var field = union.type().findFieldByName(caseName);
        if (field == null) {
            throw new IllegalArgumentException("Union " + union.type().getFullName()
                    + " has no case '" + caseName + "'");
        }
        return DynamicMessage.newBuilder(union.type()).setField(field, value).build();
    }
}
