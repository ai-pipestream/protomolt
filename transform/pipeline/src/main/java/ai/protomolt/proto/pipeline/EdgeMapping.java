package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.workflow.v1.CelMappingRule;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.mapper.cel.CelProtoMapper;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the values that flow along a typed edge: rule and CEL mapping from the bindings in
 * scope, projection, declared edge validation, gate evaluation, and the item and collect sides
 * of a fan-out. Nothing here calls transport.
 */
final class EdgeMapping {

    private EdgeMapping() {
    }

    static List<DynamicMessage> mapEdge(TypedEdge edge,
                                                Map<String, PipelineBinding> scope,
                                                DescriptorRegistry registry,
                                                String step)
            throws PipelineExecutionException {
        PipelineBinding stream = null;
        for (String source : edge.getSourcesList()) {
            PipelineBinding binding = scope.get(source);
            if (binding.cardinality() == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                stream = binding;
            }
        }
        Descriptor produceType = registry.findDescriptorByFullName(edge.getProduceType());
        List<DynamicMessage> produced = new ArrayList<>();
        int count = stream == null ? 1 : stream.values().size();
        for (int i = 0; i < count; i++) {
            Map<String, DynamicMessage> values = new LinkedHashMap<>();
            for (String source : edge.getSourcesList()) {
                PipelineBinding binding = scope.get(source);
                values.put(source, binding.cardinality()
                        == EdgeCardinality.EDGE_CARDINALITY_MANY
                        ? binding.values().get(i) : binding.one());
            }
            produced.add(map(produceType, values, edge.getRulesList(),
                    edge.getCelRulesList(), registry, step));
        }
        return produced;
    }

    static List<DynamicMessage> deliver(List<DynamicMessage> produced,
                                                TypedEdge edge,
                                                DescriptorRegistry registry,
                                                String step)
            throws PipelineExecutionException {
        List<DynamicMessage> delivered = new ArrayList<>(produced.size());
        for (DynamicMessage value : produced) {
            DynamicMessage next = edge.getProjectTo().isEmpty()
                    ? value : project(value, edge.getProjectTo(), registry, step);
            if (edge.getValidate()) {
                StepValues.validate(next, step, "edge value");
            }
            delivered.add(next);
        }
        return delivered;
    }

    static List<DynamicMessage> prepareFanOut(String step, DynamicMessage holder,
                                                      TypedEdge edge, FanOutSpec fanOut,
                                                      DescriptorRegistry registry)
            throws PipelineExecutionException {
        List<DynamicMessage> raw = fanOutItems(holder, fanOut);
        if (raw.size() > fanOut.getMaxItems()) {
            throw PipelineExecutor.failure(step, FailureKind.FAN_OUT,
                    "fan-out produced " + raw.size() + " items, over max_items "
                            + fanOut.getMaxItems(), null);
        }
        return deliver(raw, edge, registry, step);
    }

    static List<DynamicMessage> fanOutItems(DynamicMessage holder,
                                                    FanOutSpec fanOut) {
        return StreamPaths.repeatedValues(holder, fanOut.getItems()).stream()
                .map(value -> StepValues.dynamic((Message) value)).toList();
    }

    static DynamicMessage collect(FanOutSpec fanOut,
                                          List<DynamicMessage> outputs,
                                          DescriptorRegistry registry) {
        Descriptor type = registry.findDescriptorByFullName(fanOut.getCollectType());
        FieldDescriptor field = type.findFieldByName(fanOut.getCollectInto());
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        outputs.forEach(output -> builder.addRepeatedField(field, output));
        return builder.build();
    }

    static DynamicMessage map(Descriptor type,
                                      Map<String, DynamicMessage> values,
                                      List<String> rules,
                                      List<CelMappingRule> celRules,
                                      DescriptorRegistry registry,
                                      String where)
            throws PipelineExecutionException {
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);
        MessageScope.Builder messageScope = MessageScope.builder();
        values.forEach(messageScope::add);
        DynamicMessage.Builder target = DynamicMessage.newBuilder(type);
        try {
            mapper.map(messageScope.build(), target, rules);
            if (!celRules.isEmpty()) {
                new CelProtoMapper(mapper.fieldMapper(), evaluator(values, type), "target",
                        new LinkedHashMap<>(values)).map(target,
                        CelMappingRules.toMapperRules(celRules));
            }
            return target.build();
        } catch (Exception e) {
            throw PipelineExecutor.failure(where, FailureKind.MAPPING,
                    "mapping failed: " + e.getMessage(), e);
        }
    }

    private static DynamicMessage project(DynamicMessage source, String targetType,
                                          DescriptorRegistry registry, String step)
            throws PipelineExecutionException {
        Descriptor target = registry.findDescriptorByFullName(targetType);
        try {
            Message projected = MessageProjection.forTarget(target, registry)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "projection target declares no sources"))
                    .project(source);
            return StepValues.dynamic(projected);
        } catch (Exception e) {
            throw PipelineExecutor.failure(step, FailureKind.PROJECTION,
                    "edge projection failed: " + e.getMessage(), e);
        }
    }

    static boolean evaluateGate(PipelineStep step, Map<String, PipelineBinding> scope,
                                        Descriptor target) throws PipelineExecutionException {
        Map<String, DynamicMessage> values = singleValues(scope);
        try {
            return evaluator(values, target).evaluateBooleanOrFail(step.getWhen(),
                    new LinkedHashMap<>(values));
        } catch (RuntimeException e) {
            throw PipelineExecutor.failure(step.getName(), FailureKind.GATE,
                    "gate failed: " + e.getMessage(), e);
        }
    }

    private static CelEvaluator evaluator(Map<String, DynamicMessage> values,
                                          Descriptor target) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        values.forEach((name, value) ->
                factory.addMessageVar(name, value.getDescriptorForType()));
        factory.addMessageVar("target", target);
        return new CelEvaluator(factory.build());
    }

    static Map<String, DynamicMessage> singleValues(Map<String, PipelineBinding> scope) {
        Map<String, DynamicMessage> values = new LinkedHashMap<>();
        scope.forEach((name, binding) -> {
            if (binding.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
                values.put(name, binding.one());
            }
        });
        return values;
    }
}
